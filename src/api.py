"""
API Flask para el procesamiento de reportes VPN
Recibe archivos Excel R033 y R065, los procesa y retorna el Excel resultante
"""
from flask import Flask, request, jsonify, send_file, Response
from werkzeug.utils import secure_filename
import os
import tempfile
from datetime import datetime
from logic import ExcelProcessor
from connections import GCSConnection
from flask_cors import CORS

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

# Configuración
RESULTS_FOLDER = 'results'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# Conexión a GCS (se inicializa en cada request o al inicio)
gcs = GCSConnection()


def allowed_file(filename: str) -> bool:
    """Verifica si el archivo tiene una extensión permitida"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de health check"""
    print("[API] Health check solicitado")
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "col_reporte_vpn"
    })


@app.route('/process', methods=['POST'])
def process_files():
    """
    Endpoint principal para procesar archivos R033 y R065
    
    Espera recibir:
    - r033: Archivo Excel R033
    - r065: Archivo Excel R065
    
    Retorna:
    - JSON con el link del bucket para descargar el Excel
    """
    print("=" * 60)
    print("[API] Nueva solicitud de procesamiento recibida")
    print("=" * 60)
    
    # Validar que se recibieron los archivos
    if 'r033' not in request.files:
        print("[API] Error: Archivo R033 no recibido")
        return jsonify({"error": "Archivo R033 es requerido"}), 400
    
    if 'r065' not in request.files:
        print("[API] Error: Archivo R065 no recibido")
        return jsonify({"error": "Archivo R065 es requerido"}), 400
    
    file_r033 = request.files['r033']
    file_r065 = request.files['r065']
    
    # Validar nombres de archivo
    if file_r033.filename == '':
        print("[API] Error: Archivo R033 sin nombre")
        return jsonify({"error": "Archivo R033 sin nombre"}), 400
    
    if file_r065.filename == '':
        print("[API] Error: Archivo R065 sin nombre")
        return jsonify({"error": "Archivo R065 sin nombre"}), 400
    
    # Validar extensiones
    if not allowed_file(file_r033.filename):
        print(f"[API] Error: Extensión no permitida para R033: {file_r033.filename}")
        return jsonify({"error": "R033 debe ser un archivo Excel (.xlsx o .xls)"}), 400
    
    if not allowed_file(file_r065.filename):
        print(f"[API] Error: Extensión no permitida para R065: {file_r065.filename}")
        return jsonify({"error": "R065 debe ser un archivo Excel (.xlsx o .xls)"}), 400
    
    print(f"[API] Archivos recibidos: R033={file_r033.filename}, R065={file_r065.filename}")
    
    # Usar archivos temporales
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Crear archivos temporales para los uploads
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_r033:
            file_r033.save(tmp_r033.name)
            r033_path = tmp_r033.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_r065:
            file_r065.save(tmp_r065.name)
            r065_path = tmp_r065.name
        
        print(f"[API] Archivos guardados temporalmente")
        
        # Procesar los archivos
        output_filename = f"reporte_vpn_{timestamp}.xlsx"
        
        # Usar carpeta results local para el procesamiento
        os.makedirs(RESULTS_FOLDER, exist_ok=True)
        output_path = os.path.join(RESULTS_FOLDER, output_filename)
        
        processor = ExcelProcessor()
        result = processor.execute(r033_path, r065_path, output_path)
        
        # Limpiar archivos temporales de entrada
        if os.path.exists(r033_path):
            os.remove(r033_path)
        if os.path.exists(r065_path):
            os.remove(r065_path)
        
        if not result["success"]:
            print(f"[API] Error en procesamiento: {result['error']}")
            return jsonify({"error": result["error"]}), 500
        
        print(f"[API] Procesamiento exitoso: {result['rows_processed']} filas procesadas")
        
        # Subir el archivo al bucket
        if not gcs.connect():
            print("[API] Error: No se pudo conectar a GCS")
            return jsonify({
                "success": True,
                "message": "Procesamiento completado, pero no se pudo subir a GCS",
                "rows_processed": result["rows_processed"],
                "local_path": output_path,
                "gcs_url": None
            })
        
        # Organizar en carpeta por fecha (YYYY-MM-DD)
        fecha_carpeta = datetime.now().strftime('%Y-%m-%d')
        blob_name = f"reportes/{fecha_carpeta}/{output_filename}"
        
        if gcs.upload_file(output_path, blob_name):
            # Generar URL de descarga
            download_url = gcs.get_signed_url(blob_name)
            public_url = gcs.get_public_url(blob_name)
            
            print(f"[API] Archivo subido a GCS: {blob_name}")
            
            # Limpiar archivo local después de subir
            if os.path.exists(output_path):
                os.remove(output_path)
            
            return jsonify({
                "success": True,
                "message": "Procesamiento completado exitosamente",
                "rows_processed": result["rows_processed"],
                "filename": output_filename,
                "gcs_path": blob_name,
                "gcs_folder": fecha_carpeta,
                "download_url": download_url,
                "public_url": public_url,
                "timestamp": timestamp
            })
        else:
            print("[API] Error al subir archivo a GCS")
            return jsonify({
                "success": True,
                "message": "Procesamiento completado, pero error al subir a GCS",
                "rows_processed": result["rows_processed"],
                "local_path": output_path,
                "gcs_url": None
            })
        
    except Exception as e:
        print(f"[API] Error inesperado: {str(e)}")
        # Limpiar archivos temporales en caso de error
        for path in [r033_path, r065_path]:
            if 'path' in dir() and os.path.exists(path):
                os.remove(path)
        return jsonify({"error": f"Error inesperado: {str(e)}"}), 500


@app.route('/download/<filename>', methods=['GET'])
@app.route('/download/<fecha>/<filename>', methods=['GET'])
def download_file(filename: str, fecha: str = None):
    """
    Endpoint para descargar un archivo del bucket de GCS
    
    Rutas:
    - /download/<filename> - Busca en la carpeta de hoy
    - /download/<fecha>/<filename> - Busca en la carpeta de la fecha especificada (YYYY-MM-DD)
    """
    print(f"[API] Solicitud de descarga desde bucket: {filename}, fecha: {fecha}")
    
    # Sanitizar nombre de archivo
    safe_filename = secure_filename(filename)
    
    # Si no se especifica fecha, usar la de hoy
    if fecha is None:
        fecha = datetime.now().strftime('%Y-%m-%d')
    
    blob_name = f"reportes/{fecha}/{safe_filename}"
    
    # Conectar a GCS
    if not gcs.connect():
        print("[API] Error: No se pudo conectar a GCS")
        return jsonify({"error": "Error de conexión a GCS"}), 500
    
    # Verificar si el archivo existe en el bucket
    if not gcs.blob_exists(blob_name):
        print(f"[API] Archivo no encontrado en bucket: {blob_name}")
        return jsonify({"error": f"Archivo no encontrado en el bucket: {blob_name}"}), 404
    
    # Opción 1: Redirigir a URL firmada
    signed_url = gcs.get_signed_url(blob_name)
    if signed_url:
        return jsonify({
            "filename": safe_filename,
            "folder": fecha,
            "gcs_path": blob_name,
            "download_url": signed_url,
            "message": "Use el download_url para descargar el archivo"
        })
    
    # Opción 2: Descargar y enviar el archivo directamente
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            if gcs.download_file(blob_name, tmp_file.name):
                return send_file(
                    tmp_file.name,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name=safe_filename
                )
            else:
                return jsonify({"error": "Error al descargar archivo del bucket"}), 500
    except Exception as e:
        print(f"[API] Error al descargar: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/files', methods=['GET'])
def list_files():
    """Lista los archivos disponibles en el bucket de GCS"""
    print("[API] Listando archivos del bucket")
    
    # Conectar a GCS
    if not gcs.connect():
        print("[API] Error: No se pudo conectar a GCS")
        return jsonify({"error": "Error de conexión a GCS"}), 500
    
    # Listar archivos del bucket (en la carpeta reportes/)
    files = gcs.list_files(prefix="reportes/")
    
    return jsonify({
        "bucket": gcs.bucket_name,
        "files": files,
        "count": len(files)
    })


if __name__ == '__main__':
    print("=" * 60)
    print("  COL REPORTE VPN - API Flask")
    print("=" * 60)
    
    # Cloud Run usa la variable PORT, default 5000 para desarrollo local
    port = int(os.environ.get('PORT', 5000))
    debug_mode = os.environ.get('FLASK_DEBUG', '0') == '1'
    
    print(f"[API] Iniciando servidor en puerto {port}...")
    print(f"[API] Debug mode: {debug_mode}")
    
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
