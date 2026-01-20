"""
API Flask para el procesamiento de reportes VPN
Recibe archivos Excel R033 y R065, los procesa y retorna el Excel resultante
"""
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
from datetime import datetime
from logic import ExcelProcessor

app = Flask(__name__)

# Configuración
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# Crear carpetas necesarias
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)


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
    - El archivo Excel procesado
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
    
    # Guardar archivos temporalmente
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    r033_filename = secure_filename(f"r033_{timestamp}_{file_r033.filename}")
    r065_filename = secure_filename(f"r065_{timestamp}_{file_r065.filename}")
    
    r033_path = os.path.join(UPLOAD_FOLDER, r033_filename)
    r065_path = os.path.join(UPLOAD_FOLDER, r065_filename)
    
    try:
        file_r033.save(r033_path)
        file_r065.save(r065_path)
        print(f"[API] Archivos guardados temporalmente")
    except Exception as e:
        print(f"[API] Error al guardar archivos: {str(e)}")
        return jsonify({"error": f"Error al guardar archivos: {str(e)}"}), 500
    
    # Procesar los archivos
    output_filename = f"reporte_vpn_{timestamp}.xlsx"
    output_path = os.path.join(RESULTS_FOLDER, output_filename)
    
    try:
        processor = ExcelProcessor()
        result = processor.execute(r033_path, r065_path, output_path)
        
        # Limpiar archivos temporales
        if os.path.exists(r033_path):
            os.remove(r033_path)
        if os.path.exists(r065_path):
            os.remove(r065_path)
        
        if not result["success"]:
            print(f"[API] Error en procesamiento: {result['error']}")
            return jsonify({"error": result["error"]}), 500
        
        print(f"[API] Procesamiento exitoso: {result['rows_processed']} filas procesadas")
        print(f"[API] Enviando archivo: {output_path}")
        
        # Retornar el archivo Excel
        return send_file(
            output_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=output_filename
        )
        
    except Exception as e:
        print(f"[API] Error inesperado: {str(e)}")
        # Limpiar archivos temporales en caso de error
        if os.path.exists(r033_path):
            os.remove(r033_path)
        if os.path.exists(r065_path):
            os.remove(r065_path)
        return jsonify({"error": f"Error inesperado: {str(e)}"}), 500


@app.route('/process-json', methods=['POST'])
def process_files_json():
    """
    Endpoint alternativo que retorna JSON con información del procesamiento
    en lugar del archivo directamente
    """
    print("=" * 60)
    print("[API] Nueva solicitud de procesamiento (JSON) recibida")
    print("=" * 60)
    
    # Validar que se recibieron los archivos
    if 'r033' not in request.files:
        return jsonify({"error": "Archivo R033 es requerido"}), 400
    
    if 'r065' not in request.files:
        return jsonify({"error": "Archivo R065 es requerido"}), 400
    
    file_r033 = request.files['r033']
    file_r065 = request.files['r065']
    
    if file_r033.filename == '' or file_r065.filename == '':
        return jsonify({"error": "Archivos sin nombre"}), 400
    
    if not allowed_file(file_r033.filename) or not allowed_file(file_r065.filename):
        return jsonify({"error": "Los archivos deben ser Excel (.xlsx o .xls)"}), 400
    
    # Guardar archivos temporalmente
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    r033_path = os.path.join(UPLOAD_FOLDER, secure_filename(f"r033_{timestamp}.xlsx"))
    r065_path = os.path.join(UPLOAD_FOLDER, secure_filename(f"r065_{timestamp}.xlsx"))
    output_path = os.path.join(RESULTS_FOLDER, f"reporte_vpn_{timestamp}.xlsx")
    
    try:
        file_r033.save(r033_path)
        file_r065.save(r065_path)
        
        processor = ExcelProcessor()
        result = processor.execute(r033_path, r065_path, output_path)
        
        # Limpiar archivos temporales
        if os.path.exists(r033_path):
            os.remove(r033_path)
        if os.path.exists(r065_path):
            os.remove(r065_path)
        
        if result["success"]:
            return jsonify({
                "success": True,
                "message": "Procesamiento completado exitosamente",
                "rows_processed": result["rows_processed"],
                "output_file": output_path,
                "timestamp": timestamp
            })
        else:
            return jsonify({
                "success": False,
                "error": result["error"]
            }), 500
            
    except Exception as e:
        print(f"[API] Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename: str):
    """Endpoint para descargar un archivo de resultados previamente generado"""
    print(f"[API] Solicitud de descarga: {filename}")
    
    # Sanitizar nombre de archivo
    safe_filename = secure_filename(filename)
    file_path = os.path.join(RESULTS_FOLDER, safe_filename)
    
    if not os.path.exists(file_path):
        print(f"[API] Archivo no encontrado: {file_path}")
        return jsonify({"error": "Archivo no encontrado"}), 404
    
    return send_file(
        file_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=safe_filename
    )


@app.route('/files', methods=['GET'])
def list_files():
    """Lista los archivos de resultados disponibles"""
    print("[API] Listando archivos de resultados")
    
    files = []
    if os.path.exists(RESULTS_FOLDER):
        for f in os.listdir(RESULTS_FOLDER):
            if f.endswith(('.xlsx', '.xls')):
                file_path = os.path.join(RESULTS_FOLDER, f)
                files.append({
                    "filename": f,
                    "size_bytes": os.path.getsize(file_path),
                    "created": datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
                })
    
    return jsonify({
        "files": files,
        "count": len(files)
    })


if __name__ == '__main__':
    print("=" * 60)
    print("  COL REPORTE VPN - API Flask")
    print("=" * 60)
    print("[API] Iniciando servidor...")
    app.run(host='0.0.0.0', port=5000, debug=True)

