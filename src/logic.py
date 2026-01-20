"""
Módulo de lógica de procesamiento de archivos Excel R033 y R065
Utiliza dos hilos: uno para crear el Excel y otro para procesar y enviar a BigQuery
"""
import pandas as pd
import threading
from datetime import datetime
import os
from connections import BigQueryConnection, GCSConnection


# =============================================================================
# CONSTANTES
# =============================================================================

# Cabezales esperados para cada archivo
HEADERS_R033 = [
    "Orden de Compra", "Código Proveedor", "Sucursal Proveedor", "Proveedor",
    "Cód. Tienda", "Tienda", "Estatus", "Días Condición (RMS)", "Unidades Recibidas",
    "Documento", "Recepción", "Diferencia AP", "Saldo Herramienta", 
    "Fecha Recepción", "Termino de Plazo"
]

HEADERS_R065 = [
    "ORDEN COMPRA", "NRO FACTURA", "ID PROVEEDOR", "NOMBRE PROVEEDOR", "MENSAJE",
    "ITEM 1", "ITEM 2", "VPN", "ITEM DESCRIPCION", "FECHA CREACION", 
    "NOMBRE ARCHIVO", "ESTADO FACTURA", "ID PROVEEDOR PADRE", "NOMBRE PROVEEDOR PADRE",
    "FECHA FACTURA", "SUBTTOTAL", "IMPUESTO", "TOTAL"
]

# Mensaje para filtrar en R065
MENSAJE_FILTRO_R065 = "No se encuentra en RMS el ítem para esta factura"


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class ExcelProcessor:
    """Clase principal para procesar los archivos Excel R033 y R065"""
    
    def __init__(self):
        self.df_r033 = None
        self.df_r065 = None
        self.df_r065_filtrado = None
        self.df_resultado = None
        self.excel_path = None
        self.processing_complete = threading.Event()
        self.excel_ready = threading.Event()
        self.error_occurred = False
        self.error_message = None
        
        # Conexiones a Google Cloud
        self.bq_connection = BigQueryConnection()
        self.gcs_connection = GCSConnection()
    
    # =========================================================================
    # PASO 1: ENCONTRAR CABEZALES
    # =========================================================================
    
    def find_header_row(self, file_path, expected_headers: list, max_rows: int = 20) -> int:
        """
        Busca la fila donde se encuentran los cabezales esperados
        
        Args:
            file_path: Ruta del archivo Excel
            expected_headers: Lista de cabezales esperados
            max_rows: Máximo de filas a revisar
        
        Returns:
            Número de fila donde están los cabezales (0-indexed), -1 si no se encuentra
        """
        print(f"[PASO 1 - CABEZALES] Buscando cabezales en archivo...")
        
        try:
            # Leer las primeras filas sin cabezal
            df_preview = pd.read_excel(file_path, header=None, nrows=max_rows, engine='openpyxl')
            
            for row_idx in range(len(df_preview)):
                row_values = df_preview.iloc[row_idx].astype(str).str.strip().tolist()
                
                # Verificar si la mayoría de los cabezales esperados están en esta fila
                matches = sum(1 for header in expected_headers if header in row_values)
                match_ratio = matches / len(expected_headers)
                
                if match_ratio >= 0.7:  # Al menos 70% de coincidencia
                    print(f"[PASO 1 - CABEZALES] ✓ Cabezales encontrados en fila {row_idx} ({matches}/{len(expected_headers)} coincidencias)")
                    return row_idx
            
            print(f"[PASO 1 - CABEZALES] ⚠ No se encontraron cabezales. Usando fila 0 por defecto.")
            return 0
            
        except Exception as e:
            print(f"[PASO 1 - CABEZALES] ✗ Error buscando cabezales: {str(e)}")
            return 0
    
    # =========================================================================
    # PASO 2: CARGAR ARCHIVOS
    # =========================================================================
    
    def load_r033(self, file_path) -> bool:
        """Carga el archivo R033 en un DataFrame"""
        print(f"\n[PASO 2A - CARGAR R033] Iniciando carga...")
        
        try:
            header_row = self.find_header_row(file_path, HEADERS_R033)
            self.df_r033 = pd.read_excel(file_path, header=header_row, engine='openpyxl')
            
            print(f"[PASO 2A - CARGAR R033] ✓ Archivo cargado")
            print(f"[PASO 2A - CARGAR R033]   - Filas: {len(self.df_r033)}")
            print(f"[PASO 2A - CARGAR R033]   - Columnas: {len(self.df_r033.columns)}")
            print(f"[PASO 2A - CARGAR R033]   - Nombres: {list(self.df_r033.columns)}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 2A - CARGAR R033] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error cargando R033: {str(e)}"
            return False
    
    def load_r065(self, file_path) -> bool:
        """Carga el archivo R065 en un DataFrame"""
        print(f"\n[PASO 2B - CARGAR R065] Iniciando carga...")
        
        try:
            header_row = self.find_header_row(file_path, HEADERS_R065)
            self.df_r065 = pd.read_excel(file_path, header=header_row, engine='openpyxl')
            
            print(f"[PASO 2B - CARGAR R065] ✓ Archivo cargado")
            print(f"[PASO 2B - CARGAR R065]   - Filas: {len(self.df_r065)}")
            print(f"[PASO 2B - CARGAR R065]   - Columnas: {len(self.df_r065.columns)}")
            print(f"[PASO 2B - CARGAR R065]   - Nombres: {list(self.df_r065.columns)}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 2B - CARGAR R065] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error cargando R065: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 3: FILTRAR R065
    # =========================================================================
    
    def filter_r065(self) -> bool:
        """
        Filtra el DataFrame R065 por el mensaje específico
        Solo mantiene las filas donde MENSAJE = "No se encuentra en RMS el ítem para esta factura"
        """
        print(f"\n[PASO 3 - FILTRAR R065] Iniciando filtrado...")
        print(f"[PASO 3 - FILTRAR R065] Filtro: MENSAJE = '{MENSAJE_FILTRO_R065}'")
        
        try:
            filas_antes = len(self.df_r065)
            
            # Buscar la columna MENSAJE
            mensaje_col = None
            for col in self.df_r065.columns:
                if 'MENSAJE' in str(col).upper():
                    mensaje_col = col
                    break
            
            if mensaje_col is None:
                print("[PASO 3 - FILTRAR R065] ⚠ No se encontró la columna MENSAJE")
                self.df_r065_filtrado = self.df_r065.copy()
                return True
            
            print(f"[PASO 3 - FILTRAR R065] Columna encontrada: '{mensaje_col}'")
            
            # Aplicar filtro
            self.df_r065_filtrado = self.df_r065[
                self.df_r065[mensaje_col].astype(str).str.strip() == MENSAJE_FILTRO_R065
            ].copy()
            
            filas_despues = len(self.df_r065_filtrado)
            filas_eliminadas = filas_antes - filas_despues
            
            print(f"[PASO 3 - FILTRAR R065] ✓ Filtrado completado")
            print(f"[PASO 3 - FILTRAR R065]   - Filas antes: {filas_antes}")
            print(f"[PASO 3 - FILTRAR R065]   - Filas después: {filas_despues}")
            print(f"[PASO 3 - FILTRAR R065]   - Filas eliminadas: {filas_eliminadas}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 3 - FILTRAR R065] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error filtrando R065: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 4: PROCESAR Y CRUZAR DATAFRAMES
    # =========================================================================
    
    def process_and_merge(self) -> bool:
        """
        Procesa y cruza los DataFrames R033 y R065 filtrado
        Crea el DataFrame de resultado final
        """
        print(f"\n[PASO 4 - PROCESAR] Iniciando procesamiento y cruce...")
        
        try:
            # Preparar DataFrames
            df_r033_work = self.df_r033.copy()
            df_r065_work = self.df_r065_filtrado.copy()
            
            # Agregar metadatos
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_r033_work['origen'] = 'R033'
            df_r033_work['fecha_procesamiento'] = timestamp
            df_r065_work['origen'] = 'R065'
            df_r065_work['fecha_procesamiento'] = timestamp
            
            print(f"[PASO 4 - PROCESAR] R033 preparado: {len(df_r033_work)} filas")
            print(f"[PASO 4 - PROCESAR] R065 filtrado preparado: {len(df_r065_work)} filas")
            
            # TODO: Aquí va la lógica de cruce específica
            # Por ahora, el resultado es el R065 filtrado
            self.df_resultado = df_r065_work.copy()
            
            print(f"[PASO 4 - PROCESAR] ✓ Procesamiento completado")
            print(f"[PASO 4 - PROCESAR]   - Filas en resultado: {len(self.df_resultado)}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 4 - PROCESAR] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error en procesamiento: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 5: CREAR EXCEL (HILO 1)
    # =========================================================================
    
    def create_excel(self, output_path: str) -> bool:
        """Crea el archivo Excel de retorno con múltiples hojas"""
        print(f"\n[PASO 5 - CREAR EXCEL] Iniciando creación...")
        print(f"[PASO 5 - CREAR EXCEL] Ruta: {output_path}")
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Hoja principal
                self.df_resultado.to_excel(writer, sheet_name='Resultado', index=False)
                print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'Resultado' creada")
                
                # Hoja R065 filtrado
                if self.df_r065_filtrado is not None:
                    self.df_r065_filtrado.to_excel(writer, sheet_name='R065_Filtrado', index=False)
                    print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R065_Filtrado' creada")
                
                # Hojas originales
                if self.df_r033 is not None:
                    self.df_r033.to_excel(writer, sheet_name='R033_Original', index=False)
                    print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R033_Original' creada")
                
                if self.df_r065 is not None:
                    self.df_r065.to_excel(writer, sheet_name='R065_Original', index=False)
                    print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R065_Original' creada")
            
            self.excel_path = output_path
            print(f"[PASO 5 - CREAR EXCEL] ✓ Excel creado exitosamente")
            
            return True
            
        except Exception as e:
            print(f"[PASO 5 - CREAR EXCEL] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error creando Excel: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 6: SUBIR A BIGQUERY (HILO 2)
    # =========================================================================
    
    def upload_to_bigquery(self, table_name: str = "reporte_vpn") -> bool:
        """Sube el DataFrame de resultado a BigQuery"""
        print(f"\n[PASO 6 - BIGQUERY] Iniciando subida...")
        print(f"[PASO 6 - BIGQUERY] Tabla destino: {table_name}")
        
        try:
            if not self.bq_connection.connect():
                print("[PASO 6 - BIGQUERY] ⚠ No se pudo conectar, continuando sin subir")
                return False
            
            success = self.bq_connection.insert_dataframe(
                self.df_resultado, 
                table_name, 
                if_exists="append"
            )
            
            if success:
                print(f"[PASO 6 - BIGQUERY] ✓ Datos insertados: {len(self.df_resultado)} filas")
            else:
                print("[PASO 6 - BIGQUERY] ✗ Error al insertar datos")
            
            return success
            
        except Exception as e:
            print(f"[PASO 6 - BIGQUERY] ✗ Error: {str(e)}")
            return False
    
    # =========================================================================
    # FUNCIONES DE HILOS
    # =========================================================================
    
    def _thread_excel_worker(self, output_path: str):
        """Hilo 1: Espera el procesamiento y crea el Excel"""
        print("\n[HILO-EXCEL] Iniciado, esperando procesamiento...")
        self.processing_complete.wait()
        
        if self.error_occurred:
            print("[HILO-EXCEL] Error detectado, abortando")
            self.excel_ready.set()
            return
        
        self.create_excel(output_path)
        self.excel_ready.set()
        print("[HILO-EXCEL] Finalizado")
    
    def _thread_bq_worker(self, table_name: str):
        """Hilo 2: Procesa los datos y los sube a BigQuery"""
        print("\n[HILO-BQ] Iniciado")
        
        # Ejecutar pasos 3 y 4
        if not self.filter_r065():
            self.processing_complete.set()
            return
        
        if not self.process_and_merge():
            self.processing_complete.set()
            return
        
        # Señalar que el procesamiento terminó
        self.processing_complete.set()
        print("[HILO-BQ] Procesamiento completado, señal enviada")
        
        # Subir a BigQuery
        self.upload_to_bigquery(table_name)
        print("[HILO-BQ] Finalizado")
    
    def start_threads(self, output_path: str, table_name: str):
        """Inicia los dos hilos de procesamiento"""
        print("\n[HILOS] Iniciando hilos de procesamiento...")
        
        thread_excel = threading.Thread(
            target=self._thread_excel_worker, 
            args=(output_path,),
            name="Thread-Excel"
        )
        thread_bq = threading.Thread(
            target=self._thread_bq_worker, 
            args=(table_name,),
            name="Thread-BigQuery"
        )
        
        thread_excel.start()
        thread_bq.start()
        
        print("[HILOS] Esperando a que los hilos terminen...")
        thread_bq.join()
        thread_excel.join()
        
        print("[HILOS] ✓ Todos los hilos finalizados")
    
    # =========================================================================
    # FUNCIÓN PRINCIPAL (MAIN)
    # =========================================================================
    
    def main(self, r033_file, r065_file, output_path: str = None, table_name: str = "reporte_vpn") -> dict:
        """
        FUNCIÓN PRINCIPAL - Orquesta todo el flujo de procesamiento
        
        Flujo:
            1. Inicialización y validación
            2. Cargar R033 (find_header_row + load_r033)
            3. Cargar R065 (find_header_row + load_r065)
            4. Iniciar hilos:
               - Hilo 1: Espera → Crear Excel
               - Hilo 2: Filtrar R065 → Procesar → Subir a BigQuery
            5. Retornar resultado
        
        Args:
            r033_file: Archivo Excel R033 (path o file-like object)
            r065_file: Archivo Excel R065 (path o file-like object)
            output_path: Ruta donde guardar el Excel resultante
            table_name: Nombre de la tabla en BigQuery
        
        Returns:
            dict con el resultado del procesamiento
        """
        print("\n" + "=" * 70)
        print("  INICIO DEL PROCESAMIENTO - REPORTE VPN")
        print("=" * 70)
        
        # ---------------------------------------------------------------------
        # PASO 0: INICIALIZACIÓN
        # ---------------------------------------------------------------------
        print("\n[PASO 0 - INIT] Inicializando...")
        
        self.processing_complete.clear()
        self.excel_ready.clear()
        self.error_occurred = False
        self.error_message = None
        
        # Generar ruta de salida si no se proporciona
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"results/reporte_vpn_{timestamp}.xlsx"
        
        # Asegurar que existe el directorio
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        print(f"[PASO 0 - INIT] ✓ Output path: {output_path}")
        print(f"[PASO 0 - INIT] ✓ Tabla BigQuery: {table_name}")
        
        # ---------------------------------------------------------------------
        # PASO 1-2: CARGAR ARCHIVOS
        # ---------------------------------------------------------------------
        if not self.load_r033(r033_file):
            return self._build_error_response()
        
        if not self.load_r065(r065_file):
            return self._build_error_response()
        
        # ---------------------------------------------------------------------
        # PASOS 3-6: EJECUTAR EN HILOS
        # ---------------------------------------------------------------------
        self.start_threads(output_path, table_name)
        
        # ---------------------------------------------------------------------
        # RESULTADO FINAL
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("  FIN DEL PROCESAMIENTO")
        print("=" * 70)
        
        if self.error_occurred:
            return self._build_error_response()
        
        return self._build_success_response()
    
    def _build_error_response(self) -> dict:
        """Construye la respuesta de error"""
        print(f"\n[RESULTADO] ✗ ERROR: {self.error_message}")
        return {
            "success": False,
            "error": self.error_message,
            "excel_path": None
        }
    
    def _build_success_response(self) -> dict:
        """Construye la respuesta de éxito"""
        response = {
            "success": True,
            "error": None,
            "excel_path": self.excel_path,
            "rows_processed": len(self.df_resultado) if self.df_resultado is not None else 0,
            "rows_r033": len(self.df_r033) if self.df_r033 is not None else 0,
            "rows_r065_original": len(self.df_r065) if self.df_r065 is not None else 0,
            "rows_r065_filtrado": len(self.df_r065_filtrado) if self.df_r065_filtrado is not None else 0
        }
        
        print(f"\n[RESULTADO] ✓ ÉXITO")
        print(f"[RESULTADO]   - Excel: {response['excel_path']}")
        print(f"[RESULTADO]   - Filas procesadas: {response['rows_processed']}")
        print(f"[RESULTADO]   - R033: {response['rows_r033']} filas")
        print(f"[RESULTADO]   - R065 original: {response['rows_r065_original']} filas")
        print(f"[RESULTADO]   - R065 filtrado: {response['rows_r065_filtrado']} filas")
        
        return response
    
    # =========================================================================
    # MÉTODO LEGACY (mantiene compatibilidad)
    # =========================================================================
    
    def execute(self, r033_file, r065_file, output_path: str = None, table_name: str = "reporte_vpn") -> dict:
        """Alias de main() para mantener compatibilidad"""
        return self.main(r033_file, r065_file, output_path, table_name)


# =============================================================================
# FUNCIÓN DE CONVENIENCIA
# =============================================================================

def process_vpn_reports(r033_file, r065_file, output_path: str = None) -> dict:
    """
    Función de conveniencia para procesar reportes VPN
    
    Args:
        r033_file: Archivo R033
        r065_file: Archivo R065
        output_path: Ruta de salida del Excel
    
    Returns:
        dict con resultado del procesamiento
    """
    processor = ExcelProcessor()
    return processor.main(r033_file, r065_file, output_path)
