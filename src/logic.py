"""
Módulo de lógica de procesamiento de archivos Excel R033 y R065
Utiliza dos hilos: uno para crear el Excel y otro para procesar y enviar a BigQuery
"""
import pandas as pd
import threading
from datetime import datetime
import os
from connections import BigQueryConnection, GCSConnection


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
    
    def _find_header_row(self, file_path, expected_headers: list, max_rows: int = 20) -> int:
        """
        Busca la fila donde se encuentran los cabezales esperados
        
        Args:
            file_path: Ruta del archivo Excel
            expected_headers: Lista de cabezales esperados
            max_rows: Máximo de filas a revisar
        
        Returns:
            Número de fila donde están los cabezales (0-indexed), -1 si no se encuentra
        """
        try:
            # Leer las primeras filas sin cabezal
            df_preview = pd.read_excel(file_path, header=None, nrows=max_rows, engine='openpyxl')
            
            for row_idx in range(len(df_preview)):
                row_values = df_preview.iloc[row_idx].astype(str).str.strip().tolist()
                
                # Verificar si la mayoría de los cabezales esperados están en esta fila
                matches = sum(1 for header in expected_headers if header in row_values)
                match_ratio = matches / len(expected_headers)
                
                if match_ratio >= 0.7:  # Al menos 70% de coincidencia
                    print(f"[HEADER] Cabezales encontrados en fila {row_idx} ({matches}/{len(expected_headers)} coincidencias)")
                    return row_idx
            
            print(f"[HEADER] No se encontraron cabezales. Usando fila 0 por defecto.")
            return 0
            
        except Exception as e:
            print(f"[HEADER] Error buscando cabezales: {str(e)}")
            return 0
    
    def load_excel_files(self, r033_file, r065_file) -> bool:
        """Carga los archivos Excel R033 y R065 en DataFrames, detectando automáticamente los cabezales"""
        try:
            # Cargar R033
            print("[LOAD] Buscando cabezales en archivo R033...")
            header_row_r033 = self._find_header_row(r033_file, HEADERS_R033)
            
            print(f"[LOAD] Cargando archivo R033 (cabezales en fila {header_row_r033})...")
            self.df_r033 = pd.read_excel(r033_file, header=header_row_r033, engine='openpyxl')
            print(f"[LOAD] R033 cargado: {len(self.df_r033)} filas, {len(self.df_r033.columns)} columnas")
            print(f"[LOAD] Columnas R033: {list(self.df_r033.columns)}")
            
            # Cargar R065
            print("[LOAD] Buscando cabezales en archivo R065...")
            header_row_r065 = self._find_header_row(r065_file, HEADERS_R065)
            
            print(f"[LOAD] Cargando archivo R065 (cabezales en fila {header_row_r065})...")
            self.df_r065 = pd.read_excel(r065_file, header=header_row_r065, engine='openpyxl')
            print(f"[LOAD] R065 cargado: {len(self.df_r065)} filas, {len(self.df_r065.columns)} columnas")
            print(f"[LOAD] Columnas R065: {list(self.df_r065.columns)}")
            
            return True
        except Exception as e:
            print(f"[LOAD] Error al cargar archivos: {str(e)}")
            self.error_occurred = True
            self.error_message = str(e)
            return False
    
    def _filter_r065(self) -> pd.DataFrame:
        """
        Filtra el DataFrame R065 por el mensaje específico
        Solo mantiene las filas donde MENSAJE = "No se encuentra en RMS el ítem para esta factura"
        """
        try:
            print(f"[FILTER] Filtrando R065 por MENSAJE = '{MENSAJE_FILTRO_R065}'")
            print(f"[FILTER] Filas antes del filtro: {len(self.df_r065)}")
            
            # Buscar la columna MENSAJE (puede tener variaciones en el nombre)
            mensaje_col = None
            for col in self.df_r065.columns:
                if 'MENSAJE' in str(col).upper():
                    mensaje_col = col
                    break
            
            if mensaje_col is None:
                print("[FILTER] ADVERTENCIA: No se encontró la columna MENSAJE en R065")
                self.df_r065_filtrado = self.df_r065.copy()
                return self.df_r065_filtrado
            
            print(f"[FILTER] Columna de mensaje encontrada: '{mensaje_col}'")
            
            # Filtrar por el mensaje específico
            self.df_r065_filtrado = self.df_r065[
                self.df_r065[mensaje_col].astype(str).str.strip() == MENSAJE_FILTRO_R065
            ].copy()
            
            print(f"[FILTER] Filas después del filtro: {len(self.df_r065_filtrado)}")
            print(f"[FILTER] Filas eliminadas: {len(self.df_r065) - len(self.df_r065_filtrado)}")
            
            return self.df_r065_filtrado
            
        except Exception as e:
            print(f"[FILTER] Error al filtrar R065: {str(e)}")
            self.df_r065_filtrado = self.df_r065.copy()
            return self.df_r065_filtrado
    
    def process_dataframes(self) -> pd.DataFrame:
        """
        Procesa y cruza los DataFrames R033 y R065
        1. Filtra R065 por el mensaje específico
        2. Crea el DataFrame de resultado
        """
        try:
            print("[PROCESS] Iniciando procesamiento de DataFrames...")
            
            # Paso 1: Filtrar R065
            self._filter_r065()
            
            if self.error_occurred:
                return pd.DataFrame()
            
            # Paso 2: Preparar DataFrames para el cruce
            df_r033_work = self.df_r033.copy()
            df_r065_work = self.df_r065_filtrado.copy()
            
            print(f"[PROCESS] R033 tiene {len(df_r033_work)} filas")
            print(f"[PROCESS] R065 filtrado tiene {len(df_r065_work)} filas")
            
            # Agregar columna de origen y timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_r033_work['origen'] = 'R033'
            df_r033_work['fecha_procesamiento'] = timestamp
            df_r065_work['origen'] = 'R065'
            df_r065_work['fecha_procesamiento'] = timestamp
            
            # Paso 3: Crear DataFrame de resultado
            # TODO: Aquí puedes agregar la lógica de cruce específica
            # Por ahora, el resultado es el R065 filtrado con las columnas adicionales
            self.df_resultado = df_r065_work.copy()
            
            print(f"[PROCESS] Procesamiento completado: {len(self.df_resultado)} filas resultantes")
            
            return self.df_resultado
            
        except Exception as e:
            print(f"[PROCESS] Error en procesamiento: {str(e)}")
            self.error_occurred = True
            self.error_message = str(e)
            return pd.DataFrame()
    
    def _thread_create_excel(self, output_path: str):
        """Hilo 1: Crea el archivo Excel de retorno"""
        try:
            print("[THREAD-EXCEL] Esperando que el procesamiento termine...")
            self.processing_complete.wait()
            
            if self.error_occurred:
                print("[THREAD-EXCEL] Error detectado, abortando creación de Excel")
                return
            
            print(f"[THREAD-EXCEL] Creando archivo Excel en: {output_path}")
            
            # Crear el archivo Excel con formato
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Hoja principal con el resultado
                self.df_resultado.to_excel(writer, sheet_name='Resultado', index=False)
                
                # Hoja con R065 filtrado
                if self.df_r065_filtrado is not None:
                    self.df_r065_filtrado.to_excel(writer, sheet_name='R065_Filtrado', index=False)
                
                # Hojas con datos originales
                if self.df_r033 is not None:
                    self.df_r033.to_excel(writer, sheet_name='R033_Original', index=False)
                if self.df_r065 is not None:
                    self.df_r065.to_excel(writer, sheet_name='R065_Original', index=False)
            
            self.excel_path = output_path
            print(f"[THREAD-EXCEL] Excel creado exitosamente: {output_path}")
            self.excel_ready.set()
            
        except Exception as e:
            print(f"[THREAD-EXCEL] Error al crear Excel: {str(e)}")
            self.error_occurred = True
            self.error_message = str(e)
            self.excel_ready.set()
    
    def _thread_process_and_upload(self, table_name: str = "reporte_vpn"):
        """Hilo 2: Procesa el DataFrame y lo envía a BigQuery"""
        try:
            print("[THREAD-BQ] Iniciando procesamiento...")
            
            # Procesar los DataFrames
            self.process_dataframes()
            
            if self.error_occurred:
                print("[THREAD-BQ] Error en procesamiento, abortando")
                self.processing_complete.set()
                return
            
            # Señalar que el procesamiento está completo
            self.processing_complete.set()
            print("[THREAD-BQ] Procesamiento completado, DataFrame listo")
            
            # Intentar conectar y subir a BigQuery
            print("[THREAD-BQ] Conectando a BigQuery...")
            if self.bq_connection.connect():
                print(f"[THREAD-BQ] Insertando datos en tabla: {table_name}")
                success = self.bq_connection.insert_dataframe(
                    self.df_resultado, 
                    table_name, 
                    if_exists="append"
                )
                if success:
                    print("[THREAD-BQ] Datos insertados exitosamente en BigQuery")
                else:
                    print("[THREAD-BQ] Error al insertar datos en BigQuery")
            else:
                print("[THREAD-BQ] No se pudo conectar a BigQuery, continuando sin subir datos")
            
        except Exception as e:
            print(f"[THREAD-BQ] Error: {str(e)}")
            self.error_occurred = True
            self.error_message = str(e)
            self.processing_complete.set()
    
    def execute(self, r033_file, r065_file, output_path: str = None, table_name: str = "reporte_vpn") -> dict:
        """
        Ejecuta el procesamiento completo usando dos hilos
        
        Args:
            r033_file: Archivo Excel R033 (path o file-like object)
            r065_file: Archivo Excel R065 (path o file-like object)
            output_path: Ruta donde guardar el Excel resultante
            table_name: Nombre de la tabla en BigQuery
        
        Returns:
            dict con el resultado del procesamiento
        """
        # Reiniciar eventos y estado
        self.processing_complete.clear()
        self.excel_ready.clear()
        self.error_occurred = False
        self.error_message = None
        
        # Generar ruta de salida si no se proporciona
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"results/reporte_vpn_{timestamp}.xlsx"
        
        # Asegurar que existe el directorio de resultados
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        print("=" * 60)
        print("[EXECUTE] Iniciando procesamiento de archivos VPN")
        print("=" * 60)
        
        # Cargar archivos Excel
        if not self.load_excel_files(r033_file, r065_file):
            return {
                "success": False,
                "error": self.error_message,
                "excel_path": None
            }
        
        # Crear los hilos
        thread_excel = threading.Thread(
            target=self._thread_create_excel, 
            args=(output_path,),
            name="Thread-Excel"
        )
        thread_bq = threading.Thread(
            target=self._thread_process_and_upload, 
            args=(table_name,),
            name="Thread-BigQuery"
        )
        
        print("[EXECUTE] Iniciando hilos de procesamiento...")
        
        # Iniciar ambos hilos
        thread_excel.start()
        thread_bq.start()
        
        # Esperar a que ambos terminen
        thread_bq.join()
        thread_excel.join()
        
        print("=" * 60)
        print("[EXECUTE] Procesamiento finalizado")
        print("=" * 60)
        
        if self.error_occurred:
            return {
                "success": False,
                "error": self.error_message,
                "excel_path": None
            }
        
        return {
            "success": True,
            "error": None,
            "excel_path": self.excel_path,
            "rows_processed": len(self.df_resultado) if self.df_resultado is not None else 0,
            "rows_r033": len(self.df_r033) if self.df_r033 is not None else 0,
            "rows_r065_original": len(self.df_r065) if self.df_r065 is not None else 0,
            "rows_r065_filtrado": len(self.df_r065_filtrado) if self.df_r065_filtrado is not None else 0
        }


# Función de conveniencia para uso directo
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
    return processor.execute(r033_file, r065_file, output_path)
