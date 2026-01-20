"""
Módulo de lógica de procesamiento de archivos Excel R033 y R065
Utiliza dos hilos: uno para crear el Excel y otro para procesar y enviar a BigQuery
"""
import pandas as pd
import threading
from datetime import datetime
import os
from connections import BigQueryConnection, GCSConnection


class ExcelProcessor:
    """Clase principal para procesar los archivos Excel R033 y R065"""
    
    def __init__(self):
        self.df_r033 = None
        self.df_r065 = None
        self.df_resultado = None
        self.excel_path = None
        self.processing_complete = threading.Event()
        self.excel_ready = threading.Event()
        self.error_occurred = False
        self.error_message = None
        
        # Conexiones a Google Cloud
        self.bq_connection = BigQueryConnection()
        self.gcs_connection = GCSConnection()
    
    def load_excel_files(self, r033_file, r065_file) -> bool:
        """Carga los archivos Excel R033 y R065 en DataFrames"""
        try:
            print("[LOAD] Cargando archivo R033...")
            self.df_r033 = pd.read_excel(r033_file, engine='openpyxl')
            print(f"[LOAD] R033 cargado: {len(self.df_r033)} filas, {len(self.df_r033.columns)} columnas")
            
            print("[LOAD] Cargando archivo R065...")
            self.df_r065 = pd.read_excel(r065_file, engine='openpyxl')
            print(f"[LOAD] R065 cargado: {len(self.df_r065)} filas, {len(self.df_r065.columns)} columnas")
            
            return True
        except Exception as e:
            print(f"[LOAD] Error al cargar archivos: {str(e)}")
            self.error_occurred = True
            self.error_message = str(e)
            return False
    
    def process_dataframes(self) -> pd.DataFrame:
        """
        Procesa y cruza los DataFrames R033 y R065
        Esta función debe ser personalizada según la lógica de negocio específica
        """
        try:
            print("[PROCESS] Iniciando procesamiento de DataFrames...")
            
            # Aquí va la lógica de cruce entre R033 y R065
            # Por ahora, se hace un merge básico que deberás personalizar
            
            # Limpieza básica de datos
            df_r033_clean = self.df_r033.copy()
            df_r065_clean = self.df_r065.copy()
            
            # Normalizar nombres de columnas (quitar espacios, minúsculas)
            df_r033_clean.columns = df_r033_clean.columns.str.strip().str.lower().str.replace(' ', '_')
            df_r065_clean.columns = df_r065_clean.columns.str.strip().str.lower().str.replace(' ', '_')
            
            print(f"[PROCESS] Columnas R033: {list(df_r033_clean.columns)}")
            print(f"[PROCESS] Columnas R065: {list(df_r065_clean.columns)}")
            
            # TODO: Aquí debes definir la columna clave para el cruce
            # Por ejemplo: 'id', 'numero_documento', 'codigo', etc.
            # Por defecto, concatenamos los dos DataFrames
            
            # Agregar columna de origen para identificar de qué archivo viene cada registro
            df_r033_clean['origen'] = 'R033'
            df_r065_clean['origen'] = 'R065'
            
            # Agregar timestamp de procesamiento
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_r033_clean['fecha_procesamiento'] = timestamp
            df_r065_clean['fecha_procesamiento'] = timestamp
            
            # Concatenar los DataFrames (modificar según lógica de negocio)
            self.df_resultado = pd.concat([df_r033_clean, df_r065_clean], ignore_index=True)
            
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
                self.df_resultado.to_excel(writer, sheet_name='Resultado', index=False)
                
                # Opcional: agregar hojas adicionales con los datos originales
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
            "rows_processed": len(self.df_resultado) if self.df_resultado is not None else 0
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

