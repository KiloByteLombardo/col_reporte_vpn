"""
Módulo de conexiones a Google Cloud Storage y BigQuery
"""
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from datetime import timedelta, datetime

# Ruta al archivo de credenciales
CREDENTIALS_FILE = "credentials.json"


def get_credentials():
    """
    Obtiene las credenciales de GCP.
    Primero busca el archivo credentials.json en varias ubicaciones.
    Si no lo encuentra, intenta usar ADC (Application Default Credentials).
    """
    print("[AUTH] Buscando credenciales...")
    
    # Buscar en diferentes ubicaciones
    possible_paths = [
        CREDENTIALS_FILE,
        os.path.join(os.path.dirname(__file__), CREDENTIALS_FILE),
        os.path.join(os.path.dirname(__file__), '..', CREDENTIALS_FILE),
        '/app/credentials.json',
        '/app/src/credentials.json',
        './credentials.json',
        '../credentials.json'
    ]
    
    # Mostrar rutas donde busca
    print(f"[AUTH] Buscando credentials.json en:")
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        exists = os.path.exists(path)
        print(f"[AUTH]   - {abs_path}: {'✓ ENCONTRADO' if exists else '✗ no existe'}")
        if exists:
            print(f"[AUTH] ✓ Usando credenciales de archivo: {abs_path}")
            try:
                return service_account.Credentials.from_service_account_file(path)
            except Exception as e:
                print(f"[AUTH] ✗ Error cargando credenciales: {str(e)}")
                continue
    
    # Si no se encontró el archivo, usar ADC
    print("[AUTH] ⚠ No se encontró credentials.json, intentando usar ADC...")
    return None  # Intentar ADC


class GCSConnection:
    """Clase para manejar conexiones a Google Cloud Storage"""
    
    def __init__(self, project_id: str = None, bucket_name: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME")
        self.client = None
        self.bucket = None
    
    def connect(self):
        """Establece conexión con GCS (usa credentials.json si no hay ADC)"""
        try:
            credentials = get_credentials()
            
            if credentials:
                self.client = storage.Client(project=self.project_id, credentials=credentials)
            else:
                self.client = storage.Client(project=self.project_id)
            
            self.bucket = self.client.bucket(self.bucket_name)
            print(f"[GCS] Conexión establecida con bucket: {self.bucket_name}")
            return True
        except Exception as e:
            print(f"[GCS] Error al conectar: {str(e)}")
            return False
    
    def upload_file(self, source_path: str, destination_blob_name: str) -> bool:
        """Sube un archivo a GCS"""
        try:
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_path)
            print(f"[GCS] Archivo subido: {destination_blob_name}")
            return True
        except Exception as e:
            print(f"[GCS] Error al subir archivo: {str(e)}")
            return False
    
    def download_file(self, blob_name: str, destination_path: str) -> bool:
        """Descarga un archivo de GCS"""
        try:
            blob = self.bucket.blob(blob_name)
            blob.download_to_filename(destination_path)
            print(f"[GCS] Archivo descargado: {destination_path}")
            return True
        except Exception as e:
            print(f"[GCS] Error al descargar archivo: {str(e)}")
            return False
    
    def upload_dataframe_as_excel(self, df: pd.DataFrame, destination_blob_name: str, temp_path: str = "temp_upload.xlsx") -> bool:
        """Sube un DataFrame como archivo Excel a GCS"""
        try:
            df.to_excel(temp_path, index=False, engine='openpyxl')
            result = self.upload_file(temp_path, destination_blob_name)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return result
        except Exception as e:
            print(f"[GCS] Error al subir DataFrame como Excel: {str(e)}")
            return False
    
    def blob_exists(self, blob_name: str) -> bool:
        """Verifica si un blob existe en el bucket"""
        try:
            blob = self.bucket.blob(blob_name)
            return blob.exists()
        except Exception as e:
            print(f"[GCS] Error al verificar blob: {str(e)}")
            return False
    
    def list_files(self, prefix: str = None) -> list:
        """
        Lista los archivos en el bucket
        
        Args:
            prefix: Prefijo para filtrar archivos (ej: 'reportes/')
        
        Returns:
            Lista de diccionarios con información de cada archivo
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            files = []
            for blob in blobs:
                files.append({
                    "name": blob.name,
                    "size_bytes": blob.size,
                    "created": blob.time_created.isoformat() if blob.time_created else None,
                    "updated": blob.updated.isoformat() if blob.updated else None,
                    "content_type": blob.content_type
                })
            print(f"[GCS] Archivos listados: {len(files)}")
            return files
        except Exception as e:
            print(f"[GCS] Error al listar archivos: {str(e)}")
            return []
    
    def get_signed_url(self, blob_name: str, expiration_hours: int = 1) -> str:
        """
        Genera una URL firmada para descargar un archivo
        
        Args:
            blob_name: Nombre del blob
            expiration_hours: Horas de validez de la URL
        
        Returns:
            URL firmada o None si hay error
        """
        try:
            blob = self.bucket.blob(blob_name)
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(hours=expiration_hours),
                method="GET"
            )
            print(f"[GCS] URL firmada generada para: {blob_name}")
            return url
        except Exception as e:
            print(f"[GCS] Error al generar URL firmada: {str(e)}")
            return None
    
    def get_public_url(self, blob_name: str) -> str:
        """
        Retorna la URL pública de un blob (solo funciona si el bucket es público)
        
        Args:
            blob_name: Nombre del blob
        
        Returns:
            URL pública del archivo
        """
        return f"https://storage.googleapis.com/{self.bucket_name}/{blob_name}"


class BigQueryConnection:
    """Clase para manejar conexiones a BigQuery"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None, table_name: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.dataset_id = dataset_id or os.getenv("BQ_DATASET_ID")
        self.table_name = table_name or os.getenv("BQ_TABLE_NAME", "reporte_vpn")
        self.client = None
    
    def connect(self):
        """Establece conexión con BigQuery (usa credentials.json si no hay ADC)"""
        try:
            credentials = get_credentials()
            
            if credentials:
                self.client = bigquery.Client(project=self.project_id, credentials=credentials)
            else:
                self.client = bigquery.Client(project=self.project_id)
            
            print(f"[BigQuery] Conexión establecida con proyecto: {self.project_id}")
            return True
        except Exception as e:
            print(f"[BigQuery] Error al conectar: {str(e)}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """
        Inserta un DataFrame en una tabla de BigQuery
        
        Args:
            df: DataFrame a insertar
            table_name: Nombre de la tabla
            if_exists: 'append', 'replace' o 'fail'
        """
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            job_config = bigquery.LoadJobConfig()
            
            if if_exists == "replace":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif if_exists == "append":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
            
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Espera a que termine el job
            
            print(f"[BigQuery] DataFrame insertado en tabla: {table_name}")
            print(f"[BigQuery] Filas insertadas: {len(df)}")
            return True
        except Exception as e:
            print(f"[BigQuery] Error al insertar DataFrame: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Ejecuta una query y retorna el resultado como DataFrame"""
        try:
            print(f"[BigQuery] Ejecutando query...")
            result = self.client.query(query).to_dataframe()
            print(f"[BigQuery] Query ejecutada, filas retornadas: {len(result)}")
            return result
        except Exception as e:
            print(f"[BigQuery] Error al ejecutar query: {str(e)}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

