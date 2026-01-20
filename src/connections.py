"""
Módulo de conexiones a Google Cloud Storage y BigQuery
"""
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os


class GCSConnection:
    """Clase para manejar conexiones a Google Cloud Storage"""
    
    def __init__(self, project_id: str = None, bucket_name: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME")
        self.client = None
        self.bucket = None
    
    def connect(self):
        """Establece conexión con GCS"""
        try:
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


class BigQueryConnection:
    """Clase para manejar conexiones a BigQuery"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.dataset_id = dataset_id or os.getenv("BQ_DATASET_ID")
        self.client = None
    
    def connect(self):
        """Establece conexión con BigQuery"""
        try:
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

