# COL Reporte VPN

API Flask para el procesamiento y cruce de reportes VPN (R033 y R065). Recibe dos archivos Excel, los procesa, cruza la información y genera un reporte consolidado que se almacena en Google Cloud Storage y BigQuery.

## 📋 Descripción

Este servicio automatiza el proceso de:

1. **Recepción** de dos archivos Excel (R033 y R065)
2. **Detección automática** de cabezales en ambos archivos
3. **Filtrado** del R065 por mensajes específicos de error
4. **Cruce de datos** entre R033 y R065 usando Orden de Compra
5. **Generación** de un Excel con hojas de Resultado y Resumen
6. **Almacenamiento** en Google Cloud Storage organizado por fecha
7. **Carga** de datos procesados a BigQuery

## 🏗️ Arquitectura

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Cliente       │────▶│   Flask API     │────▶│   GCS Bucket    │
│   (Excel R033   │     │   /process      │     │   /reportes/    │
│    + R065)      │     │                 │     │   YYYY-MM-DD/   │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │    BigQuery     │
                        │    Dataset      │
                        └─────────────────┘
```

## 🚀 Endpoints

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `GET` | `/health` | Health check del servicio |
| `POST` | `/process` | Procesa archivos R033 y R065 |
| `GET` | `/files` | Lista archivos en el bucket |
| `GET` | `/download/<filename>` | Descarga archivo (carpeta de hoy) |
| `GET` | `/download/<fecha>/<filename>` | Descarga archivo de fecha específica |

### POST /process

Recibe dos archivos Excel mediante `multipart/form-data`:

```bash
curl -X POST http://localhost:5000/process \
  -F "r033=@R033_archivo.xlsx" \
  -F "r065=@R065_archivo.xlsx"
```

**Respuesta exitosa:**
```json
{
  "success": true,
  "message": "Procesamiento completado exitosamente",
  "rows_processed": 150,
  "filename": "reporte_vpn_20260121_143022.xlsx",
  "gcs_path": "reportes/2026-01-21/reporte_vpn_20260121_143022.xlsx",
  "download_url": "https://storage.googleapis.com/...",
  "timestamp": "20260121_143022"
}
```

## 📦 Requisitos

- Python 3.11+
- Docker & Docker Compose (para desarrollo local)
- Cuenta de Google Cloud Platform con:
  - Cloud Storage bucket
  - BigQuery dataset y tabla
  - Service Account con permisos

## ⚙️ Configuración

### Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
GCP_PROJECT_ID=tu-proyecto-gcp
GCS_BUCKET_NAME=nombre-del-bucket
BQ_DATASET_ID=nombre_dataset
BQ_TABLE_NAME=nombre_tabla
```

### Credenciales GCP

El servicio busca credenciales en el siguiente orden:

1. **Application Default Credentials (ADC)** - Recomendado para Cloud Run
2. **Archivo `credentials.json`** - Para desarrollo local

Para desarrollo local, colocar el archivo `credentials.json` en la raíz del proyecto.

## 🐳 Desarrollo Local

### Con Docker Compose

```bash
# Construir e iniciar el servicio
docker-compose up --build

# Ver logs
docker-compose logs -f

# Detener
docker-compose down
```

El servicio estará disponible en `http://localhost:5000`

### Sin Docker

```bash
# Crear entorno virtual
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
cd src
python api.py
```

## ☁️ Despliegue en Cloud Run

### Prerrequisitos

1. Tener `gcloud` CLI instalado y configurado
2. Tener Docker instalado
3. Service Account con los siguientes roles:
   - `roles/storage.objectAdmin` (Cloud Storage)
   - `roles/bigquery.dataEditor` (BigQuery)

### Desplegar

```powershell
# Ejecutar script de despliegue
.\deploy.ps1

# Con parámetros personalizados
.\deploy.ps1 -ProjectId "mi-proyecto" -Region "us-east1"
```

### Parámetros del Script

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `-ProjectId` | `gtf-cxp` | ID del proyecto GCP |
| `-ServiceName` | `col-reporte-vpn` | Nombre del servicio |
| `-Region` | `us-central1` | Región de despliegue |
| `-Memory` | `512Mi` | Memoria asignada |
| `-Cpu` | `1` | CPUs asignadas |
| `-MinInstances` | `0` | Instancias mínimas |
| `-MaxInstances` | `10` | Instancias máximas |

## 📊 Estructura del Excel Generado

### Hoja "Resultado"

Contiene los datos filtrados y cruzados con las siguientes columnas:

| Columna | Origen |
|---------|--------|
| ORDEN COMPRA | R065 |
| NRO FACTURA | R065 |
| ID PROVEEDOR | R065 |
| NOMBRE PROVEEDOR | R065 |
| MENSAJE | R065 |
| ... (demás columnas R065) | R065 |
| Centro de Costo | R033 (Tienda) |
| Estatus R033 | R033 (Estatus) |
| Grupo de Pago | Calculado |

### Hoja "Resumen"

Tabla agregada por proveedor:

| Columna | Descripción |
|---------|-------------|
| Proveedor | Nombre del proveedor padre |
| Número de Facturas | Facturas únicas |
| Monto Total | Suma de totales (sin duplicados) |
| Número de Ítems | Ítems únicos |

## 🗄️ Esquema BigQuery

```sql
col_vpn_orden_compra          STRING
col_vpn_nro_factura           STRING
col_vpn_id_proveedor          STRING
col_vpn_nombre_proveedor      STRING
col_vpn_mensaje               STRING
col_vpn_item_1                STRING
col_vpn_item_2                STRING
col_vpn_column_vpn            STRING
col_vpn_item_descripcion      STRING
col_vpn_fecha_creacion        DATETIME
col_vpn_nombre_archivo        STRING
col_vpn_estado_factura        STRING
col_vpn_id_proveedor_padre    STRING
col_vpn_nombre_proveedor_padre STRING
col_vpn_fecha_factura         DATE
col_vpn_subtotal              FLOAT
col_vpn_impuesto              FLOAT
col_vpn_total                 FLOAT
col_vpn_centro_costo          STRING
col_vpn_estatus_r033          STRING
col_vpn_grupo_pago            STRING
col_vpn_timestamp             TIMESTAMP
```

## 📁 Estructura del Proyecto

```
col_reporte_vpn/
├── src/
│   ├── api.py           # Flask API endpoints
│   ├── connections.py   # Conexiones GCS y BigQuery
│   └── logic.py         # Lógica de procesamiento
├── results/             # Resultados locales (desarrollo)
├── Dockerfile           # Imagen Docker
├── docker-compose.yml   # Configuración desarrollo local
├── deploy.ps1           # Script despliegue Cloud Run
├── requirements.txt     # Dependencias Python
├── .env                 # Variables de entorno (no versionado)
├── credentials.json     # Credenciales GCP (no versionado)
└── README.md
```

## 🔒 Seguridad

Los siguientes archivos **NO deben** ser versionados:

- `.env` - Variables de entorno
- `credentials.json` - Credenciales de servicio GCP

Estos archivos están incluidos en `.gitignore`.

## 📝 Licencia

Proyecto interno - Todos los derechos reservados.
