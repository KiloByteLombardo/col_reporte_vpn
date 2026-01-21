"""
Módulo de lógica de procesamiento de archivos Excel R033 y R065
Utiliza dos hilos: uno para crear el Excel y otro para procesar y enviar a BigQuery
"""
import pandas as pd
import threading
from datetime import datetime
import os
import pytz
from connections import BigQueryConnection, GCSConnection

# Zona horaria de Venezuela (Caracas) - UTC-4
TIMEZONE_CARACAS = pytz.timezone('America/Caracas')


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

# Mapeo de columnas del DataFrame a BigQuery
COLUMN_MAPPING_BQ = {
    "ORDEN COMPRA": "col_vpn_orden_compra",
    "NRO FACTURA": "col_vpn_nro_factura",
    "ID PROVEEDOR": "col_vpn_id_proveedor",
    "NOMBRE PROVEEDOR": "col_vpn_nombre_proveedor",
    "MENSAJE": "col_vpn_mensaje",
    "ITEM 1": "col_vpn_item_1",
    "ITEM 2": "col_vpn_item_2",
    "VPN": "col_vpn_column_vpn",
    "ITEM DESCRIPCION": "col_vpn_item_descripcion",
    "FECHA CREACION": "col_vpn_fecha_creacion",
    "NOMBRE ARCHIVO": "col_vpn_nombre_archivo",
    "ESTADO FACTURA": "col_vpn_estado_factura",
    "ID PROVEEDOR PADRE": "col_vpn_id_proveedor_padre",
    "NOMBRE PROVEEDOR PADRE": "col_vpn_nombre_proveedor_padre",
    "FECHA FACTURA": "col_vpn_fecha_factura",
    "SUBTTOTAL": "col_vpn_subtotal",
    "IMPUESTO": "col_vpn_impuesto",
    "TOTAL": "col_vpn_total",
    "Centro de Costo": "col_vpn_centro_costo",
    "Estatus R033": "col_vpn_estatus_r033",
    "Grupo de Pago": "col_vpn_grupo_pago"
}

# Columnas numéricas que deben ser FLOAT
FLOAT_COLUMNS_BQ = ["col_vpn_subtotal", "col_vpn_impuesto", "col_vpn_total"]

# Columnas de fecha
DATE_COLUMNS_BQ = {
    "col_vpn_fecha_creacion": "datetime",  # DATETIME
    "col_vpn_fecha_factura": "date"        # DATE
}


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
    # PASO 1-2: CARGAR ARCHIVOS (OPTIMIZADO - UNA SOLA LECTURA)
    # =========================================================================
    
    def _find_header_in_dataframe(self, df: pd.DataFrame, expected_headers: list) -> int:
        """
        Busca la fila de cabezales en un DataFrame ya cargado (sin releer el archivo)
        """
        max_rows = min(20, len(df))
        
        for row_idx in range(max_rows):
            row_values = df.iloc[row_idx].astype(str).str.strip().tolist()
            matches = sum(1 for header in expected_headers if header in row_values)
            match_ratio = matches / len(expected_headers)
            
            if match_ratio >= 0.7:
                return row_idx
        
        return 0
    
    def load_r033(self, file_path) -> bool:
        """Carga el archivo R033 en un DataFrame (lectura única optimizada)"""
        print(f"\n[PASO 2A - CARGAR R033] Iniciando carga...")
        start_time = datetime.now()
        
        try:
            # Leer todo el archivo de una vez sin cabezal
            print(f"[PASO 2A - CARGAR R033] Leyendo archivo...")
            df_raw = pd.read_excel(file_path, header=None, engine='openpyxl')
            
            # Encontrar fila de cabezales en memoria
            header_row = self._find_header_in_dataframe(df_raw, HEADERS_R033)
            print(f"[PASO 2A - CARGAR R033] Cabezales en fila {header_row}")
            
            # Ajustar DataFrame: usar fila como cabezal y eliminar filas anteriores
            if header_row > 0:
                df_raw.columns = df_raw.iloc[header_row]
                self.df_r033 = df_raw.iloc[header_row + 1:].reset_index(drop=True)
            else:
                df_raw.columns = df_raw.iloc[0]
                self.df_r033 = df_raw.iloc[1:].reset_index(drop=True)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"[PASO 2A - CARGAR R033] ✓ Cargado en {elapsed:.2f}s")
            print(f"[PASO 2A - CARGAR R033]   - Filas: {len(self.df_r033)}")
            print(f"[PASO 2A - CARGAR R033]   - Columnas: {len(self.df_r033.columns)}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 2A - CARGAR R033] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error cargando R033: {str(e)}"
            return False
    
    def load_r065(self, file_path) -> bool:
        """Carga el archivo R065 en un DataFrame (lectura única optimizada)"""
        print(f"\n[PASO 2B - CARGAR R065] Iniciando carga...")
        start_time = datetime.now()
        
        try:
            # Leer todo el archivo de una vez sin cabezal
            print(f"[PASO 2B - CARGAR R065] Leyendo archivo...")
            df_raw = pd.read_excel(file_path, header=None, engine='openpyxl')
            
            # Encontrar fila de cabezales en memoria
            header_row = self._find_header_in_dataframe(df_raw, HEADERS_R065)
            print(f"[PASO 2B - CARGAR R065] Cabezales en fila {header_row}")
            
            # Ajustar DataFrame
            if header_row > 0:
                df_raw.columns = df_raw.iloc[header_row]
                self.df_r065 = df_raw.iloc[header_row + 1:].reset_index(drop=True)
            else:
                df_raw.columns = df_raw.iloc[0]
                self.df_r065 = df_raw.iloc[1:].reset_index(drop=True)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"[PASO 2B - CARGAR R065] ✓ Cargado en {elapsed:.2f}s")
            print(f"[PASO 2B - CARGAR R065]   - Filas: {len(self.df_r065)}")
            print(f"[PASO 2B - CARGAR R065]   - Columnas: {len(self.df_r065.columns)}")
            
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
    
    def _find_column(self, df: pd.DataFrame, possible_names: list, exact_match: bool = False) -> str:
        """
        Busca una columna en el DataFrame que coincida con alguno de los nombres posibles
        
        Args:
            df: DataFrame donde buscar
            possible_names: Lista de posibles nombres de columna
            exact_match: Si True, busca coincidencia exacta. Si False, busca si contiene el nombre.
        
        Returns:
            Nombre de la columna encontrada o None
        """
        for col in df.columns:
            col_clean = str(col).strip()
            col_upper = col_clean.upper()
            for name in possible_names:
                name_upper = name.upper().strip()
                if exact_match:
                    # Coincidencia exacta (ignorando mayúsculas/minúsculas)
                    if col_upper == name_upper:
                        return col
                else:
                    # Coincidencia parcial
                    if name_upper in col_upper:
                        return col
        return None
    
    def process_and_merge(self) -> bool:
        """
        Procesa y cruza los DataFrames R033 y R065 filtrado
        
        Lógica:
        1. Toma el R065 filtrado como base
        2. Crea la columna "Centro de Costo"
        3. Hace un cruce por Orden de Compra con R033
        4. Trae el Centro de Costo del R033
        """
        print(f"\n[PASO 4 - PROCESAR] Iniciando procesamiento y cruce...")
        
        try:
            # Preparar DataFrames
            df_r033_work = self.df_r033.copy()
            df_r065_work = self.df_r065_filtrado.copy()
            
            print(f"[PASO 4 - PROCESAR] R033: {len(df_r033_work)} filas")
            print(f"[PASO 4 - PROCESAR] R065 filtrado: {len(df_r065_work)} filas")
            
            # -----------------------------------------------------------------
            # PASO 4.1: Identificar columnas de Orden de Compra
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.1 - COLUMNAS OC] Buscando columnas de Orden de Compra...")
            
            # Buscar columna OC en R065
            oc_col_r065 = self._find_column(df_r065_work, ["ORDEN COMPRA", "ORDEN_COMPRA", "OC"])
            if oc_col_r065 is None:
                print(f"[PASO 4.1 - COLUMNAS OC] ✗ No se encontró columna OC en R065")
                print(f"[PASO 4.1 - COLUMNAS OC] Columnas disponibles: {list(df_r065_work.columns)}")
                self.error_occurred = True
                self.error_message = "No se encontró columna Orden de Compra en R065"
                return False
            print(f"[PASO 4.1 - COLUMNAS OC] ✓ R065 OC: '{oc_col_r065}'")
            
            # Buscar columna OC en R033
            oc_col_r033 = self._find_column(df_r033_work, ["Orden de Compra", "ORDEN DE COMPRA", "OC"])
            if oc_col_r033 is None:
                print(f"[PASO 4.1 - COLUMNAS OC] ✗ No se encontró columna OC en R033")
                print(f"[PASO 4.1 - COLUMNAS OC] Columnas disponibles: {list(df_r033_work.columns)}")
                self.error_occurred = True
                self.error_message = "No se encontró columna Orden de Compra en R033"
                return False
            print(f"[PASO 4.1 - COLUMNAS OC] ✓ R033 OC: '{oc_col_r033}'")
            
            # -----------------------------------------------------------------
            # PASO 4.2: Identificar columna Tienda en R033 (será Centro de Costo)
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.2 - CENTRO COSTO] Buscando columna 'Tienda' en R033...")
            
            # Buscar columna Tienda en R033 (coincidencia EXACTA para evitar "Cód. Tienda")
            cc_col_r033 = self._find_column(df_r033_work, ["Tienda"], exact_match=True)
            
            if cc_col_r033 is None:
                print(f"[PASO 4.2 - CENTRO COSTO] ⚠ No se encontró columna 'Tienda' en R033")
                print(f"[PASO 4.2 - CENTRO COSTO] Columnas disponibles en R033:")
                for col in df_r033_work.columns:
                    print(f"[PASO 4.2 - CENTRO COSTO]   - {col}")
                # Continuar sin Centro de Costo
                cc_col_r033 = None
            else:
                print(f"[PASO 4.2 - CENTRO COSTO] ✓ Columna 'Tienda' encontrada: '{cc_col_r033}'")
                print(f"[PASO 4.2 - CENTRO COSTO]   Se mapeará a 'Centro de Costo' en el resultado")
            
            # -----------------------------------------------------------------
            # PASO 4.3: Buscar columna Estatus en R033
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.3 - ESTATUS] Buscando columna 'Estatus' en R033...")
            
            estatus_col_r033 = self._find_column(df_r033_work, ["Estatus"], exact_match=True)
            if estatus_col_r033:
                print(f"[PASO 4.3 - ESTATUS] ✓ Columna encontrada: '{estatus_col_r033}'")
            else:
                print(f"[PASO 4.3 - ESTATUS] ⚠ No se encontró columna 'Estatus' en R033")
            
            # -----------------------------------------------------------------
            # PASO 4.4: Preparar R033 para el cruce (columnas necesarias)
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.4 - PREPARAR CRUCE] Preparando datos para el merge...")
            
            # Columnas a traer del R033
            cols_to_merge = [oc_col_r033]
            rename_dict = {oc_col_r033: 'OC_MERGE'}
            
            if cc_col_r033:
                cols_to_merge.append(cc_col_r033)
                rename_dict[cc_col_r033] = 'Centro de Costo'
            
            if estatus_col_r033:
                cols_to_merge.append(estatus_col_r033)
                rename_dict[estatus_col_r033] = 'Estatus R033'
            
            # Crear DataFrame de lookup
            df_r033_lookup = df_r033_work[cols_to_merge].copy()
            df_r033_lookup = df_r033_lookup.rename(columns=rename_dict)
            
            # Si no hay Centro de Costo, crear columna vacía
            if 'Centro de Costo' not in df_r033_lookup.columns:
                df_r033_lookup['Centro de Costo'] = ''
            
            # Si no hay Estatus, crear columna vacía
            if 'Estatus R033' not in df_r033_lookup.columns:
                df_r033_lookup['Estatus R033'] = ''
            
            # Eliminar duplicados de OC (quedarse con el primero)
            df_r033_lookup = df_r033_lookup.drop_duplicates(subset=['OC_MERGE'], keep='first')
            print(f"[PASO 4.4 - PREPARAR CRUCE] R033 lookup: {len(df_r033_lookup)} OCs únicas")
            
            # Preparar R065 para el merge
            df_r065_work['OC_MERGE'] = df_r065_work[oc_col_r065].astype(str).str.strip()
            df_r033_lookup['OC_MERGE'] = df_r033_lookup['OC_MERGE'].astype(str).str.strip()
            
            # -----------------------------------------------------------------
            # PASO 4.5: Realizar el cruce (LEFT JOIN)
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.5 - MERGE] Realizando cruce por Orden de Compra...")
            
            self.df_resultado = df_r065_work.merge(
                df_r033_lookup[['OC_MERGE', 'Centro de Costo', 'Estatus R033']],
                on='OC_MERGE',
                how='left'
            )
            
            # Eliminar columna temporal de merge
            self.df_resultado = self.df_resultado.drop(columns=['OC_MERGE'])
            
            # Contar cuántos registros tienen Centro de Costo
            filas_antes = len(self.df_resultado)
            con_cc = self.df_resultado['Centro de Costo'].notna().sum()
            sin_cc = self.df_resultado['Centro de Costo'].isna().sum()
            
            print(f"[PASO 4.5 - MERGE] ✓ Cruce completado")
            print(f"[PASO 4.5 - MERGE]   - Total filas: {filas_antes}")
            print(f"[PASO 4.5 - MERGE]   - Con Centro de Costo: {con_cc}")
            print(f"[PASO 4.5 - MERGE]   - Sin Centro de Costo: {sin_cc}")
            
            # -----------------------------------------------------------------
            # PASO 4.6: Eliminar filas sin Centro de Costo
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.6 - LIMPIAR] Eliminando filas sin Centro de Costo...")
            
            self.df_resultado = self.df_resultado[
                self.df_resultado['Centro de Costo'].notna() & 
                (self.df_resultado['Centro de Costo'].astype(str).str.strip() != '')
            ].copy()
            
            filas_despues = len(self.df_resultado)
            filas_eliminadas = filas_antes - filas_despues
            
            print(f"[PASO 4.6 - LIMPIAR] ✓ Limpieza completada")
            print(f"[PASO 4.6 - LIMPIAR]   - Filas eliminadas: {filas_eliminadas}")
            print(f"[PASO 4.6 - LIMPIAR]   - Filas restantes: {filas_despues}")
            
            # -----------------------------------------------------------------
            # PASO 4.7: Crear columna Grupo de Pago
            # -----------------------------------------------------------------
            print(f"\n[PASO 4.7 - GRUPO PAGO] Creando columna 'Grupo de Pago'...")
            
            # Si Centro de Costo contiene "CENDIS" → "CENDIS", sino → "Directo"
            self.df_resultado['Grupo de Pago'] = self.df_resultado['Centro de Costo'].apply(
                lambda x: 'CENDIS' if 'CENDIS' in str(x).upper() else 'Directo'
            )
            
            # Contar distribución
            grupo_cendis = (self.df_resultado['Grupo de Pago'] == 'CENDIS').sum()
            grupo_directo = (self.df_resultado['Grupo de Pago'] == 'Directo').sum()
            
            print(f"[PASO 4.7 - GRUPO PAGO] ✓ Columna creada")
            print(f"[PASO 4.7 - GRUPO PAGO]   - CENDIS: {grupo_cendis}")
            print(f"[PASO 4.7 - GRUPO PAGO]   - Directo: {grupo_directo}")
            
            print(f"\n[PASO 4 - PROCESAR] ✓ Procesamiento completado")
            print(f"[PASO 4 - PROCESAR]   - Filas en resultado: {len(self.df_resultado)}")
            print(f"[PASO 4 - PROCESAR]   - Columnas: {list(self.df_resultado.columns)}")
            
            return True
            
        except Exception as e:
            print(f"[PASO 4 - PROCESAR] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error en procesamiento: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 5: CREAR EXCEL (HILO 1) - OPTIMIZADO CON XLSXWRITER
    # =========================================================================
    
    def _create_resumen(self) -> pd.DataFrame:
        """
        Crea el DataFrame de resumen agrupado por proveedor
        
        Columnas:
        - Proveedor
        - Número de Facturas (facturas únicas)
        - Monto Total (suma de montos de facturas únicas, sin duplicar por ítems)
        - Número de Ítems (filas/registros)
        """
        print(f"\n[PASO 5.1 - RESUMEN] Creando tabla resumen por proveedor...")
        
        try:
            df = self.df_resultado.copy()
            
            # Identificar columnas necesarias
            proveedor_col = self._find_column(df, ["NOMBRE PROVEEDOR PADRE"], exact_match=True)
            factura_col = self._find_column(df, ["NRO FACTURA"])
            monto_col = self._find_column(df, ["TOTAL"], exact_match=True)
            item_col = self._find_column(df, ["ITEM DESCRIPCION"], exact_match=True)  # Columna de ítem
            
            if proveedor_col is None:
                print(f"[PASO 5.1 - RESUMEN] ⚠ No se encontró columna de proveedor")
                return pd.DataFrame()
            
            print(f"[PASO 5.1 - RESUMEN] Columna proveedor: '{proveedor_col}'")
            print(f"[PASO 5.1 - RESUMEN] Columna factura: '{factura_col}'")
            print(f"[PASO 5.1 - RESUMEN] Columna monto: '{monto_col}'")
            print(f"[PASO 5.1 - RESUMEN] Columna ítem: '{item_col}'")
            
            # Convertir monto a numérico
            if monto_col:
                df[monto_col] = pd.to_numeric(df[monto_col], errors='coerce').fillna(0)
            
            # -----------------------------------------------------------------
            # PASO 1: Contar ítems ÚNICOS por proveedor
            # -----------------------------------------------------------------
            if item_col:
                # Contar valores únicos de la columna ITEM
                df_items = df.groupby(proveedor_col)[item_col].nunique().reset_index(name='Número de Ítems')
            else:
                # Si no hay columna de ítem, contar filas
                df_items = df.groupby(proveedor_col).size().reset_index(name='Número de Ítems')
            
            # -----------------------------------------------------------------
            # PASO 2: Obtener facturas únicas con su monto (evitar duplicar monto por ítem)
            # El monto de la factura se repite en cada ítem, así que tomamos solo el primero
            # -----------------------------------------------------------------
            if factura_col and monto_col:
                # Obtener una fila por cada combinación proveedor + factura
                df_facturas = df.drop_duplicates(subset=[proveedor_col, factura_col])[[proveedor_col, factura_col, monto_col]]
                
                # Agrupar por proveedor: contar facturas y sumar montos únicos
                df_montos = df_facturas.groupby(proveedor_col).agg(
                    **{
                        'Número de Facturas': (factura_col, 'nunique'),
                        'Monto Total': (monto_col, 'sum')
                    }
                ).reset_index()
            elif factura_col:
                df_montos = df.groupby(proveedor_col).agg(
                    **{'Número de Facturas': (factura_col, 'nunique')}
                ).reset_index()
                df_montos['Monto Total'] = 0
            else:
                df_montos = df.groupby(proveedor_col).size().reset_index(name='Número de Facturas')
                df_montos['Monto Total'] = 0
            
            # -----------------------------------------------------------------
            # PASO 3: Combinar resultados
            # -----------------------------------------------------------------
            df_resumen = df_montos.merge(df_items, on=proveedor_col, how='left')
            df_resumen = df_resumen.rename(columns={proveedor_col: 'Proveedor'})
            
            # Reordenar columnas
            cols_order = ['Proveedor', 'Número de Facturas', 'Monto Total', 'Número de Ítems']
            df_resumen = df_resumen[[c for c in cols_order if c in df_resumen.columns]]
            
            # Ordenar por monto total descendente
            if 'Monto Total' in df_resumen.columns:
                df_resumen = df_resumen.sort_values('Monto Total', ascending=False)
            
            print(f"[PASO 5.1 - RESUMEN] ✓ Resumen creado: {len(df_resumen)} proveedores")
            
            # Calcular totales
            total_facturas = df_resumen['Número de Facturas'].sum() if 'Número de Facturas' in df_resumen.columns else 0
            total_monto = df_resumen['Monto Total'].sum() if 'Monto Total' in df_resumen.columns else 0
            total_items = df_resumen['Número de Ítems'].sum() if 'Número de Ítems' in df_resumen.columns else 0
            
            print(f"[PASO 5.1 - RESUMEN]   - Total facturas: {total_facturas}")
            print(f"[PASO 5.1 - RESUMEN]   - Total monto: {total_monto:,.2f}")
            print(f"[PASO 5.1 - RESUMEN]   - Total ítems: {total_items}")
            
            # Agregar fila de totales al final
            fila_totales = {
                'Proveedor': 'TOTAL',
                'Número de Facturas': total_facturas,
                'Monto Total': total_monto,
                'Número de Ítems': total_items
            }
            df_totales = pd.DataFrame([fila_totales])
            df_resumen = pd.concat([df_resumen, df_totales], ignore_index=True)
            
            return df_resumen
            
        except Exception as e:
            print(f"[PASO 5.1 - RESUMEN] ✗ Error creando resumen: {str(e)}")
            return pd.DataFrame()
    
    def _format_sheet(self, writer, sheet_name: str, df: pd.DataFrame, has_totals_row: bool = False):
        """
        Aplica formato a una hoja de Excel:
        - Cabezales azul oscuro con letras blancas
        - Ancho de columnas ajustado al contenido
        - Fila de totales con formato especial (si aplica)
        """
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Formato para cabezales: azul oscuro, letras blancas, negrita
        header_format = workbook.add_format({
            'bold': True,
            'font_color': 'white',
            'bg_color': '#1F4E79',  # Azul oscuro
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        # Formato para fila de totales: azul claro, negrita
        totals_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D6E3F8',  # Azul claro
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        totals_format_text = workbook.add_format({
            'bold': True,
            'bg_color': '#D6E3F8',  # Azul claro
            'border': 1
        })
        
        # Aplicar formato a los cabezales (fila 0)
        for col_num, column_name in enumerate(df.columns):
            worksheet.write(0, col_num, column_name, header_format)
            
            # Calcular ancho de columna basado en el contenido
            max_length = len(str(column_name))
            
            # Revisar el contenido de la columna para obtener el máximo
            for value in df[column_name].astype(str):
                if len(value) > max_length:
                    max_length = len(value)
            
            # Ajustar ancho (agregar un poco de padding)
            adjusted_width = min(max_length + 2, 50)  # Máximo 50 caracteres
            worksheet.set_column(col_num, col_num, adjusted_width)
        
        # Aplicar formato a la fila de totales (última fila)
        if has_totals_row and len(df) > 0:
            last_row = len(df)  # +1 por el header, pero ya está en 0-indexed
            for col_num, column_name in enumerate(df.columns):
                value = df.iloc[-1][column_name]
                if column_name == 'Proveedor':
                    worksheet.write(last_row, col_num, value, totals_format_text)
                else:
                    worksheet.write(last_row, col_num, value, totals_format)
    
    def create_excel(self, output_path: str, include_originals: bool = False) -> bool:
        """
        Crea el archivo Excel de retorno (optimizado con xlsxwriter)
        
        Args:
            output_path: Ruta del archivo de salida
            include_originals: Si True, incluye hojas con datos originales (más lento)
        """
        print(f"\n[PASO 5 - CREAR EXCEL] Iniciando creación...")
        print(f"[PASO 5 - CREAR EXCEL] Ruta: {output_path}")
        start_time = datetime.now()
        
        try:
            # Crear resumen antes de escribir el Excel
            df_resumen = self._create_resumen()
            
            # Usar xlsxwriter que es mucho más rápido que openpyxl
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Hoja Resumen primero
                if not df_resumen.empty:
                    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
                    self._format_sheet(writer, 'Resumen', df_resumen, has_totals_row=True)
                    print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'Resumen' creada ({len(df_resumen) - 1} proveedores + fila totales)")
                
                # Hoja principal - Resultado
                self.df_resultado.to_excel(writer, sheet_name='Resultado', index=False)
                self._format_sheet(writer, 'Resultado', self.df_resultado)
                print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'Resultado' creada ({len(self.df_resultado)} filas)")
                
                # Hojas originales solo si se solicita (¡muy lento con archivos grandes!)
                if include_originals:
                    if self.df_r065_filtrado is not None:
                        self.df_r065_filtrado.to_excel(writer, sheet_name='R065_Filtrado', index=False)
                        print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R065_Filtrado' creada")
                    
                    if self.df_r033 is not None:
                        self.df_r033.to_excel(writer, sheet_name='R033_Original', index=False)
                        print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R033_Original' creada")
                    
                    if self.df_r065 is not None:
                        self.df_r065.to_excel(writer, sheet_name='R065_Original', index=False)
                        print(f"[PASO 5 - CREAR EXCEL]   - Hoja 'R065_Original' creada")
            
            self.excel_path = output_path
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"[PASO 5 - CREAR EXCEL] ✓ Excel creado en {elapsed:.2f}s")
            
            return True
            
        except Exception as e:
            print(f"[PASO 5 - CREAR EXCEL] ✗ Error: {str(e)}")
            self.error_occurred = True
            self.error_message = f"Error creando Excel: {str(e)}"
            return False
    
    # =========================================================================
    # PASO 6: SUBIR A BIGQUERY (HILO 2)
    # =========================================================================
    
    def _prepare_dataframe_for_bigquery(self) -> pd.DataFrame:
        """
        Prepara el DataFrame para BigQuery:
        1. Renombra columnas según el mapeo
        2. Convierte tipos de datos
        3. Agrega columna timestamp
        """
        print(f"\n[PASO 6.1 - PREPARAR BQ] Preparando DataFrame para BigQuery...")
        
        df_bq = self.df_resultado.copy()
        
        # Paso 1: Renombrar columnas según el mapeo
        columns_renamed = {}
        for old_name, new_name in COLUMN_MAPPING_BQ.items():
            # Buscar la columna (puede tener variaciones)
            for col in df_bq.columns:
                if str(col).strip().upper() == old_name.upper():
                    columns_renamed[col] = new_name
                    break
        
        df_bq = df_bq.rename(columns=columns_renamed)
        print(f"[PASO 6.1 - PREPARAR BQ]   - Columnas renombradas: {len(columns_renamed)}")
        
        # Paso 2: Convertir columnas numéricas a FLOAT
        for col in FLOAT_COLUMNS_BQ:
            if col in df_bq.columns:
                df_bq[col] = pd.to_numeric(df_bq[col], errors='coerce').fillna(0.0)
        print(f"[PASO 6.1 - PREPARAR BQ]   - Columnas numéricas convertidas")
        
        # Paso 3: Convertir columnas de fecha
        for col, date_type in DATE_COLUMNS_BQ.items():
            if col in df_bq.columns:
                try:
                    if date_type == "datetime":
                        df_bq[col] = pd.to_datetime(df_bq[col], errors='coerce')
                    elif date_type == "date":
                        df_bq[col] = pd.to_datetime(df_bq[col], errors='coerce').dt.date
                except Exception as e:
                    print(f"[PASO 6.1 - PREPARAR BQ]   ⚠ Error convirtiendo {col}: {str(e)}")
        print(f"[PASO 6.1 - PREPARAR BQ]   - Columnas de fecha convertidas")
        
        # Paso 4: Agregar timestamp actual (hora de Caracas, Venezuela UTC-4)
        df_bq['col_vpn_timestamp'] = datetime.now(TIMEZONE_CARACAS)
        print(f"[PASO 6.1 - PREPARAR BQ]   - Timestamp agregado (Caracas UTC-4)")
        
        # Paso 5: Convertir todas las columnas STRING a str y limpiar
        string_cols = [col for col in df_bq.columns if col not in FLOAT_COLUMNS_BQ 
                       and col not in DATE_COLUMNS_BQ.keys() 
                       and col != 'col_vpn_timestamp']
        for col in string_cols:
            df_bq[col] = df_bq[col].astype(str).replace('nan', '').replace('None', '')
        
        print(f"[PASO 6.1 - PREPARAR BQ] ✓ DataFrame preparado")
        print(f"[PASO 6.1 - PREPARAR BQ]   - Filas: {len(df_bq)}")
        print(f"[PASO 6.1 - PREPARAR BQ]   - Columnas: {list(df_bq.columns)}")
        
        return df_bq
    
    def upload_to_bigquery(self, table_name: str = None) -> bool:
        """Sube el DataFrame de resultado a BigQuery con los nombres de columnas correctos"""
        # Usar variable de entorno si no se especifica
        if table_name is None:
            table_name = os.getenv("BQ_TABLE_NAME", "reporte_vpn")
        
        print(f"\n[PASO 6 - BIGQUERY] Iniciando subida...")
        print(f"[PASO 6 - BIGQUERY] Tabla destino: {table_name}")
        
        try:
            # Preparar DataFrame con nombres y tipos de BigQuery
            df_bq = self._prepare_dataframe_for_bigquery()
            
            if not self.bq_connection.connect():
                print("[PASO 6 - BIGQUERY] ⚠ No se pudo conectar, continuando sin subir")
                return False
            
            success = self.bq_connection.insert_dataframe(
                df_bq, 
                table_name, 
                if_exists="append"
            )
            
            if success:
                print(f"[PASO 6 - BIGQUERY] ✓ Datos insertados: {len(df_bq)} filas")
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
    
    def main(self, r033_file, r065_file, output_path: str = None, table_name: str = None) -> dict:
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
        total_start_time = datetime.now()
        
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
        # PASO 1-2: CARGAR ARCHIVOS EN PARALELO
        # ---------------------------------------------------------------------
        print("\n[PASO 1-2 - CARGA PARALELA] Cargando archivos en paralelo...")
        load_start = datetime.now()
        
        load_errors = []
        
        def _load_r033_thread():
            if not self.load_r033(r033_file):
                load_errors.append("R033")
        
        def _load_r065_thread():
            if not self.load_r065(r065_file):
                load_errors.append("R065")
        
        # Cargar ambos archivos en paralelo
        t1 = threading.Thread(target=_load_r033_thread)
        t2 = threading.Thread(target=_load_r065_thread)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        load_elapsed = (datetime.now() - load_start).total_seconds()
        print(f"\n[PASO 1-2 - CARGA PARALELA] ✓ Archivos cargados en {load_elapsed:.2f}s")
        
        if load_errors:
            return self._build_error_response()
        
        # ---------------------------------------------------------------------
        # PASOS 3-6: EJECUTAR EN HILOS
        # ---------------------------------------------------------------------
        self.start_threads(output_path, table_name)
        
        # ---------------------------------------------------------------------
        # RESULTADO FINAL
        # ---------------------------------------------------------------------
        total_elapsed = (datetime.now() - total_start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print(f"  FIN DEL PROCESAMIENTO - TIEMPO TOTAL: {total_elapsed:.2f}s")
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
    
    def execute(self, r033_file, r065_file, output_path: str = None, table_name: str = None) -> dict:
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
