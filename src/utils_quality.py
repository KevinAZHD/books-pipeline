import pandas as pd
import re
from datetime import datetime

# Valida y convierte un string de fecha a formato ISO 8601 (YYYY-MM-DD)
def validar_fecha(string_fecha):
    if not isinstance(string_fecha, str):
        return None
    
    string_fecha = string_fecha.strip()
    try:
        # Fecha completa: YYYY-MM-DD
        return datetime.strptime(string_fecha, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Año y mes: YYYY-MM
            dt = datetime.strptime(string_fecha, '%Y-%m')
            return dt.strftime('%Y-%m-01')
        except ValueError:
            try:
                # Solo año: YYYY
                dt = datetime.strptime(string_fecha, '%Y')
                return dt.strftime('%Y-01-01')
            except ValueError:
                return None

# Valida si un código de idioma está en formato BCP-47
def validar_codigo_idioma(codigo_idioma):
    if not isinstance(codigo_idioma, str):
        return None
    
    # Regex simple para formatos comunes de BCP-47 como 'en' o 'en-US'
    if re.match(r'^[a-z]{2}(-[A-Z]{2})?$', codigo_idioma):
        return codigo_idioma
    return None

# Valida si un código de moneda está en formato ISO 4217
def validar_codigo_moneda(codigo_moneda):
    if not isinstance(codigo_moneda, str):
        return None
    
    if re.match(r'^[A-Z]{3}$', codigo_moneda.upper()):
        return codigo_moneda.upper()
    return None

# Elimina espacios en blanco al principio y al final y el exceso de espacios en un string
def limpiar_string(texto):
    if isinstance(texto, str):
        return re.sub(r'\s+', ' ', texto).strip()
    return texto

# Convierte todos los nombres de las columnas de un DataFrame a snake_case
def normalizar_nombres_columnas(df):
    df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', nombre).lower() for nombre in df.columns]
    return df

# Genera un informe de calidad para un DataFrame dado
def generar_reporte_calidad(df, nombre_fuente):
    reporte = {
        'fuente': nombre_fuente,
        'filas_totales': len(df),
        'nulos_por_columna': {col: int(df[col].isnull().sum()) for col in df.columns},
        'porcentaje_nulos_por_columna': {col: f"{df[col].isnull().mean() * 100:.2f}%" for col in df.columns},
        'filas_duplicadas': int(df.duplicated().sum())
    }
    return reporte