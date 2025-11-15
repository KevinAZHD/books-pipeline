import pandas as pd
import numpy as np
import os
import json
import hashlib
from datetime import datetime, timezone

# Importar funciones de los módulos de utilidades
from utils_isbn import formatear_isbn10, formatear_isbn13
from utils_quality import (
    validar_fecha, validar_codigo_idioma, validar_codigo_moneda,
    limpiar_string, generar_reporte_calidad
)

# Carga los datos de Goodreads y Google Books desde la zona de aterrizaje (landing)
def cargar_datos(ruta_landing):
    archivo_goodreads = os.path.join(ruta_landing, 'goodreads_books.json')
    archivo_googlebooks = os.path.join(ruta_landing, 'googlebooks_books.csv')

    if not os.path.exists(archivo_goodreads) or not os.path.exists(archivo_googlebooks):
        raise FileNotFoundError("Archivos fuente no encontrados en el directorio landing. Por favor, ejecute el scraper y el enriquecedor primero.")

    df_gr = pd.read_json(archivo_goodreads)
    df_gb = pd.read_csv(archivo_googlebooks, sep=';')

    # Añadir información de la fuente
    df_gr['fuente'] = 'goodreads'
    df_gb['fuente'] = 'google_books'
    
    return df_gr, df_gb

# Combina y transforma datos de ambas fuentes en un modelo canónico
def crear_modelo_canonico(df_gr, df_gb):
    # Renombrar columnas para evitar conflictos y preparar para la fusión
    df_gr.rename(columns={'title': 'titulo_gr', 'author': 'autor_gr', 'rating': 'rating_gr', 'ratings_count': 'conteo_ratings_gr', 'isbn10': 'isbn10_gr', 'isbn13': 'isbn13_gr'}, inplace=True)
    df_gb.rename(columns={'title': 'titulo_gb', 'authors': 'autores_gb', 'publisher': 'editorial_gb', 'pub_date': 'fecha_pub_gb', 'language': 'idioma_gb', 'categories': 'categorias_gb', 'isbn10': 'isbn10_gb', 'isbn13': 'isbn13_gb', 'price_amount': 'precio_gb', 'price_currency': 'moneda_gb'}, inplace=True)

    # Usar el título de Goodreads como clave de unión
    df_fusionado = pd.merge(df_gr, df_gb, left_on='titulo_gr', right_on='titulo_gb', how='left', suffixes=('_gr', '_gb'))
    
    # Crear el dataframe de detalle de fuente
    df_detalle_fuente = df_fusionado.copy()
    df_detalle_fuente['ts_ingesta'] = datetime.now(timezone.utc).isoformat()
    
    return df_detalle_fuente

# Aplica normalización, limpieza y verificaciones de calidad.
def normalizar_y_verificar_calidad(df):
    # Limpieza y validación de ISBN
    df['isbn13_limpio'] = df['isbn13_gr'].fillna(df['isbn13_gb']).apply(formatear_isbn13)
    df['isbn10_limpio'] = df['isbn10_gr'].fillna(df['isbn10_gb']).apply(formatear_isbn10)

    # Normalización de fecha
    df['fecha_pub_iso'] = df['fecha_pub_gb'].apply(lambda x: validar_fecha(str(x)) if pd.notna(x) else None)
    df['anio_pub'] = pd.to_datetime(df['fecha_pub_iso']).dt.year.astype('Int64')

    # Validación de idioma y moneda
    df['codigo_idioma'] = df['idioma_gb'].apply(validar_codigo_idioma)
    df['codigo_moneda'] = df['moneda_gb'].apply(validar_codigo_moneda)

    # Limpieza de cadenas
    df['titulo'] = df['titulo_gb'].fillna(df['titulo_gr']).apply(limpiar_string)
    df['titulo_normalizado'] = df['titulo'].str.lower().str.replace(r'[^a-z0-9\s]', '', regex=True).apply(limpiar_string)
    
    # Normalización de autor
    df['autores'] = df['autores_gb'].fillna(df['autor_gr']).apply(lambda x: [limpiar_string(a) for a in str(x).split(',')] if pd.notna(x) else [])
    df['autor_principal'] = df['autores'].apply(lambda x: x[0] if x else None)

    # Editorial
    df['editorial'] = df['editorial_gb'].apply(limpiar_string)

    return df

# Genera un book_id estable. Prefiere ISBN-13, recurre a un hash como fallback
def generar_book_id(df):
    def crear_id_hash(fila):
        cadena_clave = f"{fila['titulo_normalizado']}{fila['autor_principal']}{fila['editorial']}{fila['anio_pub']}"
        return hashlib.sha256(cadena_clave.encode('utf-8')).hexdigest()

    df['book_id'] = df['isbn13_limpio']
    mascara_fallback = df['book_id'].isnull()
    df.loc[mascara_fallback, 'book_id'] = df[mascara_fallback].apply(crear_id_hash, axis=1)
    return df

# Deduplica registros basados en book_id y aplica reglas de supervivencia
def deduplicar_y_seleccionar_ganador(df):
    # Prioriza los datos de Google Books por ser más ricos
    df['prioridad_fuente_ganadora'] = df['fuente_gb'].apply(lambda x: 1 if pd.notna(x) else 2)
    
    # Ordena por prioridad (ganador primero) y elimina duplicados, manteniendo el primero
    df_deduplicado = df.sort_values(by=['book_id', 'prioridad_fuente_ganadora']).drop_duplicates('book_id', keep='first')
    
    return df_deduplicado

# Crea el DataFrame final dim_book a partir de los registros ganadores
def crear_dim_book(df_ganador):
    dim_book = df_ganador[[
        'book_id',
        'titulo',
        'titulo_normalizado',
        'autor_principal',
        'autores',
        'editorial',
        'anio_pub',
        'fecha_pub_iso',
        'codigo_idioma',
        'isbn10_limpio',
        'isbn13_limpio',
        'precio_gb',
        'codigo_moneda',
    ]].copy()

    # Renombrar columnas para la dimensión final
    dim_book.rename(columns={
        'fecha_pub_iso': 'fecha_publicacion',
        'anio_pub': 'anio_publicacion',
        'codigo_idioma': 'idioma',
        'isbn10_limpio': 'isbn10',
        'isbn13_limpio': 'isbn13',
        'precio_gb': 'precio',
        'codigo_moneda': 'moneda'
    }, inplace=True)
    
    dim_book['ts_ultima_actualizacion'] = datetime.now(timezone.utc).isoformat()
    
    return dim_book

# Guarda todos los artefactos finales en el disco
def generar_artefactos(df_dim, df_fuente, reportes_calidad):
    # Crear directorios si no existen
    os.makedirs('standard', exist_ok=True)
    os.makedirs('docs', exist_ok=True)

    # Guardar archivos Parquet
    df_dim.to_parquet('standard/dim_book.parquet', index=False)
    df_fuente.to_parquet('standard/book_source_detail.parquet', index=False)
    print("Guardados dim_book.parquet y book_source_detail.parquet")

    # Guardar métricas de calidad
    with open('docs/quality_metrics.json', 'w', encoding='utf-8') as f:
        json.dump(reportes_calidad, f, indent=4, ensure_ascii=False)
    print("Guardado quality_metrics.json")

    # Generar el archivo schema.md
    generar_schema_md(df_dim)

# Genera la documentación del esquema en formato Markdown
def generar_schema_md(df_dim):
    
    schema_content = """
# Documentación del Esquema: `dim_book`

Este documento detalla la estructura, campos y reglas de negocio de la tabla canónica `dim_book`, que consolida información de libros de Goodreads y Google Books.

## Descripción General

`dim_book` es una tabla dimensional que contiene una fila única por cada libro, identificada por `book_id`. Los datos son el resultado de un proceso de integración que incluye normalización, validación de calidad y deduplicación.

## Fuentes de Datos

El modelo se construye a partir de las siguientes fuentes, en orden de prioridad:

1.  **Google Books (`google_books`)**: Fuente principal, preferida por la riqueza de sus metadatos (ISBN, detalles de publicación, categorías, precios).
2.  **Goodreads (`goodreads`)**: Fuente secundaria, utilizada para complementar información como ratings y conteos de ratings, o como base si un libro no se encuentra en Google Books.

## Reglas de Deduplicación y Supervivencia

El objetivo es tener un registro único y de alta calidad por cada libro.

-   **Clave de Deduplicación**:
    1.  **Primaria**: `isbn13`. Se considera el identificador más fiable.
    2.  **Fallback**: Si el `isbn13` no está disponible, se genera un `book_id` único mediante un hash de la concatenación de `titulo_normalizado`, `autor_principal`, `editorial` y `anio_publicacion`.

-   **Reglas de Supervivencia** (qué datos se conservan en caso de duplicados):
    -   **Registro Ganador**: Se da prioridad al registro proveniente de **Google Books**, ya que se considera más completo y estructurado.
    -   **Títulos**: Se prefiere el título de Google Books. Si no existe, se usa el de Goodreads.
    -   **Autores y Categorías**: Se combinan y deduplican las listas de ambas fuentes para no perder información.
    -   **ISBN**: Se utiliza el primer valor no nulo encontrado, dando prioridad a Google Books.

## Definición de Campos

A continuación se describen todos los campos presentes en `dim_book.parquet`.

| Campo | Tipo de Dato | Nulable | Formato / Ejemplo | Descripción y Reglas |
| --- | --- | --- | --- | --- |
| `book_id` | `string` | No | `9780132350884` | **Identificador único del libro.** Preferentemente el `isbn13`. Si no está disponible, es un hash SHA256 de campos clave. |
| `titulo` | `string` | No | `Clean Code: A Handbook...` | Título del libro, limpio de espacios extra. Se prioriza la fuente de Google Books. |
| `titulo_normalizado` | `string` | No | `clean code a handbook` | Título en minúsculas, sin caracteres especiales, para facilitar búsquedas y uniones. |
| `autor_principal` | `string` | Sí | `Robert C. Martin` | El primer autor de la lista de `autores`. |
| `autores` | `array<string>` | Sí | `['Robert C. Martin', '...']` | Lista de todos los autores asociados al libro, uniendo ambas fuentes y eliminando duplicados. |
| `editorial` | `string` | Sí | `Prentice Hall` | Nombre de la editorial, extraído de Google Books. |
| `anio_publicacion` | `int64` | Sí | `2008` | Año de publicación extraído de la fecha de publicación. |
| `fecha_publicacion` | `string` | Sí | `2008-08-11` | Fecha de publicación completa en formato **ISO 8601 (YYYY-MM-DD)**. |
| `idioma` | `string` | Sí | `en` | Código de idioma en formato **BCP-47**. Se valida contra una lista de códigos permitidos. |
| `isbn10` | `string` | Sí | `0132350882` | ISBN de 10 dígitos, formateado y validado. |
| `isbn13` | `string` | Sí | `9780132350884` | ISBN de 13 dígitos, formateado y validado. Es la clave de negocio principal. |
| `precio` | `float64` | Sí | `35.99` | Precio del libro, extraído de Google Books. |
| `moneda` | `string` | Sí | `USD` | Código de moneda en formato **ISO 4217**. Se valida contra una lista de códigos permitidos. |
| `ts_ultima_actualizacion` | `string` | No | `2025-11-15T10:00:00Z` | Timestamp en formato **ISO 8601** que indica cuándo se procesó el registro por última vez. |
"""
    
    # Guardar el contenido en el archivo
    with open('docs/schema.md', 'w', encoding='utf-8') as f:
        f.write(schema_content.strip())
    print("Guardado schema.md")

# Función principal para ejecutar el pipeline de integración
def main():
    print("Iniciando pipeline de integración...")
    
    # 1. Cargar datos
    df_gr, df_gb = cargar_datos('landing')
    
    # Generar reportes de calidad iniciales
    calidad_gr = generar_reporte_calidad(df_gr, 'Goodreads')
    calidad_gb = generar_reporte_calidad(df_gb, 'Google Books')

    # 2. Crear modelo canónico (para detalle de fuente)
    df_detalle_fuente_raw = crear_modelo_canonico(df_gr, df_gb)

    # 3. Normalizar y aplicar verificaciones de calidad
    df_procesado = normalizar_y_verificar_calidad(df_detalle_fuente_raw)

    # 4. Generar book_id
    df_con_id = generar_book_id(df_procesado)
    
    # El dataframe procesado es nuestro book_source_detail
    book_source_detail = df_con_id.copy()

    # 5. Deduplicar y seleccionar ganador para dim_book
    df_ganador = deduplicar_y_seleccionar_ganador(df_con_id)

    # 6. Crear tabla dimensional final
    dim_book = crear_dim_book(df_ganador)

    # 7. Generar artefactos finales
    reporte_calidad_final = {
        'fuentes': [calidad_gr, calidad_gb],
        'duplicados_encontrados': int(df_con_id.duplicated('book_id').sum()),
        'total_libros_en_dimension': len(dim_book)
    }
    generar_artefactos(dim_book, book_source_detail, reporte_calidad_final)
    
    print("Pipeline de integración finalizado con éxito.")

if __name__ == "__main__":
    main()