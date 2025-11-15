# Mini-pipeline de Libros

## Descripción Breve del Proyecto

Este proyecto implementa un pipeline de datos simple (Extracción, Transformación y Carga) para procesar información de libros. El flujo completo es el siguiente:

1.  **Extracción**: Se obtiene una lista de libros sobre "data science" desde [Goodreads](https://www.goodreads.com/) mediante web scraping.
2.  **Enriquecimiento**: Utilizando los datos extraídos (principalmente ISBN), se consulta la [API de Google Books](https://developers.google.com/books) para obtener información más detallada y estructurada.
3.  **Integración y Carga**: Los datos de ambas fuentes se unifican en un modelo canónico. Se aplican reglas de calidad, normalización y deduplicación para generar un conjunto de datos final y limpio en formato Parquet.

El objetivo es producir una tabla dimensional de libros (`dim_book`) junto con artefactos de trazabilidad y calidad.

---

## Cómo Ejecutar el Pipeline

Para ejecutar el pipeline completo desde cero, sigue estos pasos en orden.

### 1. Preparar el Entorno

Asegúrate de tener Python 3.10+ y clona este repositorio.

**Instalar dependencias:**
```bash
pip install -r requirements.txt
```

**Configurar la API Key:**
Crea un archivo llamado `.env` en la raíz del proyecto y añade tu clave de la API de Google Books de la siguiente manera:
```
GOOGLE_BOOKS_API_KEY=TU_API_KEY_AQUI
```

### 2. Ejecutar los Scripts del Pipeline

Los scripts deben ejecutarse en secuencia desde la raíz del proyecto:

**Paso 1: Extraer datos de Goodreads**
```bash
python src/scrape_goodreads.py
```
*Esto generará el archivo `landing/goodreads_books.json`. Por defecto, busca "data science", pero puedes cambiar la consulta y el número de libros modificando las variables `CONSULTA_DEFAULT` y `NUM_LIBROS_DEFAULT` al principio del script.*

**Paso 2: Enriquecer con Google Books API**
```bash
python src/enrich_googlebooks.py
```
*Esto generará el archivo `landing/googlebooks_books.csv`.*

**Paso 3: Integrar datos y generar artefactos finales**
```bash
python src/integrate_pipeline.py
```
*Esto generará los archivos finales en los directorios `standard/` y `docs/`.*

---

## Dependencias

El proyecto utiliza las siguientes librerías de Python:

-   `requests`: Para realizar las peticiones HTTP al sitio de Goodreads y a la API de Google Books.
-   `beautifulsoup4`: Para parsear el HTML de Goodreads y extraer la información.
-   `lxml`: Parser de alto rendimiento para BeautifulSoup.
-   `pandas`: Para la manipulación y transformación de los datos.
-   `pyarrow`: Para escribir los datos finales en formato Parquet.
-   `numpy`: Dependencia de pandas para operaciones numéricas.
-   `python-dotenv`: Para gestionar la clave de la API de forma segura a través de variables de entorno.

---

## Metadatos y Decisiones Clave de Diseño

### Extracción (Scraping - Goodreads)
-   **User-Agent**: Se utiliza un User-Agent de un navegador común (`Chrome/96.0`) para simular una petición legítima y evitar bloqueos.
-   **Selectores CSS**:
    -   Contenedor de libro: `tr[itemtype='http://schema.org/Book']`
    -   Título: `a.bookTitle`
    -   Autor: `a.authorName`
    -   **Decisión Clave**: Para obtener el ISBN, se prioriza la extracción de datos estructurados desde una etiqueta `<script type='application/ld+json'>`. Este método es más robusto y menos propenso a romperse por cambios en el layout HTML que el parseo directo de texto en la página.

### Enriquecimiento (API - Google Books)
-   **Separador CSV**: Se eligió el punto y coma (`;`) para el archivo `.csv` de salida para evitar conflictos con comas que puedan aparecer en los títulos o descripciones de los libros.
-   **Codificación**: Se usa `utf-8` para asegurar la compatibilidad con cualquier carácter especial.
-   **Decisión Clave (Lógica de Búsqueda)**: Se estableció una jerarquía de búsqueda para maximizar la precisión: `ISBN-13` (más específico) → `ISBN-10` → `título + autor` (como fallback).

### Integración y Modelo de Datos
-   **Decisión Clave (Generación de ID)**: Se genera un `book_id` único y estable. Se prioriza el `isbn13` por ser un identificador universal. Si falta, se genera un hash `SHA-256` a partir de metadatos clave (título normalizado, autor principal, editorial, año), garantizando un ID consistente incluso sin ISBN.
-   **Decisión Clave (Regla de Supervivencia)**: Durante la deduplicación, si un mismo libro existe en ambas fuentes, se da prioridad a los datos de **Google Books**, ya que su API tiende a ofrecer información más completa y estructurada (fechas, categorías, etc.) que el scraping.
-   **Seguridad**: La clave de la API está configurada en un archivo `.env` que es ignorado por Git, siguiendo las mejores prácticas para no exponer credenciales en el repositorio.
