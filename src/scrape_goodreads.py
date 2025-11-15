import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os

# --- CONFIGURACIÓN ---
# Modifica estos valores para cambiar la búsqueda por defecto
CONSULTA_DEFAULT = "data science"
NUM_LIBROS_DEFAULT = 3

# Obtiene una URL con un número específico de reintentos y backoff
def obtener_url(url, headers, reintentos=3, factor_backoff=0.5, timeout=30):
    for i in range(reintentos):
        try:
            respuesta = requests.get(url, headers=headers, timeout=timeout)
            respuesta.raise_for_status()
            return respuesta
        except requests.exceptions.RequestException as e:
            print(f"Solicitud fallida para {url}: {e}. Reintentando en {factor_backoff * (2 ** i)} segundos...")
            time.sleep(factor_backoff * (2 ** i))
    print(f"No se pudo obtener {url} después de {reintentos} reintentos.")
    return None

# Obtiene la página del libro y extrae el ISBN10 e ISBN13
def obtener_detalles_libro(url_libro, headers):
    respuesta = obtener_url(url_libro, headers)
    if not respuesta:
        return None, None

    sopa = BeautifulSoup(respuesta.text, 'lxml')
    isbn10 = None
    isbn13 = None

    try:
        # Prioriza el script JSON-LD para datos estructurados
        etiqueta_script = sopa.find('script', type='application/ld+json')
        if etiqueta_script:
            datos_json = json.loads(etiqueta_script.string)
            if 'isbn' in datos_json:
                isbn13 = datos_json.get('isbn')

        # Fallback a regex en el contenido de la página si JSON-LD falla
        if not isbn13:
            coincidencia_isbn = re.search(r'ISBN(?:13)?:?\s*(\d{10,13})', respuesta.text)
            if coincidencia_isbn:
                isbn_encontrado = coincidencia_isbn.group(1)
                if len(isbn_encontrado) == 13:
                    isbn13 = isbn_encontrado
                elif len(isbn_encontrado) == 10:
                    isbn10 = isbn_encontrado
    except Exception as e:
        print(f"Ocurrió un error inesperado al analizar los detalles del libro desde {url_libro}: {e}")

    return isbn10, isbn13


# Extrae datos de Goodreads para una consulta dada para obtener detalles de libros
def extraer_goodreads(consulta, num_libros):
    url_base = "https://www.goodreads.com"
    url_busqueda = f"{url_base}/search?q={consulta.replace(' ', '+')}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
    }

    todos_los_datos_libros = []
    pagina = 1
    
    while len(todos_los_datos_libros) < num_libros:
        url_busqueda_actual = f"{url_busqueda}&page={pagina}"
        print(f"Extrayendo página {pagina}: {url_busqueda_actual}")
        
        respuesta = obtener_url(url_busqueda_actual, headers)
        if not respuesta:
            break # Detener si una página de búsqueda no se carga

        sopa = BeautifulSoup(respuesta.text, 'lxml')
        contenedores_libros = sopa.find_all('tr', itemtype='http://schema.org/Book')
        
        if not contenedores_libros:
            print("No se encontraron más contenedores de libros o el selector cambió. Saliendo.")
            break

        for contenedor in contenedores_libros:
            if len(todos_los_datos_libros) >= num_libros:
                break

            etiqueta_titulo = contenedor.find('a', class_='bookTitle')
            etiqueta_autor = contenedor.find('a', class_='authorName')
            etiqueta_rating = contenedor.find('span', class_='minirating')
            
            if etiqueta_titulo and etiqueta_autor and etiqueta_rating:
                titulo = etiqueta_titulo.get_text(strip=True)
                autor = etiqueta_autor.get_text(strip=True)
                url_libro = url_base + etiqueta_titulo['href']
                
                texto_rating = etiqueta_rating.get_text(strip=True)
                coincidencia_rating = re.search(r'(\d\.\d+)', texto_rating)
                coincidencia_conteo_ratings = re.search(r'(\d{1,3}(?:,\d{3})*)\s+ratings', texto_rating)
                
                rating = float(coincidencia_rating.group(1)) if coincidencia_rating else None
                conteo_ratings = int(coincidencia_conteo_ratings.group(1).replace(',', '')) if coincidencia_conteo_ratings else None

                print(f"Obteniendo detalles para: {titulo}")
                isbn10, isbn13 = obtener_detalles_libro(url_libro, headers)
                time.sleep(1) # Ser cortés

                datos_libro = {
                    "title": titulo,
                    "author": autor,
                    "rating": rating,
                    "ratings_count": conteo_ratings,
                    "book_url": url_libro,
                    "isbn10": isbn10,
                    "isbn13": isbn13
                }
                todos_los_datos_libros.append(datos_libro)
            
        pagina += 1
        time.sleep(2) # Pausa entre páginas

    return todos_los_datos_libros

# Guarda los datos de los libros extraídos en un archivo JSON
def guardar_resultados(libros):
    directorio_salida = "landing"
    os.makedirs(directorio_salida, exist_ok=True)
    archivo_salida = os.path.join(directorio_salida, "goodreads_books.json")
    
    with open(archivo_salida, 'w', encoding='utf-8') as f:
        json.dump(libros, f, ensure_ascii=False, indent=4)
    
    print(f"Extracción finalizada. {len(libros)} libros guardados en {archivo_salida}")
    return len(libros)

# Función principal para ejecutar el scraper
def main():
    print(f"Iniciando extracción de Goodreads para libros de '{CONSULTA_DEFAULT}'...")
    libros = extraer_goodreads(consulta=CONSULTA_DEFAULT, num_libros=NUM_LIBROS_DEFAULT)
    if libros:
        guardar_resultados(libros)
    else:
        print("No se extrajeron libros.")

if __name__ == "__main__":
    main()