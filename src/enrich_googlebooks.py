import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv

# Carga la clave de la API de Google Books
def cargar_clave_api():
    load_dotenv()
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    return api_key

# Busca en la API de Google Books un libro por ISBN, o título y autor, con reintentos
def buscar_en_google_books(api_key, isbn=None, titulo=None, autor=None, reintentos=3, factor_backoff=0.5):
    url_base = "https://www.googleapis.com/books/v1/volumes"
    consulta = ""
    
    if isbn:
        consulta = f"isbn:{isbn}"
    elif titulo and autor:
        consulta = f"intitle:{titulo}+inauthor:{autor}"
    else:
        return None

    params = {"q": consulta, "key": api_key, "langRestrict": "es,en"}
    
    for i in range(reintentos):
        try:
            respuesta = requests.get(url_base, params=params, timeout=10)
            respuesta.raise_for_status()
            return respuesta.json()
        except requests.exceptions.RequestException as e:
            print(f"La solicitud a la API falló para la consulta '{consulta}': {e}. Reintentando en {factor_backoff * (2 ** i)} segundos...")
            time.sleep(factor_backoff * (2 ** i))
    
    print(f"No se pudieron obtener datos para la consulta '{consulta}' después de {reintentos} reintentos.")
    return None

# Extrae la información deseada del libro de un item de la API de Google Books
def extraer_info_libro(item):
    info_volumen = item.get('volumeInfo', {})
    info_venta = item.get('saleInfo', {})
    
    # Ayudante para encontrar ISBNs
    def encontrar_isbn(tipo):
        for identificador in info_volumen.get('industryIdentifiers', []):
            if identificador.get('type') == tipo:
                return identificador.get('identifier')
        return None

    return {
        "gb_id": item.get('id'),
        "title": info_volumen.get('title'),
        "subtitle": info_volumen.get('subtitle'),
        "authors": ", ".join(info_volumen.get('authors', [])),
        "publisher": info_volumen.get('publisher'),
        "pub_date": info_volumen.get('publishedDate'),
        "language": info_volumen.get('language'),
        "categories": ", ".join(info_volumen.get('categories', [])),
        "isbn13": encontrar_isbn('ISBN_13'),
        "isbn10": encontrar_isbn('ISBN_10'),
        "price_amount": info_venta.get('listPrice', {}).get('amount'),
        "price_currency": info_venta.get('listPrice', {}).get('currencyCode'),
    }

# Enriquece los libros del dataframe de Goodreads con datos de Google Books
def enriquecer_libros(api_key, df_goodreads):
    datos_enriquecidos = []
    
    for _, fila in df_goodreads.iterrows():
        print(f"Enriqueciendo: {fila['title']}")
        resultado = None
        
        # Prioriza ISBN-13, luego ISBN-10
        if fila.get('isbn13') and pd.notna(fila['isbn13']):
            resultado = buscar_en_google_books(api_key, isbn=fila['isbn13'])
            time.sleep(0.5)
        
        if not resultado or resultado.get('totalItems', 0) == 0:
            if fila.get('isbn10') and pd.notna(fila['isbn10']):
                resultado = buscar_en_google_books(api_key, isbn=fila['isbn10'])
                time.sleep(0.5)

        # Fallback a título y autor
        if not resultado or resultado.get('totalItems', 0) == 0:
            resultado = buscar_en_google_books(api_key, titulo=fila['title'], autor=fila['author'])
            time.sleep(0.5)
            
        if resultado and resultado.get('totalItems', 0) > 0:
            # Toma el primer resultado, que generalmente es el más relevante
            info_libro = extraer_info_libro(resultado['items'][0])
            datos_enriquecidos.append(info_libro)
        else:
            print(f"  -> No se pudo encontrar '{fila['title']}' en Google Books.")
            datos_enriquecidos.append({ "gb_id": None, "title": fila['title'] })

    return pd.DataFrame(datos_enriquecidos)

# Función principal para ejecutar el proceso de enriquecimiento
def main():
    api_key = cargar_clave_api()
    if not api_key:
        print("Error: No se encontró la clave de la API de Google Books. Por favor, cree un archivo '.env' con la variable GOOGLE_BOOKS_API_KEY.")
        return

    directorio_landing = "landing"
    archivo_goodreads = os.path.join(directorio_landing, "goodreads_books.json")
    archivo_googlebooks = os.path.join(directorio_landing, "googlebooks_books.csv")

    if not os.path.exists(archivo_goodreads):
        print(f"Error: Archivo de entrada no encontrado en '{archivo_goodreads}'. Por favor, ejecute primero el scraper de Goodreads.")
        return

    df_goodreads = pd.read_json(archivo_goodreads)
    
    print("Iniciando proceso de enriquecimiento con la API de Google Books...")
    df_enriquecido = enriquecer_libros(api_key, df_goodreads)
    
    if not df_enriquecido.empty:
        df_enriquecido.to_csv(archivo_googlebooks, index=False, sep=';', encoding='utf-8')
        print(f"Enriquecimiento completo. Datos guardados en '{archivo_googlebooks}'.")

if __name__ == "__main__":
    main()