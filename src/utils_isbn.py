import re

# Verifica si un ISBN-10 es válido
def es_isbn10_valido(isbn):
    if not isinstance(isbn, str) or len(isbn) != 10 or not isbn[:-1].isdigit():
        return False
    
    ultimo_caracter = isbn[-1]
    if not (ultimo_caracter.isdigit() or ultimo_caracter.upper() == 'X'):
        return False

    total = 0
    for i in range(9):
        total += int(isbn[i]) * (10 - i)
    
    digito_verificador = total % 11
    if digito_verificador == 0:
        return ultimo_caracter == '0'
    
    caracter_verificador = str(11 - digito_verificador)
    if caracter_verificador == '10':
        caracter_verificador = 'X'
        
    return ultimo_caracter.upper() == caracter_verificador

# Verifica si un ISBN-13 es válido
def es_isbn13_valido(isbn):
    if not isinstance(isbn, str) or len(isbn) != 13 or not isbn.isdigit():
        return False

    total = 0
    for i in range(12):
        digito = int(isbn[i])
        total += digito * (1 if i % 2 == 0 else 3)
        
    digito_verificador = (10 - (total % 10)) % 10
    
    return str(digito_verificador) == isbn[-1]

# Elimina guiones y otros caracteres no alfanuméricos de una cadena de ISBN
def limpiar_isbn(isbn):
    if not isinstance(isbn, str):
        return None
    return re.sub(r'[^0-9X]', '', isbn.upper())

# Intenta formatear un string en un ISBN-13 válido
def formatear_isbn13(isbn):
    limpio = limpiar_isbn(isbn)
    if limpio and es_isbn13_valido(limpio):
        return limpio
    return None

# Intenta formatear un string en un ISBN-10 válido
def formatear_isbn10(isbn):
    limpio = limpiar_isbn(isbn)
    if limpio and es_isbn10_valido(limpio):
        return limpio
    return None