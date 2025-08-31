def normalizar(texto: str) -> str:
    return texto.strip().lower()

def es_palindromo(texto: str) -> bool:
    texto = normalizar(texto)
    return texto == texto[::-1]