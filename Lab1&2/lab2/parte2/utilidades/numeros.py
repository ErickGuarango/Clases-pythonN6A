from .cadenas import normalizar  # ← Importación relativa

def suma_segura(a: float, b: float) -> float:
    return a + b

def convertir_a_numero(texto: str) -> float:
    texto = normalizar(texto)  # Usamos función del otro módulo
    try:
        return float(texto)
    except ValueError:
        raise ValueError(f"No se pudo convertir '{texto}' a número.")