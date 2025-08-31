def suma_segura(a: float, b: float) -> float:
    return a + b

def dividir_sin_error(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("No se puede dividir entre cero.")
    return a / b

def validar_positivo(n: float) -> bool:
    return n > 0

def raiz_segura(n: float) -> float:
    if n < 0:
        raise ValueError("No se puede calcular la raíz cuadrada de un número negativo.")
    return n ** 0.5