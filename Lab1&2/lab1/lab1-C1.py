# Decorador que valida que todos los argumentos numéricos sean mayores que cero
def requiere_positivos(func):
    def wrapper(*args, **kwargs):
        for arg in list(args) + list(kwargs.values()):
            if isinstance(arg, (int, float)) and arg <= 0:
                raise ValueError(f"Argumento inválido: {arg}. Todos los valores numéricos deben ser mayores que cero.")
        return func(*args, **kwargs)
    return wrapper

# Función que calcula el descuento
@requiere_positivos
def calcular_descuento(precio, porcentaje):
    return precio * (1 - porcentaje)

# Función que escala un valor
@requiere_positivos
def escala(valor, factor):
    return valor * factor

# Pruebas de ejecución
def probar_funciones():
    print(" calcular_descuento(100, 0.2):", calcular_descuento(100, 0.2))  # Esperado: 80.0
    print(" escala(5, 3):", escala(5, 3))  # Esperado: 15

    try:
        calcular_descuento(-1, 0.2)
    except ValueError as e:
        print(" Error esperado en calcular_descuento(-1, 0.2):", e)

    try:
        escala(0, 2)
    except ValueError as e:
        print(" Error esperado en escala(0, 2):", e)

# Ejecutar pruebas
probar_funciones()