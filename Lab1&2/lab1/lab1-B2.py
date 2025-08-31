# Excepción personalizada
class CantidadInvalida(Exception):
    def __init__(self, mensaje="La cantidad debe ser mayor que cero."):
        super().__init__(mensaje)

# Función principal
def calcular_total(precio_unitario, cantidad):
    if cantidad <= 0:
        raise CantidadInvalida()
    if precio_unitario < 0:
        raise ValueError("El precio unitario no puede ser negativo.")
    return precio_unitario * cantidad

# Función que ejecuta y muestra todos los casos
def probar_casos():
    casos = [
        (10, 3),     # válido
        (10, 0),     # cantidad inválida
        (-5, 2),     # precio inválido
        (15, -1),    # cantidad inválida
        (0, 5),      # válido (precio 0)
    ]

    print("🧪 Validando operaciones de compra...\n")

    for i, (precio, cantidad) in enumerate(casos):
        print(f"🔹 Caso {i+1}: precio_unitario={precio}, cantidad={cantidad}")
        try:
            total = calcular_total(precio, cantidad)
            print(f"   ✅ Total calculado: ${total:.2f}\n")
        except CantidadInvalida as ci:
            print(f"   ❌ Error de cantidad: {ci}\n")
        except ValueError as ve:
            print(f"   ❌ Error de precio: {ve}\n")

# Ejecutar todos los casos
probar_casos()