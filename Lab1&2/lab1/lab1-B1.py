def parsear_enteros(entradas):
    valores = []
    errores = []

    for i, texto in enumerate(entradas):
        try:
            numero = int(texto)
            print(f"✅ Entrada válida en índice {i}: '{texto}' → {numero}")
            valores.append(numero)
        except ValueError:
            mensaje_error = f"❌ Error en índice {i}: '{texto}' no es un entero válido."
            print(mensaje_error)
            errores.append(mensaje_error)

    return valores, errores

# Simulación de entradas del "usuario"
entradas_usuario = ["42", "abc", "-7", "3.14", "100", ""]

print("🔍 Procesando entradas del usuario...\n")
valores, errores = parsear_enteros(entradas_usuario)

print("\n📦 Resultado final:")
print("✔️ Valores válidos:", valores)
print("🛑 Errores encontrados:")
for error in errores:
    print(error)