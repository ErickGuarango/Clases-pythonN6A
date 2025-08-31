def saludar(nombre):
    return f"Hola, {nombre}"

def despedir(nombre):
    return f"Adiós, {nombre}"

def aplaudir(nombre):
    return f"{nombre}, ¡bravo!"

def agradecer(nombre):
    return f"Gracias querio {nombre} cliente "

acciones = {
    "1": ("Saludar", saludar),
    "2": ("Despedir", despedir),
    "3": ("Aplaudir", aplaudir),
    "4": ("agradecer", agradecer),
    "0": ("Salir", None)
}

def ejecutar(funcion, *args, **kwargs):
    return funcion(*args, **kwargs)

def mostrar_menu():
    print("\n Centro de Comandos")
    print("Selecciona una acción:")
    for clave, (nombre, _) in acciones.items():
        print(f"  {clave}. {nombre}")

if __name__ == "__main__":
    while True:
        mostrar_menu()
        opcion = input(" Ingresa el número de la acción: ").strip()

        if opcion == "0":
            print("👋 ¡Hasta luego!")
            break

        if opcion not in acciones:
            print("❌ Opción inválida. Intenta de nuevo.")
            continue

        nombre = input(" ¿A quién se aplica la acción (NOMBRE)?: ").strip()
        nombre_accion, funcion = acciones[opcion]
        try:
            resultado = ejecutar(funcion, nombre)
            print(f"✅{resultado}")
        except Exception as e:
            print(f"⚠️ Error al ejecutar la acción: {e}")