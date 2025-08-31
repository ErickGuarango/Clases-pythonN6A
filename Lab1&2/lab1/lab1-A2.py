def crear_descuento(porcentaje):
    def aplicar_descuento(precio):
        return precio * (1 - porcentaje)
    return aplicar_descuento

#diccionario para las opciones con closures predefinidos 
descuentos={
    "1": ("10%", crear_descuento(0.10)),
    "2": ("25%", crear_descuento(0.20)),
    "3": ("50%", crear_descuento(0.50)),
    "0": ("10%", crear_descuento(0.10)),
}


def mostrar_menu():
    print("GENERADOR DE DECUNTOS")
    print("Selecciona el porentaje de descuentos: ")
    for clave, (etiqueta, _) in descuentos.items():
        print(f" {clave}. {etiqueta}")

if __name__ == "__main__":
    while True:
        mostrar_menu()
        opcion = input(" Ingresa el número de la opción: ").strip()

        if opcion == "0":
            print(" ¡Gracias por usar el generador de descuentos!")
            break

        if opcion not in descuentos:
            print(" Opción inválida. Intenta de nuevo.")
            continue

        try:
            precio = float(input(" Ingresa el precio original: "))
            etiqueta, funcion_descuento = descuentos[opcion]
            precio_final = funcion_descuento(precio)
            print(f"Precio con {etiqueta} de descuento: {precio_final:.2f}")
        except ValueError:
            print(" Entrada inválida. Asegúrate de ingresar un número válido.")
