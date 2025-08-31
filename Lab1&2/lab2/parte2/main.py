from utilidades import normalizar, es_palindromo
from utilidades.numeros import suma_segura, convertir_a_numero

print("✅ Normalizado:", normalizar("  Hola Mundo  "))
print("✅ ¿Es palíndromo 'Anilina'?:", es_palindromo("Anilina"))
print("✅ Suma segura:", suma_segura(10, 5))
print("✅ Convertir '  3.14  ' a número:", convertir_a_numero("  3.14  "))

# Caso límite
try:
    convertir_a_numero("abc")
except ValueError as e:
    print("❌ Error controlado:", e)