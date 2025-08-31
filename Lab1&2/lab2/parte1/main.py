from numericas import suma_segura, dividir_sin_error, validar_positivo, raiz_segura

print("✅ Suma segura:", suma_segura(10, 5))
print("✅ División segura:", dividir_sin_error(20, 4))
print("✅ ¿Es positivo 7?:", validar_positivo(7))
print("✅ Raíz segura de 16:", raiz_segura(16))

# Casos límite
try:
    dividir_sin_error(10, 0)
except ValueError as e:
    print("❌ Error controlado en división:", e)

try:
    raiz_segura(-9)
except ValueError as e:
    print("❌ Error controlado en raíz:", e)