from dagster import Definitions

# Import ABSOLUTO (no relativo)
from pipeline_covid.assets import (
    leer_datos,
    datos_procesados,
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    reporte_excel_covid,
    check_fechas_futuras,
    check_columnas_esenciales,
    check_paises_objetivo,
    check_valores_incidencia,
    check_factor_crecimiento
)

defs = Definitions(
    assets=[
        leer_datos,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid,
    ],
    asset_checks=[
        check_fechas_futuras,
        check_columnas_esenciales,
        check_paises_objetivo,
        check_valores_incidencia,
        check_factor_crecimiento
    ],
)