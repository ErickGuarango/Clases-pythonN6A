"""
Pipeline de Datos COVID-19 - Ecuador vs Perú
Assets de Dagster para procesamiento automatizado
"""

import pandas as pd
import requests
import numpy as np
from datetime import datetime, date
from dagster import asset, AssetCheckResult, asset_check
from typing import Dict, Any
import os

# =============================================================================
# PASO 2: LECTURA DE DATOS (SIN TRANSFORMAR)
# =============================================================================

@asset
def leer_datos() -> pd.DataFrame:
    """
     PASO 2: Carga datos completos de COVID-19 desde archivo local
    
    FINALIDAD:
    - Obtener dataset completo desde archivo CSV local
    - No aplicar filtros ni transformaciones todavía
    - Establecer punto de entrada único para todo el pipeline
    
    RETORNA:
    - DataFrame con todos los países y fechas disponibles
    """
    print("🔄 Cargando datos desde archivo local...")
    
   
    rutas_posibles = [
        "pipeline_covid/data/covid.csv", 
    ]
    
    df = None
    ruta_usada = None
    
    for ruta in rutas_posibles:
        try:
            df = pd.read_csv(ruta)
            ruta_usada = ruta
            print(f" Archivo encontrado en: {ruta}")
            break
        except FileNotFoundError:
            print(f" No encontrado en: {ruta}")
            continue
        except Exception as e:
            print(f" Error leyendo {ruta}: {e}")
            continue
    
    if df is None:
        print(" ARCHIVO NO ENCONTRADO EN NINGUNA UBICACIÓN")
        print(" SOLUCIONES:")
        print("   1. Coloca el archivo CSV en alguna de estas rutas:")
        for ruta in rutas_posibles:
            print(f"      - {ruta}")
        print("   2. O descarga desde: https://covid.ourworldindata.org/data/owid-covid-data.csv")
        raise FileNotFoundError("No se encontró el archivo de datos COVID-19 en ninguna ubicación esperada")
    
    # Información básica
    print(f" Datos cargados desde {ruta_usada}: {len(df):,} filas, {len(df.columns)} columnas")
    print(f" Rango de fechas: {df['date'].min()} a {df['date'].max()}")
    
    # Verificar que tenemos las columnas mínimas
    if 'location' in df.columns:
        print(f" Países únicos: {df['location'].nunique():,}")
    else:
        print(" No se encontró columna 'location', verificando 'country'...")
        if 'country' in df.columns:
            print(f" Países únicos (country): {df['country'].nunique():,}")
        
    return df

# =============================================================================
# PASO 2: CHEQUEOS DE ENTRADA (VALIDACIONES INICIALES)
# =============================================================================

@asset_check(asset=leer_datos)
def check_fechas_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """ CHEQUEO 1: Verificar fechas futuras (solo informativo)"""
    print(" Verificando fechas futuras...")
    
    df_fechas = leer_datos.copy()
    # Convertir fechas con coerción de errores
    df_fechas['date'] = pd.to_datetime(df_fechas['date'], errors='coerce')
    fecha_max = df_fechas['date'].max()
    hoy = pd.Timestamp(date.today())
    
    # Contar fechas futuras, ignorando NaT
    fechas_futuras = ((df_fechas['date'] > hoy) & (df_fechas['date'].notna())).sum()
    # Siempre pasa, pero reporta el problema
    passed = True
    
    if fechas_futuras == 0:
        description = f" Sin fechas futuras. Máxima: {fecha_max.strftime('%Y-%m-%d') if pd.notnull(fecha_max) else 'N/A'}"
    else:
        description = f" {fechas_futuras} fechas futuras detectadas"
    
    return AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "fecha_maxima": fecha_max.strftime('%Y-%m-%d') if pd.notnull(fecha_max) else "N/A",
            "registros_futuros": int(fechas_futuras)
        }
    )


@asset_check(asset=leer_datos)
def check_columnas_esenciales(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """ CHEQUEO 2: Verificar columnas esenciales del proyecto"""
    print("🔍 Verificando columnas esenciales...")
    
    # Definir columnas requeridas (flexibles por nombre)
    columnas_requeridas = {
        'pais': ['location', 'country'],
        'fecha': ['date'],
        'casos': ['new_cases'],
        'vacunas': ['people_vaccinated'],
        'poblacion': ['population']
    }
    
    # Verificar cada categoría
    resultados = {}
    for categoria, posibles_nombres in columnas_requeridas.items():
        encontrada = any(col in leer_datos.columns for col in posibles_nombres)
        if encontrada:
            nombre_real = next(col for col in posibles_nombres if col in leer_datos.columns)
            resultados[categoria] = {'encontrada': True, 'nombre': nombre_real}
        else:
            resultados[categoria] = {'encontrada': False, 'nombre': None}
    
    # Evaluar resultado - CONVERTIR A BOOLEANO NATIVO
    todas_encontradas = bool(all(r['encontrada'] for r in resultados.values()))
    
    if todas_encontradas:
        columnas_mapa = {cat: res['nombre'] for cat, res in resultados.items()}
        description = f" Todas las columnas encontradas: {columnas_mapa}"
    else:
        faltantes = [cat for cat, res in resultados.items() if not res['encontrada']]
        description = f" Faltan columnas: {faltantes}"
    
    return AssetCheckResult(
        passed=todas_encontradas,  
        description=description,
        metadata={
            "columnas_encontradas": {cat: res['nombre'] for cat, res in resultados.items() if res['encontrada']},
            "columnas_faltantes": [cat for cat, res in resultados.items() if not res['encontrada']]
        }
    )

@asset_check(asset=leer_datos)
def check_paises_objetivo(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """ CHEQUEO 3: Verificar que Ecuador y Perú están disponibles"""
    print(" Verificando países objetivo...")
    
    # Detectar nombre de columna país (flexibilidad)
    col_pais = 'location' if 'location' in leer_datos.columns else 'country'
    
    paises_objetivo = ['Ecuador', 'Peru']
    paises_disponibles = leer_datos[col_pais].unique()
    paises_encontrados = [p for p in paises_objetivo if p in paises_disponibles]
    
    passed = len(paises_encontrados) == len(paises_objetivo)
    
    conteos = {}
    for pais in paises_encontrados:
        conteos[pais] = len(leer_datos[leer_datos[col_pais] == pais])
    
    if passed:
        description = f" Ambos países encontrados: {conteos}"
    else:
        faltantes = [p for p in paises_objetivo if p not in paises_encontrados]
        description = f"❌ Países faltantes: {faltantes}"
    
    return AssetCheckResult(
        passed=bool(passed), 
        description=description,
        metadata={
            "paises_encontrados": paises_encontrados,
            "conteo_registros": conteos
        }
    )

# =============================================================================
# PASO 3: PROCESAMIENTO DE DATOS
# =============================================================================

@asset
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
     PASO 3: Procesar y limpiar datos para análisis
    
    FINALIDAD:
    - Filtrar solo Ecuador y Perú
    - Eliminar registros con datos faltantes críticos
    - Preparar dataset limpio para cálculo de métricas
    - Eliminar duplicados si existen
    
    RETORNA:
    - DataFrame limpio con columnas: location, date, new_cases, people_vaccinated, population
    """
    print(" Procesando datos...")
    
    df = leer_datos.copy()
    print(f" Datos originales: {len(df):,} filas")
    
    # 1. Detectar nombre de columna país (flexibilidad)
    col_pais = 'location' if 'location' in df.columns else 'country'
    print(f"📍 Usando columna de país: '{col_pais}'")
    
    # 2. Filtrar países objetivo
    paises_objetivo = ['Ecuador', 'Peru']
    df_filtrado = df[df[col_pais].isin(paises_objetivo)].copy()
    print(f" Después filtrar países: {len(df_filtrado):,} filas")
    
    # 3. Eliminar duplicados
    duplicados_antes = len(df_filtrado)
    df_filtrado = df_filtrado.drop_duplicates(subset=[col_pais, 'date'])
    duplicados_eliminados = duplicados_antes - len(df_filtrado)
    if duplicados_eliminados > 0:
        print(f" Duplicados eliminados: {duplicados_eliminados}")
    
    # 4. Convertir tipos de datos
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'])
    df_filtrado['new_cases'] = pd.to_numeric(df_filtrado['new_cases'], errors='coerce')
    df_filtrado['people_vaccinated'] = pd.to_numeric(df_filtrado['people_vaccinated'], errors='coerce')
    df_filtrado['population'] = pd.to_numeric(df_filtrado['population'], errors='coerce')
    
    # 5. Eliminar registros sin datos críticos
    # IMPORTANTE: No eliminar por vacunas faltantes (empezaron en 2021)
    antes_limpieza = len(df_filtrado)
    df_limpio = df_filtrado.dropna(subset=['new_cases', 'population'])
    eliminados = antes_limpieza - len(df_limpio)
    print(f"🧹 Registros con casos/población faltantes eliminados: {eliminados}")
    
    # 6. Seleccionar columnas esenciales
    columnas_finales = [col_pais, 'date', 'new_cases', 'people_vaccinated', 'population']
    df_final = df_limpio[columnas_finales].copy()
    
    # 7. Estandarizar nombre de columna país a 'location'
    df_final = df_final.rename(columns={col_pais: 'location'})
    
    # 6. Estadísticas finales
    print(f"📊 Datos procesados finales: {len(df_final):,} filas")
    for pais in paises_objetivo:
        count = len(df_final[df_final['location'] == pais])
        print(f"  📍 {pais}: {count:,} registros")
    
    # 7. Validaciones básicas
    if len(df_final) == 0:
        raise ValueError("❌ No quedan datos después del procesamiento")
    
    print(" Procesamiento completado exitosamente")
    return df_final

# =============================================================================
# PASO 4: CÁLCULO DE MÉTRICAS
# =============================================================================

@asset
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
     PASO 4A: Incidencia acumulada a 7 días por 100 mil habitantes
    """
    print(" Calculando incidencia acumulada 7 días...")
    
    df = datos_procesados.copy()
    
    # 1. Calcular incidencia diaria por 100k habitantes
    df['incidencia_diaria'] = (df['new_cases'] / df['population']) * 100000
    
    # 2. Ordenar por país y fecha
    df = df.sort_values(['location', 'date'])
    
    # 3. Aplicar promedio móvil de 7 días por país
    df['incidencia_7d'] = df.groupby('location')['incidencia_diaria'].rolling(
        window=7, 
        min_periods=1
    ).mean().reset_index(0, drop=True)
    
    # 4. Seleccionar columnas finales
    resultado = df[['date', 'location', 'incidencia_7d']].copy()
    resultado.columns = ['fecha', 'pais', 'incidencia_7d']
    
    # 5. Estadísticas
    print(f" Registros procesados: {len(resultado):,}")
    for pais in resultado['pais'].unique():
        pais_data = resultado[resultado['pais'] == pais]
        max_incidencia = pais_data['incidencia_7d'].max()
        print(f"  📍 {pais}: Máxima incidencia 7d = {max_incidencia:.2f}")
    
    print("✅ Métrica incidencia 7d completada")
    return resultado

@asset 
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
     PASO 4B: Factor de crecimiento semanal
    """
    print(" Calculando factor crecimiento semanal...")
    
    df = datos_procesados.copy()
    df = df.sort_values(['location', 'date'])
    
    resultados = []
    
    for pais in df['location'].unique():
        pais_data = df[df['location'] == pais].copy()
        pais_data = pais_data.set_index('date')
        
        # Calcular suma móvil de 7 días
        pais_data['casos_semana_actual'] = pais_data['new_cases'].rolling(
            window=7, 
            min_periods=7
        ).sum()
        
        # Casos de semana previa (desplazar 7 días)
        pais_data['casos_semana_prev'] = pais_data['casos_semana_actual'].shift(7)
        
        # Calcular factor de crecimiento
        pais_data['factor_crec_7d'] = (
            pais_data['casos_semana_actual'] / pais_data['casos_semana_prev']
        )
        
        # Filtrar solo registros con datos completos
        pais_data_completo = pais_data.dropna(subset=[
            'casos_semana_actual', 
            'casos_semana_prev', 
            'factor_crec_7d'
        ])
        
        # Agregar a resultados
        for fecha, row in pais_data_completo.iterrows():
            resultados.append({
                'semana_fin': fecha,
                'pais': pais,
                'casos_semana': int(row['casos_semana_actual']),
                'factor_crec_7d': round(row['factor_crec_7d'], 3)
            })
    
    resultado = pd.DataFrame(resultados)
    
    print(f" Registros procesados: {len(resultado):,}")
    for pais in resultado['pais'].unique():
        pais_data = resultado[resultado['pais'] == pais]
        factor_promedio = pais_data['factor_crec_7d'].mean()
        print(f"  📍 {pais}: Factor promedio = {factor_promedio:.3f}")
    
    print(" Métrica factor crecimiento 7d completada")
    return resultado

# =============================================================================
# PASO 5: CHEQUEOS DE SALIDA (sobre las métricas)
# =============================================================================

@asset_check(asset=metrica_incidencia_7d)
def check_valores_incidencia(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    """ CHEQUEO SALIDA 1: Validar rangos de incidencia"""
    print("🔍 Validando rangos de incidencia...")
    
    # Validar rango: 0 ≤ incidencia_7d ≤ 2000
    valores_fuera_rango = (
        (metrica_incidencia_7d['incidencia_7d'] < 0) | 
        (metrica_incidencia_7d['incidencia_7d'] > 2000)
    ).sum()
    
    passed = bool(valores_fuera_rango == 0)
    
    if passed:
        max_val = metrica_incidencia_7d['incidencia_7d'].max()
        description = f" Todos los valores en rango válido [0-2000]. Máximo: {max_val:.2f}"
    else:
        description = f" {valores_fuera_rango} valores fuera del rango [0-2000]"
    
    return AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "valores_fuera_rango": int(valores_fuera_rango),
            "total_registros": len(metrica_incidencia_7d),
            "valor_maximo": float(metrica_incidencia_7d['incidencia_7d'].max()),
            "valor_minimo": float(metrica_incidencia_7d['incidencia_7d'].min())
        }
    )

@asset_check(asset=metrica_factor_crec_7d)
def check_factor_crecimiento(metrica_factor_crec_7d: pd.DataFrame) -> AssetCheckResult:
    """ CHEQUEO SALIDA 2: Validar factor de crecimiento"""
    print("🔍 Validando factor de crecimiento...")
    
    # Validar que no hay valores extremos (< 0.1 o > 10)
    valores_extremos = (
        (metrica_factor_crec_7d['factor_crec_7d'] < 0.1) | 
        (metrica_factor_crec_7d['factor_crec_7d'] > 10)
    ).sum()
    
    # Este check es warning, no crítico
    passed = bool(valores_extremos < len(metrica_factor_crec_7d) * 0.05)  # Convertir a bool nativo
    
    if passed:
        description = f" Factor de crecimiento en rangos normales. Extremos: {valores_extremos}"
    else:
        description = f" {valores_extremos} valores extremos de factor de crecimiento"
    
    return AssetCheckResult(
        passed=passed,  # Ahora es un bool nativo
        description=description,
        metadata={
            "valores_extremos": int(valores_extremos),
            "total_registros": len(metrica_factor_crec_7d),
            "factor_promedio_ecuador": float(metrica_factor_crec_7d[metrica_factor_crec_7d['pais'] == 'Ecuador']['factor_crec_7d'].mean()) if 'Ecuador' in metrica_factor_crec_7d['pais'].values else 0,
            "factor_promedio_peru": float(metrica_factor_crec_7d[metrica_factor_crec_7d['pais'] == 'Peru']['factor_crec_7d'].mean()) if 'Peru' in metrica_factor_crec_7d['pais'].values else 0
        }
    )

# =============================================================================
# PASO 6: EXPORTACIÓN DE RESULTADOS (REPORTE FINAL)
# =============================================================================

@asset
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame, 
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    """
     PASO 6: Exportar resultados finales a Excel
    
    FINALIDAD:
    - Combinar todos los resultados en un archivo Excel
    - Crear hojas separadas para cada métrica
    - Generar archivo que se puede commitear al repo
    - Establecer punto final del pipeline
    
    RETORNA:
    - String con ruta del archivo generado
    """
    import os  
    
    print("📋 Generando reporte Excel COVID-19...")
    
    # Crear directorio de salida si no existe
    output_dir = "pipeline_covid/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Nombre del archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    archivo_excel = f"{output_dir}/reporte_covid_ecuador_peru_{timestamp}.xlsx"
    
    # Crear archivo Excel con múltiples hojas
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja 1: Datos procesados
        datos_procesados.to_excel(writer, sheet_name='Datos_Procesados', index=False)
        
        # Hoja 2: Incidencia 7 días
        metrica_incidencia_7d.to_excel(writer, sheet_name='Incidencia_7d', index=False)
        
        # Hoja 3: Factor crecimiento
        metrica_factor_crec_7d.to_excel(writer, sheet_name='Factor_Crec_7d', index=False)
        
        # Hoja 4: Resumen estadístico
        resumen_stats = generar_resumen_estadistico(datos_procesados, metrica_incidencia_7d, metrica_factor_crec_7d)
        resumen_stats.to_excel(writer, sheet_name='Resumen_Estadistico', index=False)
    
    print(f"✅ Reporte Excel generado: {archivo_excel}")
    
    # Estadísticas del archivo generado
    import os
    tamaño_mb = os.path.getsize(archivo_excel) / (1024 * 1024)
    print(f" Archivo generado: {tamaño_mb:.2f} MB")
    
    return archivo_excel

def generar_resumen_estadistico(datos_procesados: pd.DataFrame, incidencia: pd.DataFrame, factor_crec: pd.DataFrame) -> pd.DataFrame:
    """Generar tabla de resumen estadístico"""
    
    resumen = []
    
    for pais in ['Ecuador', 'Peru']:
        # Datos base
        datos_pais = datos_procesados[datos_procesados['location'] == pais]
        inc_pais = incidencia[incidencia['pais'] == pais]
        crec_pais = factor_crec[factor_crec['pais'] == pais]
        
        resumen.append({
            'pais': pais,
            'registros_totales': len(datos_pais),
            'fecha_inicio': datos_pais['date'].min().strftime('%Y-%m-%d') if len(datos_pais) > 0 else 'N/A',
            'fecha_fin': datos_pais['date'].max().strftime('%Y-%m-%d') if len(datos_pais) > 0 else 'N/A',
            'casos_promedio_diario': datos_pais['new_cases'].mean() if len(datos_pais) > 0 else 0,
            'casos_maximo_diario': datos_pais['new_cases'].max() if len(datos_pais) > 0 else 0,
            'incidencia_7d_promedio': inc_pais['incidencia_7d'].mean() if len(inc_pais) > 0 else 0,
            'incidencia_7d_maxima': inc_pais['incidencia_7d'].max() if len(inc_pais) > 0 else 0,
            'factor_crec_promedio': crec_pais['factor_crec_7d'].mean() if len(crec_pais) > 0 else 0,
            'poblacion': datos_pais['population'].iloc[0] if len(datos_pais) > 0 else 0
        })
    
    return pd.DataFrame(resumen)

# =============================================================================
# METADATOS Y DOCUMENTACIÓN
# =============================================================================

def get_asset_metadata() -> Dict[str, Any]:
    """Metadatos para documentación del pipeline"""
    return {
        "descripcion": "Pipeline de análisis COVID-19 para Ecuador vs Perú",
        "fuente_datos": "Our World in Data (OWID)",
        "url_datos": "https://covid.ourworldindata.org/data/owid-covid-data.csv",
        "paises_analizados": ["Ecuador", "Peru"],
        "columnas_clave": ["location", "date", "new_cases", "people_vaccinated", "population"],
        "metricas_calculadas": ["incidencia_7d", "factor_crec_7d"],
        "pasos_pipeline": [
            "1. Lectura datos OWID",
            "2. Chequeos entrada",
            "3. Procesamiento y limpieza", 
            "4. Cálculo métricas",
            "5. Chequeos salida",
            "6. Exportación Excel"
        ]
    }