import pandas as pd
import numpy as np

# ==============================================================================
# 1. PARAMETRIZACIÓN GEOGRÁFICA Y DE PRODUCTOS (30 TELAS)
# ==============================================================================
sucursales_info = [
    {"id": 1, "nombre": "Central Buenos Aires", "peso": 1.5, "perfil": "diversificado"},
    {"id": 2, "nombre": "Filial Córdoba", "peso": 1.1, "perfil": "diversificado"},
    {"id": 3, "nombre": "Filial Rosario", "peso": 1.0, "perfil": "industrial"},
    {"id": 4, "nombre": "Filial Mendoza", "peso": 0.7, "perfil": "agro_turismo"},
    {"id": 5, "nombre": "Filial Mar del Plata", "peso": 0.6, "perfil": "costero_invernal"},
    {"id": 6, "nombre": "Filial Salta", "peso": 0.5, "perfil": "norteño_artesanal"},
    {"id": 7, "nombre": "Filial San Salvador de Jujuy", "peso": 0.4, "perfil": "norteño_artesanal"},
    {"id": 8, "nombre": "Filial Neuquén", "peso": 0.6, "perfil": "industrial_frio"},
    {"id": 9, "nombre": "Filial Ushuaia", "peso": 0.3, "perfil": "frio_extremo"},
    {"id": 10, "nombre": "Filial Posadas", "peso": 0.5, "perfil": "tropical_humedo"},
    {"id": 11, "nombre": "Filial San Miguel de Tucumán", "peso": 0.6, "perfil": "agro_industrial"},
    {"id": 12, "nombre": "Filial Bahía Blanca", "peso": 0.5, "perfil": "industrial"}
]

anios = list(range(2016, 2026))
formatos_alfanumericos = ['M', 'B', 'S', 'C']

telas_info = [
    {"id": "TELA-001", "linea": "indumentaria", "base": 8000, "tipo": "nacional"},
    {"id": "TELA-002", "linea": "alta_costura", "base": 3000, "tipo": "premium_importado"},
    {"id": "TELA-003", "linea": "industrial",   "base": 5000, "tipo": "industrial"},
    {"id": "TELA-004", "linea": "indumentaria", "base": 6500, "tipo": "basico"},
    {"id": "TELA-005", "linea": "alta_costura", "base": 2500, "tipo": "premium"},
    {"id": "TELA-006", "linea": "tapiceria",    "base": 4000, "tipo": "hogar"},
    {"id": "TELA-007", "linea": "indumentaria", "base": 3500, "tipo": "premium"},
    {"id": "TELA-008", "linea": "indumentaria", "base": 4500, "tipo": "premium"},
    {"id": "TELA-009", "linea": "hogar",        "base": 6000, "tipo": "nacional"},
    {"id": "TELA-010", "linea": "tecnicos",     "base": 3000, "tipo": "industrial"},
    {"id": "TELA-011", "linea": "alta_costura", "base": 2800, "tipo": "premium"},
    {"id": "TELA-012", "linea": "tecnicos",     "base": 7000, "tipo": "infraestructura"},
    {"id": "TELA-013", "linea": "industrial",   "base": 5500, "tipo": "industrial"},
    {"id": "TELA-014", "linea": "tapiceria",    "base": 3200, "tipo": "hogar"},
    {"id": "TELA-015", "linea": "hogar",        "base": 9000, "tipo": "basico"},
    {"id": "TELA-016", "linea": "hogar",        "base": 5000, "tipo": "hogar"},
    {"id": "TELA-017", "linea": "hogar",        "base": 4200, "tipo": "nacional"},
    {"id": "TELA-018", "linea": "hogar",        "base": 3800, "tipo": "nacional"},
    {"id": "TELA-019", "linea": "alta_costura", "base": 1200, "tipo": "premium"},
    {"id": "TELA-020", "linea": "indumentaria", "base": 2900, "tipo": "nacional"},
    {"id": "TELA-021", "linea": "indumentaria", "base": 5100, "tipo": "basico"},
    {"id": "TELA-022", "linea": "marroquineria","base": 4600, "tipo": "basico"},
    {"id": "TELA-023", "linea": "tapiceria",    "base": 3400, "tipo": "industrial"},
    {"id": "TELA-024", "linea": "deportivo",    "base": 6800, "tipo": "nacional"},
    {"id": "TELA-025", "linea": "deportivo",    "base": 5500, "tipo": "premium_importado"},
    {"id": "TELA-026", "linea": "indumentaria", "base": 7200, "tipo": "basico"},
    # Nuevas adiciones pedidas:
    {"id": "TELA-027", "linea": "indumentaria", "base": 4100, "tipo": "nacional"},         # Escocés
    {"id": "TELA-028", "linea": "indumentaria", "base": 6900, "tipo": "basico"},           # Bengala Elastizada
    {"id": "TELA-029", "linea": "marroquineria","base": 3100, "tipo": "premium_importado"},# Metalizado Accesorios
    {"id": "TELA-030", "linea": "alta_costura", "base": 2700, "tipo": "premium"}           # Lúrex
]

# Curva de estacionalidad genérica por mes
base_estacional = [0.6, 0.6, 0.8, 1.0, 1.0, 0.9, 0.9, 1.0, 1.0, 0.8, 0.7, 0.6]

rows = []
np.random.seed(2026) # Seed actualizada para mantener aleatoriedad controlada

# ==============================================================================
# 2. ITERACIÓN Y SIMULACIÓN CON INYECCIÓN DE NULOS
# ==============================================================================
for suc in sucursales_info:
    s_id = suc["id"]
    s_nom = suc["nombre"]
    s_perfil = suc["perfil"]
    s_peso = suc["peso"]
    
    for anio in anios:
        for t in telas_info:
            t_id = t["id"]
            linea = t["linea"]
            tipo = t["tipo"]
            
            # --- CAPA DE DISPONIBILIDAD GEOGRÁFICA ---
            factor_geografico = 1.0
            if s_perfil == "norteño_artesanal":
                if t_id in ["TELA-019", "TELA-020"]: factor_geografico = 3.8
                if linea == "alta_costura" and t_id not in ["TELA-019", "TELA-030"]: factor_geografico = 0.15
            elif s_perfil == "frio_extremo":
                if t_id in ["TELA-007", "TELA-016", "TELA-026", "TELA-027"]: factor_geografico = 2.8 # Sobredemanda escocés/abrigos
                if t_id in ["TELA-005", "TELA-008", "TELA-025"]: factor_geografico = 0.05
            elif s_perfil == "tropical_humedo":
                if t_id in ["TELA-005", "TELA-008", "TELA-025"]: factor_geografico = 2.2
                if t_id in ["TELA-007", "TELA-027"]: factor_geografico = 0.02 # No se vende abrigo pesado en el norte tropical
            elif s_perfil == "industrial":
                if linea in ["industrial", "tecnicos"]: factor_geografico = 1.7
                
            v_base = t["base"] * s_peso * factor_geografico
            
            # Quiebres regionales estocásticos (Falta de stock/surtido)
            if np.random.rand() < 0.10 and s_id not in [1, 2]:
                continue 
                
            frmat = formatos_alfanumericos[(hash(s_nom + t_id) % len(formatos_alfanumericos))]
            
            # --- CURVAS DE ESTACIONALIDAD INTERNAS ESPECÍFICAS ---
            curva = list(base_estacional)
            if t_id == "TELA-027": # Escocés: Exclusivo Invierno (Mayo a Agosto)
                curva = [0.1, 0.1, 0.3, 0.9, 1.8, 2.0, 1.9, 1.5, 0.5, 0.2, 0.1, 0.1]
            elif t_id in ["TELA-029", "TELA-030"]: # Metalizados y Lúrex: Fiestas y Cierre de Año (Octubre a Diciembre)
                curva = [0.3, 0.2, 0.2, 0.4, 0.5, 0.5, 0.4, 0.6, 0.9, 1.8, 2.2, 2.5]

            # --- ESTRUCTURA MACRO DE ARGENTINA ---
            factor_anomalia = 1.0
            factor_meses = [1.0] * 12
            
            if anio == 2020:
                if linea in ["indumentaria", "alta_costura", "deportivo", "marroquineria"]:
                    factor_anomalia = 0.22 # El freno en marroquinería de accesorios fue feroz por falta de salidas
                elif linea in ["hogar", "tapiceria"]:
                    factor_anomalia = 1.25
                    for m in [3, 4, 5]: factor_meses[m] = 0.10
                    for m in [8, 9, 10, 11]: factor_meses[m] = 2.0
            elif anio in [2022, 2023]:
                factor_macro = 1.45 if tipo == "nacional" else (0.45 if tipo == "premium_importado" else 1.0)
            elif anio in [2024, 2025]:
                factor_macro = 0.40 if tipo == "infraestructura" else (0.82 if tipo == "basico" else 0.65)
            else:
                factor_macro = 1.0

            ruido = np.random.uniform(0.92, 1.08)
            volumen_ajustado = v_base * factor_anomalia * factor_macro * ruido
            
            ventas_mensuales = []
            suma_curva = sum(curva)
            for idx in range(12):
                prop = curva[idx] / suma_curva if suma_curva > 0 else 0
                v_mes = volumen_ajustado * prop * factor_meses[idx]
                v_mes = max(0, v_mes + np.random.normal(0, v_mes * 0.04))
                ventas_mensuales.append(v_mes)

            # --- EXPORTACIÓN CON NULOS AL ESTILO OMS ---
            v_final = [None] * 12
            
            if frmat == 'M':
                v_final = [round(x, 2) for x in ventas_mensuales]
            elif frmat == 'B':
                v_final[0] = round(ventas_mensuales[0] + ventas_mensuales[1], 2)
                v_final[1] = round(ventas_mensuales[2] + ventas_mensuales[3], 2)
                v_final[2] = round(ventas_mensuales[4] + ventas_mensuales[5], 2)
                v_final[3] = round(ventas_mensuales[6] + ventas_mensuales[7], 2)
                v_final[4] = round(ventas_mensuales[8] + ventas_mensuales[9], 2)
                v_final[5] = round(ventas_mensuales[10] + ventas_mensuales[11], 2)
            elif frmat == 'S':
                v_final[0] = round(sum(ventas_mensuales[0:6]), 2)
                v_final[1] = round(sum(ventas_mensuales[6:12]), 2)
            elif frmat == 'C':
                oi = sum(ventas_mensuales[2:8])
                pv = sum(ventas_mensuales[0:2]) + sum(ventas_mensuales[8:12])
                v_final[0] = round(oi, 2)
                v_final[1] = round(pv, 2)

            row = {'Sucursal_ID': s_id, 'Año': anio, 'Tela_ID': t_id, 'Format_Reporte': frmat}
            for i in range(12):
                row[f'Metros{i+1}'] = v_final[i]
            rows.append(row)

df = pd.DataFrame(rows)
df.to_csv('ventas_historicas_ortelana_argentina.csv', index=False, encoding='utf-8')
print(f"Dataset definitivo compilado con éxito. Filas totales en el lote: {len(df)}")