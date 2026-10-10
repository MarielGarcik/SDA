# ============================================================
# PRÁCTICA INTEGRADORA - ORTELANA TEXTIL
# Seminario de Actualización - Multiagentes y Automatización Inteligente
# Unidades 1, 2 y 3
# ============================================================
#
# CÓMO USAR ESTE ARCHIVO
# ----------------------
# 1. Recorrelo de arriba hacia abajo. Cada PARTE se apoya en la anterior.
# 2. Leé la explicación de cada parte ANTES de tocar el código.
# 3. Completá los TODO. Están marcados así:   algo = ...
#    Los tres puntos (...) son un valor válido de Python, por eso el archivo
#    se puede ejecutar aunque todavía no hayas completado nada.
# 4. Ejecutá:   python practica_ortelana.py
#    Cada parte tiene una VALIDACIÓN. Si la parte está completa vas a ver
#    [OK]; si falta algo vas a ver [PENDIENTE] con el motivo y la línea.
#    Un [PENDIENTE] no detiene el programa: las demás partes se siguen
#    ejecutando.
# 5. Escribí tus respuestas en las secciones REFLEXIÓN DEL ALUMNO.
# 6. No borres las explicaciones: el archivo terminado es tu material de
#    consulta para la evaluación y para más adelante.
#
# IDEA GENERAL DEL RECORRIDO
# --------------------------
#   Python y datos                      (Parte 1)
#     -> primer agente                  (Parte 2)
#     -> automatización vs agente       (Parte 3)
#     -> agentes especializados         (Parte 4)
#     -> coordinador y ejecutores       (Parte 5, con LangChain)
#     -> comunicación y contexto        (Parte 6)
#     -> secuencialidad y dependencias  (Parte 7, con LangGraph)
#     -> paralelismo y sincronización   (Parte 8, con LangGraph)
#     -> decisiones en el flujo         (Parte 9, con LangGraph)
#     -> ejercicio integrador           (final)
#
# Todo se construye sobre un único caso: ORTELANA TEXTIL.

import json
import sys
import traceback
from pathlib import Path
from typing import TypedDict

import pandas as pd
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

# En algunas consolas de Windows los acentos y la "ñ" se imprimen mal.
# Esta línea le pide a Python que escriba siempre en UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

# Si querés ejecutar una sola parte mientras trabajás en ella, poné acá su
# número (por ejemplo 4). Con None se ejecutan todas. El integrador es la 10.
PARTE_A_EJECUTAR = None

# La carpeta datos/ se busca al lado de este archivo, así el programa
# funciona sin importar desde qué carpeta lo ejecutes.
CARPETA_DATOS = Path(__file__).parent / "datos"

ARCHIVO_VENTAS = CARPETA_DATOS / "ventas_historicas_ortelana_argentina.csv"
ARCHIVO_CATALOGO_CSV = CARPETA_DATOS / "catalogo_telas_ortelana.csv"
ARCHIVO_MAESTRO_JSON = CARPETA_DATOS / "maestro.json"
ARCHIVO_FORMATOS = CARPETA_DATOS / "formatos_reporte_ortelana.csv"

# Las cantidades vendidas están repartidas en doce columnas: Metros1..Metros12.
COLUMNAS_METROS = [f"Metros{i}" for i in range(1, 13)]

# El archivo de ventas solo trae el número de sucursal. Este diccionario
# permite mostrar el nombre en los mensajes.
SUCURSALES = {
    1: "Central Buenos Aires",
    2: "Filial Córdoba",
    3: "Filial Rosario",
    4: "Filial Mendoza",
    5: "Filial Mar del Plata",
    6: "Filial Salta",
    7: "Filial San Salvador de Jujuy",
    8: "Filial Neuquén",
    9: "Filial Ushuaia",
    10: "Filial Posadas",
    11: "Filial San Miguel de Tucumán",
    12: "Filial Bahía Blanca",
}


# ------------------------------------------------------------
# Herramienta de validación (ya resuelta, no hace falta modificarla)
# ------------------------------------------------------------
# Cada parte termina con una función validar_parte_N(). Esta herramienta la
# ejecuta dentro de un try/except: si un assert falla, o si un TODO sin
# completar provoca un error, lo informa y el programa continúa.

RESULTADOS = {}


def ejecutar_validacion(numero, titulo, funcion_validacion):
    if PARTE_A_EJECUTAR is not None and PARTE_A_EJECUTAR != numero:
        return

    print()
    print("=" * 64)
    if numero == 10:
        print(titulo.upper())
    else:
        print(f"PARTE {numero} - {titulo}")
    print("=" * 64)

    try:
        funcion_validacion()
        RESULTADOS[numero] = "OK"
        print(f"[OK] {titulo}: validación superada.")
    except Exception as error:
        # Buscamos la última línea DE ESTE ARCHIVO involucrada en el error,
        # que suele ser el TODO pendiente o el assert que no se cumplió.
        linea = "?"
        for paso in traceback.extract_tb(error.__traceback__):
            if Path(paso.filename).name == Path(__file__).name:
                linea = paso.lineno
        RESULTADOS[numero] = "PENDIENTE"
        print(f"[PENDIENTE] {titulo}: todavía no pasa la validación.")
        print(f"            Motivo: {type(error).__name__}: {error}")
        print(f"            Revisá cerca de la línea {linea}.")
        if "ellipsis" in str(error) or "NoneType" in str(error):
            print("            (Ese motivo suele indicar un TODO sin completar,")
            print("             de esta parte o de una parte anterior.)")


# ============================================================
# PARTE 1 - CONOCIENDO ORTELANA
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Cargar los archivos de Ortelana Textil y mirar qué información traen:
# qué columnas tienen, cómo se ve una fila, qué falta y qué no coincide
# entre un archivo y otro.

# ¿POR QUÉ?
# Un agente solo puede decidir sobre aquello que puede observar. Antes de
# escribir un agente tenemos que saber cómo es su ENTORNO. En esta práctica
# el entorno son estos archivos. Si los interpretamos mal, todo lo que el
# agente decida después va a estar mal aunque el código "funcione".

# CONCEPTOS TRABAJADOS
# - PERCEPCIÓN: lo que el agente puede observar del entorno.
# - Información disponible e información faltante.
# - Lectura de datasets (CSV y JSON) con operaciones simples.

# EL CASO
# Ortelana Textil vende telas por metro en 12 sucursales de Argentina.
# Tenemos 10 años de historia (2016 a 2025) y 30 telas.
#
# ARCHIVOS DISPONIBLES EN datos/
#
# 1) ventas_historicas_ortelana_argentina.csv
#    Una fila = las ventas de UNA tela, en UNA sucursal, durante UN año.
#    Columnas: Sucursal_ID, Año, Tela_ID, Format_Reporte, Metros1..Metros12.
#
# 2) formatos_reporte_ortelana.csv
#    Diccionario que explica la columna Format_Reporte.
#
# 3) maestro.json
#    Catálogo semántico de las 30 telas: nombre, línea textil, composición,
#    origen de la fibra, tipo y etiquetas (tags_regionales).
#
# 4) catalogo_telas_ortelana.csv
#    Un catálogo en formato tabla, con las mismas columnas básicas pero
#    sin etiquetas.

ventas = pd.read_csv(ARCHIVO_VENTAS)
formatos = pd.read_csv(ARCHIVO_FORMATOS)
catalogo_csv = pd.read_csv(ARCHIVO_CATALOGO_CSV)

with open(ARCHIVO_MAESTRO_JSON, encoding="utf-8") as archivo:
    maestro = json.load(archivo)

# IMPORTANTE: NO TODAS LAS FILAS TIENEN LA MISMA GRANULARIDAD TEMPORAL
# --------------------------------------------------------------------
# Cada sucursal informa sus ventas con un formato distinto según la tela.
# La columna Format_Reporte indica cómo leer Metros1..Metros12:
#
#   M (mensual)    -> 12 valores. Metros1 = enero ... Metros12 = diciembre.
#   B (bimestral)  -> 6 valores.  Metros1 = ene+feb ... Metros6 = nov+dic.
#   S (semestral)  -> 2 valores.  Metros1 = ene a jun, Metros2 = jul a dic.
#   C (temporadas) -> 2 valores.  Metros1 = otoño-invierno (marzo a agosto),
#                                 Metros2 = primavera-verano (sept. a febrero).
#
# Consecuencia: "Metros1" NO significa siempre lo mismo. En una fila es enero,
# en otra es medio año. Sumar todas las columnas de una fila siempre da el
# total del AÑO; cualquier pregunta más fina (un mes, un semestre) exige mirar
# primero el formato.
#
# LA DOCUMENTACIÓN NO COINCIDE DEL TODO CON LOS DATOS
# ---------------------------------------------------
# Esto pasa en los sistemas reales y es parte de lo que hay que percibir:
#
# - El diccionario de formatos llama "E" al formato de temporadas, pero en las
#   ventas el código real es "C".
# - El diccionario dice que las columnas sin uso valen 0.0, pero en el archivo
#   están VACÍAS (pandas las muestra como NaN).
# - La primera columna del diccionario se llama "Frmat" (sin la "o").
# - El catálogo CSV y el maestro JSON no describen la misma cantidad de telas
#   (lo vas a medir en el TODO 1.3).
#
# Cuando la documentación y los datos se contradicen, el agente tiene que
# guiarse por lo que realmente observa.

# QUÉ ESTÁ RESUELTO
# La carga de los cuatro archivos y la impresión de ejemplos (dentro de
# validar_parte_1, más abajo).

# QUÉ TENÉS QUE COMPLETAR
# Tres observaciones sobre el entorno (TODO 1.1 a 1.3).

# TODO 1.1
# Indicá cuántas columnas MetrosN trae completas una fila de cada formato.
# No lo adivines: leé la explicación de arriba y mirá las filas de ejemplo
# que imprime la validación. El valor de "M" ya está resuelto.
PERIODOS_POR_FORMATO = {
    "M": 12, # mensual: trae un valor por cada mes, de Metros1 a Metros12
    "B": 6, # bimestral: trae un valor por cada período de dos meses. Hay seis bimestres en un año.
    "S": 2, # semestral: trae un valor por cada semestre. Hay dos semestres en un año.
    "C": 2, # temporadas: trae un valor por cada temporada. Hay dos temporadas en un año.
}

# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 1.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------

# TODO 1.2
# Si todas las sucursales hubieran informado todas las telas en todos los
# años, ¿cuántas filas tendría el archivo de ventas? ¿Cuántas faltan?
# Pista: ventas["Columna"].nunique() devuelve cuántos valores distintos hay
#        en una columna, y len(ventas) devuelve la cantidad de filas.
combinaciones_esperadas = (
    ventas["Sucursal_ID"].nunique()
    * ventas["Tela_ID"].nunique()
    * ventas["Año"].nunique()
)
combinaciones_faltantes = combinaciones_esperadas - len(ventas)

# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 1.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------

# TODO 1.3
# Armá la lista de telas que aparecen en las ventas pero NO tienen fila en el
# catálogo CSV. Esta observación va a definir, en la Parte 4, de qué archivo
# debe leer el agente que describe productos.
# Pista: recorré ventas["Tela_ID"].unique() con un for y usá "not in" contra
#        list(catalogo_csv["id"]).
telas_sin_ficha_en_csv = [
    tela_id
    for tela_id in ventas["Tela_ID"].unique()
    if tela_id not in list(catalogo_csv["id"])
]

# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 1.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 1
# ============================================================
# Qué mirar en la salida:
# - Las cuatro filas de ejemplo: una por formato. Fijate cuántas columnas
#   tienen valor en cada una y dónde aparecen los NaN.
# - La ficha del maestro: fijate que los tags están "adentro" de metadatos.


def validar_parte_1():
    print("Ventas:", ventas.shape[0], "filas y", ventas.shape[1], "columnas")
    print("Columnas:", list(ventas.columns))
    print("Años:", ventas["Año"].min(), "a", ventas["Año"].max())
    print("Sucursales:", ventas["Sucursal_ID"].nunique())
    print("Telas distintas en ventas:", ventas["Tela_ID"].nunique())
    print("Filas por formato:", ventas["Format_Reporte"].value_counts().to_dict())

    print()
    print("Una fila de ejemplo por cada formato (Central Buenos Aires, 2016):")
    for tela in ["TELA-005", "TELA-004", "TELA-001", "TELA-002"]:
        fila = ventas[
            (ventas["Sucursal_ID"] == 1)
            & (ventas["Año"] == 2016)
            & (ventas["Tela_ID"] == tela)
        ]
        print(fila.to_string(index=False, header=False))

    print()
    print("Diccionario de formatos (columnas):", list(formatos.columns))
    print("Códigos que documenta:", list(formatos["Frmat"]))
    print("Códigos que usan las ventas:", sorted(ventas["Format_Reporte"].unique()))

    print()
    print("Catálogo CSV:", len(catalogo_csv), "telas | columnas:", list(catalogo_csv.columns))
    print("Maestro JSON:", len(maestro), "telas")
    print("Ejemplo de ficha del maestro:")
    print(json.dumps(maestro[26], ensure_ascii=False, indent=2))

    # --- Comprobaciones de los TODO ---
    for formato, esperado in PERIODOS_POR_FORMATO.items():
        filas = ventas[ventas["Format_Reporte"] == formato]
        con_valor = filas[COLUMNAS_METROS].notna().sum(axis=1)
        assert (con_valor == esperado).all(), (
            f"TODO 1.1: las filas con formato {formato} no traen {esperado} valores"
        )

    assert combinaciones_esperadas == 3600, "TODO 1.2: revisá las combinaciones esperadas"
    assert combinaciones_faltantes == 315, "TODO 1.2: revisá las combinaciones faltantes"
    print()
    print("Combinaciones esperadas:", combinaciones_esperadas)
    print("Combinaciones faltantes:", combinaciones_faltantes)

    assert len(telas_sin_ficha_en_csv) == 14, "TODO 1.3: deberían ser 14 telas"
    assert "TELA-027" in telas_sin_ficha_en_csv, "TODO 1.3: falta TELA-027"
    print("Telas vendidas que no están en el catálogo CSV:", len(telas_sin_ficha_en_csv))


ejecutar_validacion(1, "Conociendo Ortelana", validar_parte_1)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué representa una fila del archivo de ventas.
# - Por qué Metros1 no significa lo mismo en todas las filas.
# - Por qué no se puede asumir que existe una fila para cada combinación
#   de sucursal, tela y año.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 1
# ============================================================

# Un agente recibe la pregunta "¿cuánto se vendió en marzo de 2018 de la
# TELA-001 en Central Buenos Aires?". Mirá el formato de esa fila.
# ¿Puede responderla con lo que percibe? ¿Por qué?
# RESPUESTA: No puede determinarlo exactamente porque esa fila tiene formato S (semestral). Metros1 contiene el total de enero a junio, sin separar cuánto corresponde a cada mes. Dividirlo por seis daría un promedio, no la venta real de marzo.

# Si para una sucursal, tela y año no hay fila, ¿es lo mismo que haber vendido
# cero metros? ¿Qué error cometería un agente que los trate igual?
# RESPUESTA: No. Una fila faltante indica que no hay información registrada; cero metros indica que hay un registro que confirma que no hubo ventas. Si el agente los tratara igual, podría interpretar la falta de información como una caída de ventas y sacar conclusiones equivocadas.

# Según lo que mediste en el TODO 1.3, ¿de qué archivo conviene que un agente
# obtenga la descripción de los productos? ¿Qué perdería si usara el otro?
# RESPUESTA: Conviene usar maestro.json, porque contiene las 30 telas y sus etiquetas (tags_regionales). El catálogo CSV solo contiene 16 telas: se perderían las descripciones de 14 telas que aparecen en las ventas y las etiquetas necesarias para buscar productos por características.

# La documentación dice que el formato de temporadas se llama "E" y los datos
# usan "C". ¿A cuál de los dos le debe creer el agente y por qué?
# RESPUESTA: Para interpretar este archivo, debe usar C, porque es el código que realmente aparece en las ventas. Debe reconocer la diferencia con el código E de la documentación y dejarla señalada para corregirla, evitando ignorar o interpretar mal esos registros.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 1
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 2 - PRIMER AGENTE
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Construir un agente sencillo, AgenteMonitorTela, que vigila cómo le fue a
# una tela en una sucursal respecto del año anterior y emite un aviso.

# ¿POR QUÉ?
# La gerencia no quiere mirar 3.285 filas. Quiere que alguien le diga
# "esta tela cayó fuerte" o "esta tela está estable". Es un problema chico
# y real, ideal para ver las piezas de un agente una por una.

# CONCEPTOS TRABAJADOS
# - Objetivo: para qué existe el agente.
# - Percepción: qué observa del entorno (filas del archivo de ventas).
# - Contexto: lo que sabe además de lo que observa (el umbral, lo ya visto).
# - Decisión: elegir entre varias conclusiones posibles.
# - Acción: producir un resultado hacia afuera (el aviso).
# - Memoria básica: recordar lo que ya analizó.
#
# El ciclo completo, que vas a ver reflejado en los métodos de la clase:
#
#     percibir()  ->  decidir()  ->  actuar()  ->  memoria
#
# QUÉ ESTÁ RESUELTO
# La estructura de la clase, percibir() y actuar().

# QUÉ TENÉS QUE COMPLETAR
# - TODO 2.1: sumar los metros de una o más filas tratando bien los vacíos.
# - TODO 2.2: la regla de decisión.
# - TODO 2.3: el uso de la memoria.


def sumar_metros(filas):
    # Recibe un conjunto de filas del archivo de ventas (un DataFrame) y
    # devuelve el total de metros vendidos, sumando Metros1..Metros12.
    #
    # Esta función se va a reutilizar en toda la práctica. Funciona para
    # cualquier formato de reporte porque el total ANUAL es la suma de todas
    # las columnas informadas, sean 12, 6 o 2.

    # TODO 2.1
    # Calculá el total. Tené en cuenta que las columnas sin uso están vacías
    # (NaN). Decidí explícitamente cómo tratarlas antes de sumar.
    # Pistas:
    # - filas[COLUMNAS_METROS] se queda solo con las doce columnas de metros.
    # - .fillna(0) reemplaza los vacíos por cero.
    # - .sum() suma cada columna; un segundo .sum() suma esos doce resultados.
    total = filas[COLUMNAS_METROS].fillna(0).sum().sum()

    return round(float(total), 2)


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 2.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


class AgenteMonitorTela:

    def __init__(self, ventas, umbral=20.0):
        # OBJETIVO: aquello que el agente intenta lograr. Lo dejamos escrito
        # como dato del agente para no perderlo de vista.
        self.objetivo = (
            "Avisar si una tela cambió fuertemente sus ventas en una sucursal "
            "respecto del año anterior"
        )

        # ENTORNO: de dónde percibe.
        self.ventas = ventas

        # CONTEXTO: conocimiento que el agente usa para interpretar lo que
        # percibe. Un cambio de 5% y uno de 70% no significan lo mismo; el
        # umbral es lo que le permite distinguirlos.
        self.umbral = umbral

        # MEMORIA BÁSICA: lista con los análisis que ya hizo.
        self.memoria = []

    def _filas(self, sucursal_id, tela_id, anio):
        # Método auxiliar: obtiene las filas de una sucursal, tela y año.
        return self.ventas[
            (self.ventas["Sucursal_ID"] == sucursal_id)
            & (self.ventas["Tela_ID"] == tela_id)
            & (self.ventas["Año"] == anio)
        ]

    def percibir(self, sucursal_id, tela_id, anio):
        # PERCEPCIÓN: el agente observa dos cosas del entorno, las ventas del
        # año pedido y las del año anterior. También registra SI HAY datos,
        # porque "no hay fila" y "se vendió cero" no son lo mismo (Parte 1).
        filas_actual = self._filas(sucursal_id, tela_id, anio)
        filas_anterior = self._filas(sucursal_id, tela_id, anio - 1)

        return {
            "sucursal_id": sucursal_id,
            "tela_id": tela_id,
            "anio": anio,
            "hay_actual": len(filas_actual) > 0,
            "hay_anterior": len(filas_anterior) > 0,
            "total_actual": sumar_metros(filas_actual),
            "total_anterior": sumar_metros(filas_anterior),
        }

    def decidir(self, percepcion):
        # DECISIÓN: a partir de lo percibido y del contexto (self.umbral), el
        # agente elige UNA de estas etiquetas:
        #
        #   "SIN_DATOS"          falta el año pedido o el anterior
        #   "CAIDA_FUERTE"       la variación es menor o igual a -umbral
        #   "CRECIMIENTO_FUERTE" la variación es mayor o igual a +umbral
        #   "ESTABLE"            cualquier otro caso
        #
        # La variación porcentual se calcula así:
        #   (total_actual - total_anterior) / total_anterior * 100

        # TODO 2.2
        # Implementá la regla. Empezá por el caso sin datos: si no hay con qué
        # comparar, no tiene sentido calcular nada y la variación debe ser None.
        if not percepcion["hay_actual"] or not percepcion["hay_anterior"]:
            etiqueta = "SIN_DATOS"
            variacion = None
        elif percepcion["total_anterior"] == 0:
            # Sin una base distinta de cero no se puede calcular el porcentaje.
            etiqueta = "SIN_DATOS"
            variacion = None
        else:
            variacion = (
                (percepcion["total_actual"] - percepcion["total_anterior"])
                / percepcion["total_anterior"]
                * 100
            )
            if variacion <= -self.umbral:
                etiqueta = "CAIDA_FUERTE"
            elif variacion >= self.umbral:
                etiqueta = "CRECIMIENTO_FUERTE"
            else:
                etiqueta = "ESTABLE"
            variacion = round(variacion, 2)

        return {"etiqueta": etiqueta, "variacion_pct": variacion}

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 2.2
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def actuar(self, percepcion, decision):
        # ACCIÓN: lo que el agente hace hacia afuera. Acá es redactar un
        # aviso; en un sistema real podría ser enviar un mail o abrir un
        # pedido de reposición. Notá que la acción depende de la decisión.
        sucursal = SUCURSALES[percepcion["sucursal_id"]]
        tela = percepcion["tela_id"]
        anio = percepcion["anio"]

        if decision["etiqueta"] == "SIN_DATOS":
            return (
                f"{tela} en {sucursal}, {anio}: no hay datos suficientes "
                f"para comparar con {anio - 1}."
            )

        return (
            f"{tela} en {sucursal}, {anio}: {decision['etiqueta']} "
            f"({decision['variacion_pct']}% respecto de {anio - 1})."
        )

    def ejecutar(self, sucursal_id, tela_id, anio):
        # Un ciclo completo del agente.
        consulta = (sucursal_id, tela_id, anio)

        # TODO 2.3 (a)
        # Antes de percibir, revisá la memoria. Si ya hay un resultado cuya
        # "consulta" sea igual a esta, devolvelo directamente con return.
        # Pista: recorré self.memoria con un for.
        for resultado_previo in self.memoria:
            if resultado_previo["consulta"] == consulta:
                return resultado_previo

        percepcion = self.percibir(sucursal_id, tela_id, anio)
        decision = self.decidir(percepcion)
        mensaje = self.actuar(percepcion, decision)

        resultado = {
            "consulta": consulta,
            "percepcion": percepcion,
            "decision": decision,
            "mensaje": mensaje,
        }

        # TODO 2.3 (b)
        # Guardá el resultado en la memoria para no repetir el trabajo.
        self.memoria.append(resultado)

        return resultado

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 2.3
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 2
# ============================================================
# Qué comprobar:
# - Que la suma de metros da lo mismo en una fila mensual (12 valores) y en
#   una semestral (2 valores y 10 vacíos): ninguna debe dar NaN.
# - Que el agente llega a las cuatro etiquetas posibles con casos reales.
# - Que repetir una consulta no agrega un recuerdo nuevo.


def validar_parte_2():
    fila_semestral = ventas[
        (ventas["Sucursal_ID"] == 1) & (ventas["Año"] == 2016) & (ventas["Tela_ID"] == "TELA-001")
    ]
    fila_mensual = ventas[
        (ventas["Sucursal_ID"] == 1) & (ventas["Año"] == 2016) & (ventas["Tela_ID"] == "TELA-005")
    ]
    assert sumar_metros(fila_semestral) == 11746.4, "TODO 2.1: revisá el tratamiento de los vacíos"
    assert sumar_metros(fila_mensual) == 3885.53, "TODO 2.1: revisá la suma de las doce columnas"

    agente = AgenteMonitorTela(ventas)
    print("Objetivo del agente:", agente.objetivo)

    caida = agente.ejecutar(1, "TELA-001", 2020)
    crecimiento = agente.ejecutar(1, "TELA-009", 2020)
    estable = agente.ejecutar(1, "TELA-001", 2018)
    sin_datos = agente.ejecutar(3, "TELA-001", 2019)

    for resultado in [caida, crecimiento, estable, sin_datos]:
        print(" -", resultado["mensaje"])

    assert caida["decision"]["etiqueta"] == "CAIDA_FUERTE", "TODO 2.2: caso caída"
    assert crecimiento["decision"]["etiqueta"] == "CRECIMIENTO_FUERTE", "TODO 2.2: caso crecimiento"
    assert estable["decision"]["etiqueta"] == "ESTABLE", "TODO 2.2: caso estable"
    assert sin_datos["decision"]["etiqueta"] == "SIN_DATOS", "TODO 2.2: caso sin datos"
    assert sin_datos["decision"]["variacion_pct"] is None, "TODO 2.2: sin datos no hay variación"
    assert abs(caida["decision"]["variacion_pct"] - (-76.12)) < 0.01, "TODO 2.2: variación mal calculada"

    assert len(agente.memoria) == 4, "TODO 2.3 (b): la memoria debería tener 4 análisis"
    agente.ejecutar(1, "TELA-001", 2020)
    assert len(agente.memoria) == 4, "TODO 2.3 (a): una consulta repetida no debe sumar recuerdos"
    print("Análisis recordados:", len(agente.memoria))


ejecutar_validacion(2, "Primer agente", validar_parte_2)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué línea del código corresponde a percibir, cuál a decidir y cuál a actuar.
# - Qué papel cumple el umbral y por qué lo llamamos contexto.
# - Qué cambia en el comportamiento del agente gracias a la memoria.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 2
# ============================================================

# ¿Cuál es el objetivo de AgenteMonitorTela? Decilo con tus palabras, sin
# copiar el texto del código.
# RESPUESTA:
# Detectar aumentos o caídas importantes en las ventas anuales de una tela
# en una sucursal, comparándolas con el año anterior, y emitir un aviso.

# ¿Qué percibe exactamente el agente y qué parte del entorno NO percibe?
# RESPUESTA:
# Consulta las filas de ventas de la sucursal y tela pedidas para el año
# indicado y el anterior. Obtiene sus totales y registra si existen datos.
# No observa el stock, los precios, las causas de los cambios ni las fichas
# de productos; tampoco analiza otras sucursales o años en esa consulta.

# El mismo dato (+28% en TELA-009) fue clasificado como CRECIMIENTO_FUERTE.
# ¿Qué habría decidido el agente con umbral=30? ¿Qué te dice eso sobre la
# relación entre contexto y decisión?
# RESPUESTA:
# Con umbral=30 habría decidido ESTABLE, porque un aumento del 28% no
# alcanza el 30%. El mismo dato puede generar una decisión diferente según
# el contexto: el umbral define qué cambio se considera fuerte. ESTABLE no
# significa que no hubo cambio, sino que no se alcanzó el umbral.

# ¿Por qué el agente distingue "SIN_DATOS" en lugar de informar una caída
# del 100%? Relacionalo con lo que viste en la Parte 1.
# RESPUESTA:
# Porque la ausencia de una fila indica falta de información, no ventas
# iguales a cero. Informar una caída del 100% supondría que hubo ventas
# registradas de cero metros, algo que no puede afirmarse sin ese registro.
# Como en la Parte 1, hay que distinguir los datos faltantes de los ceros.

# La memoria actual solo evita repetir trabajo. Proponé otro uso de la
# memoria que cambiaría una DECISIÓN del agente.
# RESPUESTA:
# Podría recordar las variaciones de varios años y detectar caídas
# consecutivas. Por ejemplo, tres caídas anuales del 10% podrían activar una
# alerta por descenso sostenido, aunque ninguna alcance el umbral del 20%.
# Así, el historial cambiaría la decisión que se toma mirando un solo año.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 2
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 3 - AUTOMATIZACIÓN VS AGENTE
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Resolver la MISMA pregunta de dos maneras:
#
#   "¿Cuántos metros se vendieron en el PRIMER SEMESTRE (enero a junio)
#    de una tela, en una sucursal y un año?"
#
#   a) con una automatización: una función que siempre hace lo mismo;
#   b) con un agente: observa en qué formato está el dato y decide qué hacer.

# ¿POR QUÉ?
# La diferencia entre automatizar y usar un agente se entiende mejor viendo
# dónde falla la automatización. Este dataset es ideal para eso: la pregunta
# es simple, pero la respuesta correcta depende del formato de cada fila.

# CONCEPTOS TRABAJADOS
# - Automatización tradicional: pasos fijos, sin interpretar la situación.
# - Agente: tiene un objetivo, percibe la situación y elige cómo actuar.
# - Una acción válida de un agente es reconocer que NO puede cumplir el
#   objetivo con lo que percibe.

# QUÉ ESTÁ RESUELTO
# La automatización completa y la estructura del agente (percibir y el caso
# mensual).

# QUÉ TENÉS QUE COMPLETAR
# - TODO 3.1: la decisión del agente según el formato.
# - TODO 3.2: la acción correspondiente a cada estrategia.
# - TODO 3.3: tu conclusión escrita.


def primer_semestre_automatizacion(ventas, sucursal_id, tela_id, anio):
    # AUTOMATIZACIÓN: "el primer semestre son los primeros seis meses, así
    # que sumo Metros1 a Metros6". Es una regla fija. No mira el formato y
    # no se pregunta si la fila existe.
    filas = ventas[
        (ventas["Sucursal_ID"] == sucursal_id)
        & (ventas["Tela_ID"] == tela_id)
        & (ventas["Año"] == anio)
    ]
    total = filas[COLUMNAS_METROS[:6]].fillna(0).sum().sum()
    return round(float(total), 2)


class AgentePrimerSemestre:

    def __init__(self, ventas):
        self.objetivo = "Informar los metros vendidos entre enero y junio"
        self.ventas = ventas

    def percibir(self, sucursal_id, tela_id, anio):
        # El agente observa la fila completa, INCLUIDO su formato de reporte.
        # Devuelve None si la combinación no existe.
        filas = self.ventas[
            (self.ventas["Sucursal_ID"] == sucursal_id)
            & (self.ventas["Tela_ID"] == tela_id)
            & (self.ventas["Año"] == anio)
        ]
        if len(filas) == 0:
            return None
        return filas.iloc[0]

    def decidir(self, fila):
        # El agente elige una ESTRATEGIA según lo que percibe:
        #
        #   "SIN_DATOS"         no existe la fila
        #   "SUMAR_6_MESES"     formato M: enero a junio son Metros1..Metros6
        #   "SUMAR_3_BIMESTRES" formato B: enero a junio son ...
        #   "TOMAR_1_SEMESTRE"  formato S: enero a junio es ...
        #   "NO_RESPONDIBLE"    el formato no permite aislar enero a junio
        if fila is None:
            return "SIN_DATOS"

        formato = fila["Format_Reporte"]

        if formato == "M":
            return "SUMAR_6_MESES"

        # TODO 3.1
        # Completá la decisión para los formatos B, S y C. Volvé a leer en la
        # Parte 1 qué meses cubre cada columna en cada formato y preguntate:
        # ¿con estas columnas puedo reconstruir exactamente enero a junio?
        # Uno de los tres formatos no lo permite.
        if formato == "B":
            return "SUMAR_3_BIMESTRES"

        if formato == "S":
            return "TOMAR_1_SEMESTRE"

        return "NO_RESPONDIBLE"

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 3.1
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def actuar(self, fila, estrategia):
        # La acción devuelve siempre un diccionario con la misma forma, para
        # que quien lo reciba sepa si puede confiar en el número:
        #   respondible: True / False
        #   metros:      el valor, o None si no se pudo responder
        #   motivo:      explicación breve
        if estrategia == "SIN_DATOS":
            return {
                "respondible": False,
                "metros": None,
                "motivo": "No existe registro para esa sucursal, tela y año.",
            }

        if estrategia == "SUMAR_6_MESES":
            metros = fila[COLUMNAS_METROS[:6]].sum()
            return {
                "respondible": True,
                "metros": round(float(metros), 2),
                "motivo": "Reporte mensual: se sumaron enero a junio.",
            }

        # TODO 3.2
        # Completá la acción para "SUMAR_3_BIMESTRES" y "TOMAR_1_SEMESTRE",
        # devolviendo un diccionario con la misma forma que los anteriores.
        # Pista: fila["Metros1"] devuelve un valor; fila[COLUMNAS_METROS[:3]]
        #        devuelve los tres primeros.
        if estrategia == "SUMAR_3_BIMESTRES":
            metros = fila[COLUMNAS_METROS[:3]].sum()
            return {
                "respondible": True,
                "metros": round(float(metros), 2),
                "motivo": "Reporte bimestral: se sumaron tres bimestres.",
            }

        if estrategia == "TOMAR_1_SEMESTRE":
            metros = fila["Metros1"]
            return {
                "respondible": True,
                "metros": round(float(metros), 2),
                "motivo": "Reporte semestral: se tomó el primer semestre.",
            }

        # Si llegamos acá, la estrategia es "NO_RESPONDIBLE".
        return {
            "respondible": False,
            "metros": None,
            "motivo": (
                f"El formato {fila['Format_Reporte']} agrupa los meses de una "
                "forma que no permite aislar enero a junio."
            ),
        }

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 3.2
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def ejecutar(self, sucursal_id, tela_id, anio):
        fila = self.percibir(sucursal_id, tela_id, anio)
        estrategia = self.decidir(fila)
        return self.actuar(fila, estrategia)


# ============================================================
# VALIDACIÓN - PARTE 3
# ============================================================
# Qué comprobar: la tabla que se imprime compara las dos soluciones sobre
# cinco casos reales. Mirá en cuáles coinciden y en cuáles no, y en cuáles la
# automatización devuelve un número que PARECE razonable pero es incorrecto.


def validar_parte_3():
    agente = AgentePrimerSemestre(ventas)

    casos = [
        (1, "TELA-005", 2016),  # formato M
        (1, "TELA-004", 2016),  # formato B
        (1, "TELA-001", 2016),  # formato S
        (1, "TELA-002", 2016),  # formato C
        (3, "TELA-001", 2019),  # combinación inexistente
    ]

    respuestas = []
    for sucursal_id, tela_id, anio in casos:
        fijo = primer_semestre_automatizacion(ventas, sucursal_id, tela_id, anio)
        respuesta = agente.ejecutar(sucursal_id, tela_id, anio)
        respuestas.append(respuesta)
        print(f"{tela_id} suc.{sucursal_id} {anio} | automatización: {fijo:>9} | agente: {respuesta}")

    assert respuestas[0]["metros"] == 1939.57, "caso mensual"
    assert respuestas[1]["respondible"] is True, "TODO 3.1: el formato B sí permite responder"
    assert respuestas[1]["metros"] == 4998.18, "TODO 3.2: caso bimestral"
    assert respuestas[2]["respondible"] is True, "TODO 3.1: el formato S sí permite responder"
    assert respuestas[2]["metros"] == 5826.63, "TODO 3.2: caso semestral"
    assert respuestas[3]["respondible"] is False, "TODO 3.1: el formato C no permite aislar enero a junio"
    assert respuestas[4]["respondible"] is False, "caso sin datos"


ejecutar_validacion(3, "Automatización vs agente", validar_parte_3)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - En qué casos la automatización acierta y por qué acierta solo en esos.
# - Qué observa el agente que la automatización ignora.
# - Por qué "no puedo responder" es una respuesta mejor que un número erróneo.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 3
# ============================================================

# TODO 3.3 - CONCLUSIÓN
# Mirando la tabla de la validación: ¿qué número devolvió la automatización
# para la TELA-004 y qué representa realmente ese número? ¿Qué problema
# tendría la gerencia si lo usara?
# RESPUESTA:
# La automatización, para TELA-004, suma seis columnas que representan todo el año, no solamente el primer semestre. 
# Devuelve un número válido matemáticamente, pero incorrecto para la pregunta de la gerencia.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 3.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------

# Para la combinación inexistente la automatización devolvió 0.0 y el agente
# devolvió "no existe registro". ¿Cuál de las dos respuestas es más útil
# para quien toma decisiones y por qué?
# RESPUESTA:
# La respuesta del agente es más útil porque indica que no hay datos disponibles para esa combinación, 
# mientras que la automatización devuelve 0.0, lo que podría interpretarse erróneamente como que no hubo ventas, 
# cuando en realidad no hay información registrada.

# Escribí con tus palabras la diferencia entre la función y el agente, usando
# los términos objetivo, percepción y decisión.
# RESPUESTA:
# La función tiene un objetivo fijo: sumar los metros del primer semestre, sin importar el formato de los datos.
# Su percepción es limitada, ya que no observa el formato de las filas ni si existen o no. 
# La decisión es automática y no considera las condiciones del entorno.
# El agente, en cambio, tiene un objetivo similar pero adapta su comportamiento según lo que percibe. 
# Observa el formato de las filas y decide la estrategia adecuada para calcular los metros vendidos, o reconoce cuando no puede responder debido

# ¿En qué situación elegirías igualmente la automatización simple? Pensá en
# un caso donde TODOS los datos tengan el mismo formato.
# RESPUESTA:
# Si todos los datos estuvieran en formato mensual (M), la automatización simple sería suficiente, 
# ya que siempre sumaría las columnas correctas para el primer semestre. 
# En este caso, no habría riesgo de error por formatos diferentes y la función sería más rápida y directa.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 3
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 4 - AGENTES ESPECIALIZADOS
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Hasta acá cada agente hacía todo: leía el archivo, filtraba, calculaba y
# redactaba. Ahora vamos a repartir el trabajo en tres agentes, cada uno con
# una única responsabilidad:
#
#   AgenteDatos      sabe DÓNDE y CÓMO están guardados los datos.
#   AgenteVentas     sabe ANALIZAR ventas (totales, variaciones, cobertura).
#   AgenteProductos  sabe DESCRIBIR telas (ficha, línea, composición, tags).
#
#   AgenteVentas ----\
#                     >---- AgenteDatos ---- archivos de datos/
#   AgenteProductos -/

# ¿POR QUÉ?
# Si mañana las ventas pasan de un CSV a una base de datos, queremos cambiar
# UN solo lugar. Si cambia la definición de "variación", queremos cambiar
# OTRO lugar, sin tocar el acceso a los archivos. Eso se logra separando
# responsabilidades.

# CONCEPTOS TRABAJADOS
# - Agente especializado.
# - Separación de responsabilidades.
# - Un agente puede apoyarse en otro para obtener lo que necesita.

# QUÉ ESTÁ RESUELTO
# Los constructores, la lectura de archivos y los métodos auxiliares.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 4.1: los filtros opcionales de AgenteDatos.
# - TODO 4.2 y 4.3: los dos análisis de AgenteVentas.
# - TODO 4.4 y 4.5: las dos consultas de AgenteProductos.
#
# REGLA DE DISEÑO PARA ESTA PARTE
# AgenteVentas y AgenteProductos NO abren archivos ni usan self.ventas ni
# self.maestro de AgenteDatos directamente. Siempre le PIDEN la información
# a AgenteDatos a través de sus métodos.


class AgenteDatos:

    def __init__(self, carpeta):
        # Este agente concentra el acceso a los archivos. Los demás agentes
        # no necesitan conocer nombres de archivo, codificaciones ni formatos
        # físicos: solo conocen los métodos de este agente.
        self.ventas = pd.read_csv(carpeta / "ventas_historicas_ortelana_argentina.csv")

        # Elegimos el maestro JSON como fuente de productos porque cubre las
        # 30 telas e incluye los tags (ver TODO 1.3).
        with open(carpeta / "maestro.json", encoding="utf-8") as archivo:
            self.maestro = json.load(archivo)

    def filas_de_ventas(self, tela_id, anio=None, sucursal_id=None):
        # Devuelve las filas de ventas de una tela.
        # - Si se indica anio, solo las de ese año.
        # - Si se indica sucursal_id, solo las de esa sucursal.
        # - Si alguno queda en None, NO se filtra por ese criterio (es decir,
        #   se incluyen todos los años o todas las sucursales).
        filas = self.ventas[self.ventas["Tela_ID"] == tela_id]

        # TODO 4.1
        # Aplicá los dos filtros opcionales siguiendo el modelo de la línea
        # de arriba. Cada filtro debe aplicarse solo si el valor no es None.
        if anio is not None:
            filas = filas[filas["Año"] == anio]

        if sucursal_id is not None:
            filas = filas[filas["Sucursal_ID"] == sucursal_id]

        return filas

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 4.1
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def ficha_cruda(self, tela_id):
        # Devuelve la ficha del maestro tal como está guardada, o None si la
        # tela no existe. "Cruda" significa sin interpretar: eso le
        # corresponde a AgenteProductos.
        for ficha in self.maestro:
            if ficha["id"] == tela_id:
                return ficha
        return None

    def todas_las_fichas(self):
        return self.maestro


class AgenteVentas:

    def __init__(self, agente_datos):
        # Este agente no sabe de archivos. Recibe a quién pedirle los datos.
        self.agente_datos = agente_datos

    def total_anual(self, tela_id, anio, sucursal_id=None):
        # Devuelve un diccionario con el total vendido de una tela en un año.
        # Con sucursal_id=None informa el total de toda la red.
        #
        # Además del total informa la COBERTURA: cuántas sucursales aportaron
        # datos. Un total de red calculado con 10 sucursales no es comparable
        # sin más con uno calculado con 12, y quien reciba el resultado
        # necesita saberlo.

        # TODO 4.2
        # 1) Pedile las filas a self.agente_datos (no accedas al DataFrame).
        # 2) Calculá el total con sumar_metros().
        # 3) Calculá cuántas sucursales distintas hay en esas filas.
        #    Pista: filas["Sucursal_ID"].nunique()
        filas = self.agente_datos.filas_de_ventas(tela_id, anio=anio, sucursal_id=sucursal_id)
        total = sumar_metros(filas)
        sucursales = filas["Sucursal_ID"].nunique()

        return {
            "tela_id": tela_id,
            "anio": anio,
            "sucursal_id": sucursal_id,
            "hay_datos": len(filas) > 0,
            "total_metros": total,
            "sucursales_con_datos": sucursales,
        }

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 4.2
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def variacion_porcentual(self, tela_id, anio_base, anio_comparado, sucursal_id=None):
        # Devuelve cuánto cambió (en %) el total entre anio_base y
        # anio_comparado. Devuelve None si falta alguno de los dos años.

        # TODO 4.3
        # Reutilizá self.total_anual() para los dos años. No repitas el
        # filtrado ni la suma: eso ya lo sabe hacer este mismo agente.
        # Redondeá el resultado a 2 decimales.
        base = self.total_anual(tela_id, anio_base, sucursal_id)
        comparado = self.total_anual(tela_id, anio_comparado, sucursal_id)

        if not base["hay_datos"] or not comparado["hay_datos"]:
            return None

        if base["total_metros"] == 0:
            return None

        variacion = (
            (comparado["total_metros"] - base["total_metros"])
            / base["total_metros"]
        ) * 100

        return round(variacion, 2)

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 4.3
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def anios_con_datos(self, tela_id, sucursal_id):
        # Lista ordenada de los años en que una sucursal informó una tela.
        filas = self.agente_datos.filas_de_ventas(tela_id, sucursal_id=sucursal_id)
        return sorted(int(anio) for anio in filas["Año"].unique())

    def resumen_periodo(self, telas, anios, sucursal_id):
        # Este método resuelve el cálculo comercial del integrador para UNA
        # sucursal. Vive en AgenteVentas porque los nodos del grafo no deben
        # calcular ventas ni acceder directamente a los archivos.
        #
        # Recibe las telas encontradas por tag, los años a revisar y la
        # sucursal. Devuelve el total de los registros existentes y, por tela,
        # la lista de años sin registro. Un año faltante no se suma como cero:
        # simplemente se informa aparte.
        total_metros = 0
        anios_faltantes = {}

        for tela_id in telas:
            # Reutilizamos el método del agente para saber qué años tienen
            # registros de esta tela en la sucursal elegida.
            anios_disponibles = self.anios_con_datos(tela_id, sucursal_id)
            anios_faltantes[tela_id] = []

            for anio in anios:
                if anio in anios_disponibles:
                    # Solo sumamos cuando hay una fila; total_anual reutiliza
                    # el cálculo existente y filtra por tela, año y sucursal.
                    total = self.total_anual(tela_id, anio, sucursal_id)
                    total_metros += total["total_metros"]
                else:
                    # Guardamos la ausencia explícitamente para que no se
                    # confunda con un registro cuyo total sea cero.
                    anios_faltantes[tela_id].append(anio)

        return {
            "sucursal_id": sucursal_id,
            "total_metros": round(total_metros, 2),
            "anios_faltantes": anios_faltantes,
        }


class AgenteProductos:

    def __init__(self, agente_datos):
        self.agente_datos = agente_datos

    def describir(self, tela_id):
        # Devuelve una descripción "plana" de la tela, lista para que otro
        # agente la use sin conocer cómo está armado el JSON:
        #
        #   {"tela_id", "nombre", "linea_textil", "composicion",
        #    "origen_fibra", "tipo_macro", "tags"}
        #
        # Devuelve None si la tela no existe en el maestro.

        # TODO 4.4
        # Pedile la ficha cruda a self.agente_datos y armá el diccionario.
        # Atención: en el JSON el identificador se llama "id" y los tags
        # están dentro de ficha["metadatos"]["tags_regionales"] (mirá el
        # ejemplo que imprime la validación de la Parte 1).
        ficha = self.agente_datos.ficha_cruda(tela_id)

        if ficha is None:
            return None

        return {
            "tela_id": ficha["id"],
            "nombre": ficha["nombre"],
            "linea_textil": ficha["linea_textil"],
            "composicion": ficha["composicion"],
            "origen_fibra": ficha["origen_fibra"],
            "tipo_macro": ficha["tipo_macro"],
            "tags": ficha["metadatos"]["tags_regionales"],
        }

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 4.4
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    def buscar_por_tag(self, tag):
        # Devuelve la lista de tela_id cuyas etiquetas incluyen el tag.
        # Si ninguna lo tiene, devuelve una lista vacía.

        # TODO 4.5
        # Recorré todas las fichas (pedíselas a self.agente_datos) y quedate
        # con los identificadores de las que tengan el tag.
        encontradas = []

        for ficha in self.agente_datos.todas_las_fichas():
            tags = ficha["metadatos"]["tags_regionales"]

            if tag in tags:
                encontradas.append(ficha["id"])

        return encontradas

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 4.5
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------


# Instancias que se usan en el resto de la práctica. Hay UN AgenteDatos y los
# otros dos agentes lo comparten.
agente_datos = AgenteDatos(CARPETA_DATOS)
agente_ventas = AgenteVentas(agente_datos)
agente_productos = AgenteProductos(agente_datos)


# ============================================================
# VALIDACIÓN - PARTE 4
# ============================================================
# Qué comprobar:
# - Que el mismo método sirve para una sucursal y para toda la red.
# - Que la cobertura cambia de un año a otro (11 sucursales en 2019, 12 en
#   2020 para TELA-001): el dato existe y el agente lo informa.
# - Que una tela inexistente no rompe el programa.


def validar_parte_4():
    assert len(agente_datos.filas_de_ventas("TELA-001")) == 114, "TODO 4.1: sin filtros"
    assert len(agente_datos.filas_de_ventas("TELA-001", anio=2020)) == 12, "TODO 4.1: filtro por año"
    assert len(agente_datos.filas_de_ventas("TELA-001", sucursal_id=3)) == 9, "TODO 4.1: filtro por sucursal"
    assert len(agente_datos.filas_de_ventas("TELA-001", 2019, 3)) == 0, "TODO 4.1: ambos filtros"

    red_2019 = agente_ventas.total_anual("TELA-001", 2019)
    red_2020 = agente_ventas.total_anual("TELA-001", 2020)
    central_2020 = agente_ventas.total_anual("TELA-001", 2020, sucursal_id=1)
    rosario_2019 = agente_ventas.total_anual("TELA-001", 2019, sucursal_id=3)
    print("Red 2019:", red_2019)
    print("Red 2020:", red_2020)
    print("Central 2020:", central_2020)
    print("Rosario 2019:", rosario_2019)

    assert red_2020["total_metros"] == 14728.29, "TODO 4.2: total de red"
    assert red_2019["sucursales_con_datos"] == 11, "TODO 4.2: cobertura 2019"
    assert red_2020["sucursales_con_datos"] == 12, "TODO 4.2: cobertura 2020"
    assert central_2020["total_metros"] == 2733.99, "TODO 4.2: total de una sucursal"
    assert rosario_2019["hay_datos"] is False, "TODO 4.2: combinación inexistente"

    variacion = agente_ventas.variacion_porcentual("TELA-001", 2019, 2020, sucursal_id=1)
    print("Variación 2019-2020 en Central:", variacion)
    assert abs(variacion - (-76.12)) < 0.01, "TODO 4.3: variación"
    assert agente_ventas.variacion_porcentual("TELA-001", 2019, 2020, sucursal_id=3) is None, (
        "TODO 4.3: si falta un año debe devolver None"
    )

    escoces = agente_productos.describir("TELA-027")
    print("Descripción:", escoces)
    assert escoces["nombre"] == "Escocés de Lana Wellington", "TODO 4.4: nombre"
    assert "invierno" in escoces["tags"], "TODO 4.4: tags"
    assert "metadatos" not in escoces, "TODO 4.4: la descripción debe ser plana"
    assert agente_productos.describir("TELA-999") is None, "TODO 4.4: tela inexistente"

    assert agente_productos.buscar_por_tag("jean") == ["TELA-001"], "TODO 4.5: tag jean"
    assert agente_productos.buscar_por_tag("noche") == ["TELA-011", "TELA-030"], "TODO 4.5: tag noche"
    assert agente_productos.buscar_por_tag("no-existe") == [], "TODO 4.5: tag inexistente"


ejecutar_validacion(4, "Agentes especializados", validar_parte_4)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué sabe hacer cada uno de los tres agentes y qué NO sabe hacer.
# - Por qué AgenteVentas recibe un AgenteDatos en lugar de leer el CSV.
# - Para qué sirve informar la cobertura junto con el total.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 4
# ============================================================

# ¿Qué responsabilidad tiene cada agente? Una oración por agente.
# RESPUESTA:
# AgenteDatos: Acceder y filtrar los datos de ventas y productos desde los archivos.
# AgenteVentas: Analizar las ventas, calcular totales y variaciones porcentuales.
# AgenteProductos: Describir las telas y buscar productos por etiquetas.

# ¿Por qué AgenteVentas no debería encargarse también de interpretar el
# catálogo de productos?
# RESPUESTA:
# Porque su responsabilidad es analizar las ventas, no interpretar los datos de productos.
# Mezclar responsabilidades haría que el código sea más complejo y difícil de mantener.

# ¿Qué ventaja concreta tiene separar el acceso a los datos del análisis?
# Si las ventas pasaran a guardarse en otro formato, ¿qué clases habría que
# modificar y cuáles no?
# RESPUESTA:
# La ventaja es que si el formato de los datos cambia, solo se necesita modificar AgenteDatos.
# AgenteVentas y AgenteProductos no tendrían que cambiar, ya que seguirían usando
# los métodos de AgenteDatos para acceder a la información.

# AgenteProductos.describir() devuelve un diccionario plano en lugar de la
# ficha del JSON tal cual. ¿A quién le simplifica el trabajo esa decisión?
# RESPUESTA:
# Le simplifica el trabajo a AgenteVentas y a cualquier otro agente que necesite
# la información de las telas, ya que no tienen que preocuparse por la estructura del JSON y pueden acceder directamente a los datos relevantes.

# El total de red de TELA-001 en 2019 se calculó con 11 sucursales y el de
# 2020 con 12. ¿Qué cuidado hay que tener al comparar esos dos números?
# RESPUESTA:
# Hay que tener en cuenta que el número de sucursales puede afectar el total, 
# por lo que no es directamente comparable sin ajustar por el tamaño de la red.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 4
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 5 - ARQUITECTURA MULTIAGENTE CON LANGCHAIN
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Agregar un Coordinador: el único componente que habla con el usuario.
# Recibe una consulta, decide QUIÉN debe resolverla y le delega el trabajo.
# Lo vamos a armar con LangChain, como en la Unidad 2.
#
#                       Usuario
#                          |
#                     Coordinador
#                /         |          \
#          "ventas"    "producto"   "variacion"      <- roles (ejecutores)
#               |          |            |
#        AgenteVentas  AgenteProductos  AgenteVentas
#                \         |          /
#                     AgenteDatos                    <- acceso a los archivos

# ¿POR QUÉ?
# Con tres agentes sueltos, el usuario tendría que saber a cuál llamar, qué
# método usar y con qué parámetros. El coordinador le quita esa carga y deja
# un único punto de entrada al sistema.

# CONCEPTOS TRABAJADOS
# - Rol: el tipo de trabajo que resuelve cada ejecutor.
# - Coordinador: decide y reparte, no hace el trabajo especializado.
# - Ejecutor: hace el trabajo que le delegan.
# - Delegación y jerarquía.
# - Arquitecturas centralizada, distribuida e híbrida.
# - Coordinación determinística vs coordinación apoyada por LLM.
#
# LANGCHAIN EN ESTA PARTE
# En LangChain, cada pieza de un sistema es un "Runnable": algo que se
# ejecuta siempre de la misma manera, con .invoke(entrada). Usamos solo dos
# cosas, las dos ya vistas en clase:
#
# 1) RunnableLambda(funcion)
#    Convierte una función común de Python en un Runnable.
#
#        ejecutor = RunnableLambda(mi_funcion)
#        resultado = ejecutor.invoke(entrada)     # equivale a mi_funcion(entrada)
#
# 2) El operador |
#    Encadena Runnables EN SERIE: la salida de uno es la entrada del otro.
#
#        cadena = paso_1 | paso_2
#        resultado = cadena.invoke(entrada)
#
# ¿Qué se gana? Que todos los ejecutores se usan IGUAL. El coordinador no
# necesita saber qué método tiene cada agente ni qué parámetros recibe: le
# alcanza con elegir un ejecutor y hacer .invoke(consulta).
#
# SOBRE EL TIPO DE COORDINACIÓN
# Este coordinador es DETERMINÍSTICO: la consulta llega como un diccionario
# con un campo "tipo" y ese campo define a quién se delega. Ante la misma
# consulta siempre hace lo mismo. Un coordinador apoyado por LLM recibiría la
# consulta en lenguaje natural y el modelo elegiría el ejecutor. Acá no
# usamos un LLM: así la práctica se ejecuta sin claves ni servicios externos.
# La estructura de coordinador y ejecutores es la misma en los dos casos.
#
# FORMA DE LAS CONSULTAS
#   {"tipo": "ventas",    "tela_id": ..., "anio": ..., "sucursal_id": ...}
#   {"tipo": "producto",  "tela_id": ...}
#   {"tipo": "variacion", "tela_id": ..., "anio_base": ..., "anio_comparado": ...,
#    "sucursal_id": ...}
# "sucursal_id" es opcional: consulta.get("sucursal_id") devuelve None si
# la clave no está.

# QUÉ ESTÁ RESUELTO
# El ejecutor de ventas completo (es el modelo a seguir), la conversión de
# los tres ejecutores a Runnables, la estructura del coordinador y la cadena
# final.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 5.1 y 5.2: el trabajo de los otros dos ejecutores.
# - TODO 5.3: qué hace el coordinador con una consulta que nadie resuelve.
# - TODO 5.4: la delegación.
# - TODO 5.5: clasificar la arquitectura.


# ------------------------------------------------------------
# Paso 1: los ejecutores
# ------------------------------------------------------------
# Cada ejecutor es una función que recibe la consulta (un diccionario), saca
# de ella lo que necesita y le pide el trabajo al agente especializado.
# Después la convertimos en Runnable.

def ejecutar_consulta_ventas(consulta):
    # Rol "ventas": total anual de una tela. Ejemplo resuelto.
    return agente_ventas.total_anual(
        consulta["tela_id"],
        consulta["anio"],
        consulta.get("sucursal_id"),
    )


def ejecutar_consulta_producto(consulta):
    # Rol "producto": descripción de una tela.

    # TODO 5.1
    # Siguiendo el modelo de ejecutar_consulta_ventas, devolvé lo que
    # responda el agente especializado que corresponde a este rol.
    return agente_productos.describir(consulta["tela_id"])


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 5.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def ejecutar_consulta_variacion(consulta):
    # Rol "variacion": cambio porcentual entre dos años.

    # TODO 5.2
    # Decidí qué agente y qué método corresponden a este rol y pasales los
    # datos de la consulta. Recordá que "sucursal_id" es opcional.
    return agente_ventas.variacion_porcentual(
        consulta["tela_id"],
        consulta["anio_base"],
        consulta["anio_comparado"],
        consulta.get("sucursal_id")
    )


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 5.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# Conversión a Runnables de LangChain. A partir de acá los tres ejecutores
# se usan de la misma forma: ejecutor.invoke(consulta).
ejecutor_ventas = RunnableLambda(ejecutar_consulta_ventas)
ejecutor_producto = RunnableLambda(ejecutar_consulta_producto)
ejecutor_variacion = RunnableLambda(ejecutar_consulta_variacion)

# Tabla de roles: qué ejecutor atiende cada tipo de consulta. Esta tabla es
# todo lo que el coordinador necesita conocer de sus ejecutores.
EJECUTORES = {
    "ventas": ejecutor_ventas,
    "producto": ejecutor_producto,
    "variacion": ejecutor_variacion,
}


# ------------------------------------------------------------
# Paso 2: el coordinador
# ------------------------------------------------------------

class Coordinador:

    def __init__(self, ejecutores):
        # El coordinador conoce a sus ejecutores por el ROL, no conoce a los
        # agentes especializados ni a los archivos.
        self.ejecutores = ejecutores

        # Bitácora: registro de los roles a los que se delegó, en orden.
        # Sirve para poder explicar después cómo se resolvió algo.
        self.bitacora = []

    def atender(self, consulta):
        tipo = consulta.get("tipo")

        # TODO 5.3
        # Si el tipo no figura en self.ejecutores, el coordinador no debe
        # delegar en nadie ni registrar nada en la bitácora. Devolvé (con
        # return) un diccionario con la clave "error" y un mensaje que le
        # sirva al usuario para corregir la consulta.
        if tipo not in self.ejecutores:
            return {"error": f"Tipo de consulta no válido: {tipo}"}

        # TODO 5.4
        # Delegación. Tres pasos:
        # 1) registrá el tipo en la bitácora;
        # 2) elegí el ejecutor que corresponde a ese tipo en self.ejecutores;
        # 3) devolvé lo que responda al hacerle .invoke(consulta).
        # Notá que NO hace falta un if por cada tipo de consulta.
        self.bitacora.append(tipo)
        ejecutor = self.ejecutores[tipo]
        return ejecutor.invoke(consulta)

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 5.3
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------

    # ------------------------------------------------------------
    # FEEDBACK DOCENTE - TODO 5.4
    # Estado: [ ] Correcto  [ ] Revisar
    # Comentario:
    #
    # ------------------------------------------------------------


# ------------------------------------------------------------
# Paso 3: el sistema completo como una cadena en serie
# ------------------------------------------------------------
# Antes de llegar al coordinador conviene emprolijar la consulta: el usuario
# puede escribir " Ventas " en lugar de "ventas". Ese paso previo y el
# coordinador se encadenan con el operador | de LangChain.

def normalizar_consulta(consulta):
    limpia = dict(consulta)
    limpia["tipo"] = str(consulta.get("tipo", "")).strip().lower()
    return limpia


def construir_sistema(coordinador):
    # normalizar  ->  coordinador (que delega en un ejecutor)
    return RunnableLambda(normalizar_consulta) | RunnableLambda(coordinador.atender)


coordinador = Coordinador(EJECUTORES)
sistema = construir_sistema(coordinador)

# TODO 5.5
# Clasificá la arquitectura que quedó construida. Escribí exactamente una de
# estas palabras: "centralizada", "distribuida" o "hibrida".
# La justificación va en la reflexión de esta parte.
ARQUITECTURA_CONSTRUIDA = "centralizada"  

# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 5.5
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 5
# ============================================================
# Qué comprobar:
# - Que el sistema devuelve lo mismo que devolvería el agente especializado
#   si lo llamaras directamente.
# - Que la bitácora refleja a qué rol se delegó cada consulta, en orden, y
#   que la consulta desconocida no aparece.
# - Que la consulta escrita como " VENTAS " también se resuelve: la corrige
#   el primer paso de la cadena antes de llegar al coordinador.


def validar_parte_5():
    prueba = Coordinador(EJECUTORES)
    sistema_prueba = construir_sistema(prueba)

    r_ventas = sistema_prueba.invoke({"tipo": "ventas", "tela_id": "TELA-009", "anio": 2020})
    r_producto = sistema_prueba.invoke({"tipo": "producto", "tela_id": "TELA-009"})
    r_variacion = sistema_prueba.invoke(
        {"tipo": "variacion", "tela_id": "TELA-009", "anio_base": 2019, "anio_comparado": 2020, "sucursal_id": 1}
    )
    r_error = sistema_prueba.invoke({"tipo": "stock", "tela_id": "TELA-009"})
    r_mayusculas = sistema_prueba.invoke({"tipo": " VENTAS ", "tela_id": "TELA-009", "anio": 2020})

    print("Ventas:", r_ventas)
    print("Producto:", r_producto)
    print("Variación:", r_variacion)
    print("Consulta desconocida:", r_error)
    print("Bitácora:", prueba.bitacora)

    assert r_ventas["total_metros"] == 65186.3, "TODO 5.4: delegación de ventas"
    assert r_producto["nombre"] == "Tusor de Algodón Liso", "TODO 5.1"
    assert abs(r_variacion - 28.41) < 0.02, "TODO 5.2"
    assert "error" in r_error, "TODO 5.3"
    assert r_mayusculas == r_ventas, "la cadena debe normalizar el tipo antes de coordinar"
    assert prueba.bitacora == ["ventas", "producto", "variacion", "ventas"], (
        "TODO 5.3 / 5.4: la bitácora registra solo las delegaciones realizadas, en orden"
    )
    assert ARQUITECTURA_CONSTRUIDA in ["centralizada", "distribuida", "hibrida"], "TODO 5.5"


ejecutar_validacion(5, "Arquitectura multiagente con LangChain", validar_parte_5)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué decide el coordinador y qué deja en manos de los ejecutores.
# - Cómo se ve la jerarquía en el código (quién conoce a quién).
# - Para qué sirvió convertir los ejecutores en Runnables de LangChain.
# - Qué cambiaría y qué no si el coordinador usara un LLM para elegir.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 5
# ============================================================

# Justificá tu respuesta al TODO 5.5. ¿Quién toma las decisiones de reparto?
# ¿Los ejecutores se hablan entre sí? ¿Qué tendría que cambiar en el código
# para que fuera de otro de los tres tipos?
# RESPUESTA:
# La arquitectura es centralizada, porque el Coordinador recibe las
# consultas y decide a qué ejecutor enviarlas según el rol solicitado.
# Los ejecutores no se comunican directamente entre sí.
# Para tener una arquitectura distribuida, los agentes deberían poder
# comunicarse y coordinar tareas sin depender de un coordinador central.

# Los tres ejecutores hacen trabajos distintos pero el coordinador los usa a
# todos con .invoke(consulta). ¿Qué le simplifica eso al coordinador? ¿Qué
# habría que tocar para agregar un cuarto rol?
# RESPUESTA:
# El método .invoke() permite que el Coordinador utilice todos los
# ejecutores de la misma manera, sin conocer su funcionamiento interno.
# Para agregar un cuarto rol, habría que crear su ejecutor y
# registrarlo en el diccionario de ejecutores del Coordinador.

# El rol "ventas" y el rol "variacion" terminan en el mismo agente
# especializado. ¿Qué diferencia hay entonces entre un rol y un agente?
# RESPUESTA:
# El rol representa una tarea específica, como consultar ventas o
# calcular variaciones. El agente es quien realiza esa tarea.
# Por eso, un mismo agente especializado puede cumplir varios roles.

# El Coordinador no conoce a AgenteDatos. ¿Es un descuido o una decisión de
# diseño? ¿Qué se gana con que no lo conozca?
# RESPUESTA:
# Es una decisión de diseño. El Coordinador solo se encarga de repartir
# las consultas, mientras que los agentes especializados acceden a los
# datos cuando los necesitan. Esto reduce el acoplamiento y permite
# modificar el acceso a los datos sin afectar al Coordinador.

# Si el Coordinador calculara él mismo la variación porcentual en lugar de
# delegarla, el programa daría el mismo resultado. ¿Qué se perdería?
# RESPUESTA:
# Se perdería la separación de responsabilidades, porque el Coordinador
# dejaría de encargarse solamente de distribuir tareas y también
# realizaría cálculos. Esto dificultaría el mantenimiento y la
# reutilización de la lógica de análisis.

# ¿Qué pasa con todo el sistema si el Coordinador falla? ¿Con qué tipo de
# arquitectura se relaciona esa debilidad?
# RESPUESTA:
# Si el Coordinador falla, las consultas dejan de distribuirse y el
# sistema no puede responder a través de ese flujo, aunque los agentes
# especializados sigan funcionando. Es un punto único de falla,
# característico de esta arquitectura centralizada.

# En construir_sistema() hay dos pasos encadenados con |. ¿Por qué tienen
# que ir en serie y en ese orden?
# RESPUESTA:
# Los pasos deben ejecutarse en serie porque primero se normaliza la
# consulta y después se envía al Coordinador.
# El orden importa: si se delegara antes de normalizar, podría no
# reconocerse correctamente el rol solicitado.

# Imaginá que la consulta llega como texto libre. ¿Qué parte del sistema
# reemplazarías por un LLM y qué parte dejarías igual?
# RESPUESTA:
# Reemplazaría la etapa de interpretación o normalización de la consulta
# por un LLM que identifique la intención y genere una consulta
# estructurada. Mantendría los ejecutores y agentes especializados,
# porque sus responsabilidades y cálculos seguirían siendo los mismos.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 5
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 6 - COMUNICACIÓN ENTRE AGENTES
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Resolver una consulta que NINGÚN agente puede responder solo:
#
#   "Analizá cómo le fue a una tela en un año y explicá además qué tipo
#    de producto es."
#
# AgenteVentas sabe la parte comercial. AgenteProductos sabe qué es la tela.
# Alguien tiene que juntar las dos cosas.

# ¿POR QUÉ?
# En la Parte 5 cada consulta iba a un único ejecutor. Las consultas reales
# suelen necesitar a varios, y entonces aparece el problema central de la
# Unidad 3: ¿cómo se pasan información los agentes?

# CONCEPTOS TRABAJADOS
# - Comunicación entre agentes.
# - Contexto compartido: un diccionario que viaja de un paso al siguiente.
#   Cada agente LEE lo que necesita y ESCRIBE solo su aporte.
# - Colaboración: el resultado final no pertenece a ningún agente solo.
#
# EL CONTEXTO COMPARTIDO DE ESTA PARTE
#
#   contexto = {
#       "tela_id":       dato de entrada (lo escribe el coordinador)
#       "anio":          dato de entrada (lo escribe el coordinador)
#       "ventas":        lo escribe AgenteVentas
#       "variacion_pct": lo escribe AgenteVentas
#       "producto":      lo escribe AgenteProductos
#       "informe":       lo escribe el coordinador al combinar
#       "mensajes":      lista donde cada participante anota qué aportó
#   }
#
# Es "compartido" porque todos trabajan sobre el mismo diccionario en lugar
# de pasarse valores sueltos. Así, agregar un cuarto agente no obliga a
# cambiar la firma de ninguna función: solo agrega una clave.

# QUÉ ESTÁ RESUELTO
# La creación del contexto y la función que ordena los pasos.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 6.1: el aporte de ventas.
# - TODO 6.2: el aporte de producto.
# - TODO 6.3: la combinación final.
# En cada paso agregá a contexto["mensajes"] un texto breve que empiece con
# el nombre de quien habla, por ejemplo: "AgenteVentas: total 2020 = ...".


def crear_contexto(tela_id, anio):
    return {
        "tela_id": tela_id,
        "anio": anio,
        "ventas": None,
        "variacion_pct": None,
        "producto": None,
        "informe": None,
        "mensajes": [],
    }


def aportar_ventas(contexto):
    # Paso a cargo de AgenteVentas.
    # Lee:    tela_id, anio
    # Escribe: ventas (resultado de total_anual para toda la red)
    #          variacion_pct (respecto del año anterior, toda la red)

    # TODO 6.1
    tela_id = contexto["tela_id"]
    anio = contexto["anio"]

    contexto["ventas"] = agente_ventas.total_anual(tela_id, anio)
    contexto["variacion_pct"] = agente_ventas.variacion_porcentual(tela_id, anio - 1, anio)
    contexto["mensajes"].append(
    f"AgenteVentas: analizadas las ventas de {tela_id} en {anio}.")

    return contexto


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 6.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def aportar_producto(contexto):
    # Paso a cargo de AgenteProductos.
    # Lee:    tela_id
    # Escribe: producto (resultado de describir; puede ser None)

    # TODO 6.2
    contexto["producto"] = agente_productos.describir(contexto["tela_id"])
    if contexto["producto"] is not None:
        contexto["mensajes"].append(
            f"AgenteProductos: descripción de {contexto['tela_id']} obtenida.")
    else:
        contexto["mensajes"].append(
            f"AgenteProductos: {contexto['tela_id']} no se encuentra en el catálogo.")

    return contexto


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 6.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def combinar_resultados(contexto):
    # Paso a cargo del coordinador.
    # Lee:    ventas, variacion_pct, producto
    # Escribe: informe (un texto para la gerencia)

    # TODO 6.3
    # Redactá el informe usando lo que dejaron los dos ejecutores. Tiene que
    # mencionar el nombre de la tela, su línea textil, el total vendido y la
    # variación. Contemplá dos situaciones reales:
    # - el producto puede ser None (tela que no está en el maestro);
    # - puede no haber ventas (ventas["hay_datos"] es False).
    # En esos casos el informe debe decirlo en lugar de fallar.
    producto = contexto["producto"]
    ventas = contexto["ventas"]
    variacion = contexto["variacion_pct"]

    if producto is None:
        nombre = contexto["tela_id"]
        linea = "desconocida"
    else:
        nombre = producto["nombre"]
        linea = producto["linea_textil"]

    if not ventas["hay_datos"]:
        informe = f"No hay datos de ventas para {nombre} en {contexto['anio']}."
    else:
        informe = (
            f"{nombre}, línea {linea}: "
            f"{ventas['total_metros']} metros vendidos en {contexto['anio']}. "
            f"Variación respecto del año anterior: {variacion}%."
            if variacion is not None
            else (
                f"{nombre}, línea {linea}: "
                f"{ventas['total_metros']} metros vendidos en {contexto['anio']}. "
                "No se puede calcular la variación."
            )
        )

    contexto["informe"] = informe
    contexto["mensajes"].append("Coordinador: informe final generado.")
    return contexto


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 6.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def analisis_completo(tela_id, anio):
    # El coordinador crea el contexto y lo hace pasar por cada participante.
    contexto = crear_contexto(tela_id, anio)
    contexto = aportar_ventas(contexto)
    contexto = aportar_producto(contexto)
    contexto = combinar_resultados(contexto)
    return contexto


# ============================================================
# VALIDACIÓN - PARTE 6
# ============================================================
# Qué comprobar: leé la lista de mensajes. Debería poder reconstruirse quién
# aportó qué. Verificá también que una tela inexistente produce un informe
# que lo explica.


def validar_parte_6():
    contexto = analisis_completo("TELA-001", 2020)
    print("Informe:", contexto["informe"])
    for mensaje in contexto["mensajes"]:
        print("  >", mensaje)

    assert contexto["ventas"]["total_metros"] == 14728.29, "TODO 6.1: ventas"
    assert abs(contexto["variacion_pct"] - (-74.24)) < 0.01, "TODO 6.1: variación"
    assert contexto["producto"]["nombre"] == "Denim Pesado 12oz", "TODO 6.2: producto"
    assert "Denim Pesado 12oz" in contexto["informe"], "TODO 6.3: el informe debe nombrar la tela"
    assert "indumentaria" in contexto["informe"], "TODO 6.3: el informe debe mencionar la línea"
    assert len(contexto["mensajes"]) >= 3, "cada participante debe dejar un mensaje"

    inexistente = analisis_completo("TELA-999", 2020)
    print("Informe de tela inexistente:", inexistente["informe"])
    assert inexistente["producto"] is None, "TODO 6.2: tela inexistente"
    assert isinstance(inexistente["informe"], str), "TODO 6.3: debe haber informe aunque falte el producto"


ejecutar_validacion(6, "Comunicación entre agentes", validar_parte_6)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué claves del contexto lee y cuáles escribe cada participante.
# - Por qué el informe no podría haberlo escrito ninguno de los dos
#   ejecutores por su cuenta.
# - Qué ventaja tiene un diccionario compartido frente a pasar parámetros
#   sueltos de función en función.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 6
# ============================================================

# Completá esta tabla con lo que implementaste:
#   Participante      | Lee del contexto | Escribe en el contexto
#   AgenteVentas      |                  |
#   AgenteProductos   |                  |
#   Coordinador       |                  |
# RESPUESTA:

# Participante    | Lee del contexto              | Escribe en el contexto
# AgenteVentas    | tela_id, anio                  | ventas, variacion_pct, mensajes
# AgenteProductos | tela_id                        | producto, mensajes
# Coordinador     | producto, ventas, variacion_pct| informe, mensajes

# AgenteVentas y AgenteProductos nunca se llaman entre sí. ¿Cómo llega
# entonces la información de uno a formar parte del mismo resultado que la
# del otro?
# RESPUESTA:
# La información se comparte mediante el diccionario contexto.
# Cada agente recibe ese mismo contexto y agrega los resultados de su
# trabajo. Finalmente, el Coordinador lee esos resultados y construye
# un informe que combina la información de ventas y productos.

# ¿Qué problema aparecería si aportar_producto() borrara o modificara la
# clave "ventas"? ¿Qué regla de convivencia sobre el contexto compartido se
# desprende de eso?
# RESPUESTA:
# Podría sobrescribir o eliminar los resultados de AgenteVentas,
# provocando información incorrecta o errores al generar el informe.
# Por eso, cada participante debe modificar solamente las claves que
# tiene asignadas y respetar los datos escritos por los demás.

# En analisis_completo(), ¿se podría llamar a aportar_producto() antes que a
# aportar_ventas()? ¿Y a combinar_resultados() antes que a las otras dos?
# Justificá cada respuesta.
# RESPUESTA:
# Sí, aportar_producto() puede ejecutarse antes que aportar_ventas(),
# porque ambos trabajan de manera independiente y utilizan los datos
# iniciales del contexto.
#
# En cambio, combinar_resultados() no debería ejecutarse antes,
# porque necesita que los otros dos agentes hayan completado sus
# aportes para poder construir el informe final correctamente.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 6
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 7 - SECUENCIALIDAD Y DEPENDENCIAS
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Resolver una consulta donde el usuario NO conoce el código de la tela:
#
#   "¿Cómo le fue en 2020 a la tela que usamos para jean?"
#
# Hay que hacer tres tareas y cada una necesita el resultado de la anterior:
#
#   identificar el producto  ->  analizar sus ventas  ->  redactar conclusión
#   (a partir del tag)           (necesita el tela_id)    (necesita todo)
#
# Lo vamos a armar como un grafo de LangGraph con tres nodos en serie.

# ¿POR QUÉ?
# No se puede analizar las ventas de una tela que todavía no se sabe cuál
# es. Eso es una DEPENDENCIA: una tarea necesita un dato que produce otra.
# Las dependencias son las que determinan el orden.

# CONCEPTOS TRABAJADOS
# - Dependencias entre tareas.
# - Ejecución secuencial.
# - LangGraph: estado, nodos y aristas.
#
# REPASO MÍNIMO DE LANGGRAPH
# - ESTADO: un diccionario con claves declaradas (TypedDict). Es el contexto
#   compartido de la Parte 6, ahora administrado por el grafo.
# - NODO: una función que recibe el estado y DEVUELVE un diccionario solo con
#   las claves que quiere actualizar. LangGraph se encarga de incorporarlas.
# - ARISTA (edge): indica qué nodo se ejecuta después de cuál.
# - START y END marcan el comienzo y el final.

# QUÉ ESTÁ RESUELTO
# El estado, el primer nodo y el registro de los nodos en el grafo.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 7.1: el nodo que analiza las ventas.
# - TODO 7.2: el nodo que redacta la conclusión.
# - TODO 7.3: las aristas, en el orden que imponen las dependencias.


class EstadoSecuencial(TypedDict):
    tag: str          # entrada: lo que sabe el usuario
    anio: int         # entrada
    tela_id: str      # lo escribe el nodo 1
    producto: dict    # lo escribe el nodo 1
    ventas: dict      # lo escribe el nodo 2
    conclusion: str   # lo escribe el nodo 3


def nodo_identificar_producto(estado):
    # No depende de ningún otro nodo: solo necesita el tag de entrada.
    encontrados = agente_productos.buscar_por_tag(estado["tag"])

    # Por ahora asumimos que el tag existe y tomamos la primera coincidencia.
    # Qué hacer cuando no hay coincidencias es el tema de la Parte 9.
    tela_id = encontrados[0]

    return {
        "tela_id": tela_id,
        "producto": agente_productos.describir(tela_id),
    }


def nodo_analizar_ventas(estado):
    # TODO 7.1
    # Este nodo DEPENDE del anterior. Identificá qué clave del estado
    # necesita, pedile a agente_ventas el total anual de toda la red y
    # devolvé un diccionario que actualice la clave "ventas".
    # El nodo anterior dejó la tela_id en el estado. Al no pasar sucursal_id,
    # agente_ventas calcula el total de toda la red.
    ventas = agente_ventas.total_anual(estado["tela_id"], estado["anio"])
    return {"ventas": ventas}


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 7.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def nodo_redactar_conclusion(estado):
    # TODO 7.2
    # Este nodo depende de los dos anteriores. Devolvé un diccionario que
    # actualice la clave "conclusion" con un texto que mencione el tag
    # consultado, el nombre de la tela y los metros vendidos.
    producto = estado["producto"]
    ventas = estado["ventas"]

    if ventas["hay_datos"]:
        conclusion = (
            f"Para el tag '{estado['tag']}', la tela {producto['nombre']} "
            f"vendió {ventas['total_metros']} metros en {estado['anio']}."
        )
    else:
        conclusion = (
            f"Para el tag '{estado['tag']}', la tela {producto['nombre']} "
            f"no tiene datos de ventas registrados para {estado['anio']}; "
            "no se puede informar un total."
        )

    return {"conclusion": conclusion}


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 7.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def construir_grafo_secuencial():
    grafo = StateGraph(EstadoSecuencial)

    grafo.add_node("identificar", nodo_identificar_producto)
    grafo.add_node("analizar", nodo_analizar_ventas)
    grafo.add_node("concluir", nodo_redactar_conclusion)

    # TODO 7.3
    # Conectá los nodos con grafo.add_edge(origen, destino), empezando en
    # START y terminando en END. El orden no es arbitrario: lo definen las
    # dependencias que identificaste en los TODO 7.1 y 7.2.
    grafo.add_edge(START, "identificar")
    grafo.add_edge("identificar", "analizar")
    grafo.add_edge("analizar", "concluir")
    grafo.add_edge("concluir", END)

    return grafo.compile()


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 7.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 7
# ============================================================
# Qué comprobar: el estado final contiene lo que escribió cada nodo. Después
# de que funcione, probá a propósito conectar "analizar" antes que
# "identificar", ejecutá, leé el error y volvé a dejarlo bien. Ese error es
# la dependencia hecha visible.


def validar_parte_7():
    grafo = construir_grafo_secuencial()
    final = grafo.invoke({"tag": "jean", "anio": 2020})

    print("Tela identificada:", final["tela_id"])
    print("Ventas:", final["ventas"])
    print("Conclusión:", final["conclusion"])

    assert final["tela_id"] == "TELA-001", "nodo identificar"
    assert final["ventas"]["total_metros"] == 14728.29, "TODO 7.1"
    assert "Denim Pesado 12oz" in final["conclusion"], "TODO 7.2: la conclusión debe nombrar la tela"
    assert "jean" in final["conclusion"], "TODO 7.2: la conclusión debe mencionar el tag"


ejecutar_validacion(7, "Secuencialidad y dependencias", validar_parte_7)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué dato produce cada nodo y qué nodo lo consume.
# - Por qué este flujo no puede ejecutarse en paralelo.
# - En qué se parece el estado de LangGraph al contexto de la Parte 6.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 7
# ============================================================

# Dibujá el flujo con flechas e indicá sobre cada flecha qué dato "viaja"
# de un nodo al siguiente.
# RESPUESTA:
# START → identificar: recibe tag y anio; agrega tela_id y producto.
# identificar → analizar: queda disponible tela_id; analizar también usa anio.
# analizar → concluir: agrega ventas al estado; concluir también usa producto
# y tag para redactar el texto.
# concluir → END: queda el estado final con la conclusion.

# ¿Qué error apareció cuando invertiste el orden de dos nodos? Explicá con
# tus palabras por qué ocurre.
# RESPUESTA:
# Apareció KeyError: 'tela_id'. El nodo analizar necesita esa clave, pero
# todavía no existe en el estado inicial: la agrega identificar. Por eso
# identificar debe ejecutarse antes que analizar.

# Los nodos devuelven solo las claves que modifican, no el estado completo.
# ¿Qué ventaja tiene eso respecto de la Parte 6, donde cada función recibía
# y devolvía el contexto entero?
# RESPUESTA:
# En la Parte 7 cada nodo devuelve solo las claves que modifica, por ejemplo
# analizar devuelve ventas. LangGraph las agrega al estado compartido. Esto
# permite ver claramente qué aporta cada nodo y evita devolver el contexto
# entero, como hacíamos en la Parte 6.

# Dá un ejemplo de dos tareas sobre los datos de Ortelana que NO dependan
# una de otra. (Lo vas a necesitar en la Parte 8.)
# RESPUESTA:
# Se pueden analizar en paralelo las ventas de dos telas distintas para el
# mismo año, si ya tenemos los códigos de ambas. El resultado de una tela no
# hace falta para calcular el de la otra.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 7
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 8 - PARALELISMO Y SINCRONIZACIÓN
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Comparar cómo evolucionaron DOS telas entre 2019 y 2020:
#
#   TELA-001  Denim Pesado 12oz
#   TELA-009  Tusor de Algodón Liso
#
# Analizar una no requiere ningún dato de la otra: son tareas INDEPENDIENTES
# y pueden ejecutarse en paralelo. Comparar, en cambio, necesita los DOS
# análisis terminados: es el punto de SINCRONIZACIÓN.
#
#                      START
#                        |
#                    preparar
#                    /      \
#           analizar_a      analizar_b       <- ramas en paralelo
#                    \      /
#                    comparar                <- sincronización
#                        |
#                       END

# ¿POR QUÉ?
# El paralelismo ahorra tiempo cuando las tareas no se necesitan entre sí.
# Pero trae un problema nuevo: dos ramas escribiendo en el mismo estado al
# mismo tiempo. Hay que diseñar el estado para que no se pisen.

# CONCEPTOS TRABAJADOS
# - Tareas independientes y ejecución paralela.
# - Ramas de un grafo.
# - Sincronización: un nodo que espera a que terminen varias ramas.
# - Estado compartido con escrituras simultáneas.
#
# LA REGLA PARA QUE LAS RAMAS NO SE PISEN
#
# Cada rama escribe ÚNICAMENTE en claves propias:
#
#     rama A  ->  resultado_a, mensaje_a
#     rama B  ->  resultado_b, mensaje_b
#
# Si las dos ramas intentaran escribir la misma clave al mismo tiempo,
# LangGraph no sabría con cuál valor quedarse y daría error. Lo que es común
# (la comparación y la bitácora completa) lo escribe un solo nodo, el de
# sincronización, DESPUÉS de que las dos ramas terminaron.

# QUÉ ESTÁ RESUELTO
# El estado, la función analizar_tela() que hace el trabajo de cada rama y
# el nodo preparar.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 8.1: los dos nodos de las ramas paralelas.
# - TODO 8.2: el nodo de sincronización.
# - TODO 8.3: el grafo completo (nodos y aristas).


class EstadoComparacion(TypedDict):
    tela_a: str           # entrada
    tela_b: str           # entrada
    anio_base: int        # entrada
    anio_comparado: int   # entrada
    mensaje_inicio: str   # lo escribe preparar
    resultado_a: dict     # lo escribe SOLO la rama A
    mensaje_a: str        # lo escribe SOLO la rama A
    resultado_b: dict     # lo escribe SOLO la rama B
    mensaje_b: str        # lo escribe SOLO la rama B
    comparacion: dict     # lo escribe el nodo de sincronización
    bitacora: list        # lo escribe el nodo de sincronización


def analizar_tela(tela_id, anio_base, anio_comparado):
    # Trabajo que hace cada rama: reúne lo comercial (AgenteVentas) y lo
    # descriptivo (AgenteProductos) de UNA tela. No usa nada de la otra.
    producto = agente_productos.describir(tela_id)
    base = agente_ventas.total_anual(tela_id, anio_base)
    comparado = agente_ventas.total_anual(tela_id, anio_comparado)

    return {
        "tela_id": tela_id,
        "nombre": producto["nombre"],
        "linea_textil": producto["linea_textil"],
        "total_base": base["total_metros"],
        "total_comparado": comparado["total_metros"],
        "sucursales_base": base["sucursales_con_datos"],
        "sucursales_comparado": comparado["sucursales_con_datos"],
        "variacion_pct": agente_ventas.variacion_porcentual(tela_id, anio_base, anio_comparado),
    }


def nodo_preparar(estado):
    # Punto de partida común de las dos ramas. Solo deja constancia de qué
    # se va a comparar.
    return {
        "mensaje_inicio": (
            f"preparar: se comparan {estado['tela_a']} y {estado['tela_b']} "
            f"entre {estado['anio_base']} y {estado['anio_comparado']}"
        )
    }


# TODO 8.1
# Completá los dos nodos de las ramas. Cada uno debe:
# - leer del estado la tela que le corresponde y los dos años;
# - llamar a analizar_tela();
# - devolver su resultado en SU clave (resultado_a o resultado_b);
# - devolver un texto breve en SU clave de mensaje (mensaje_a o mensaje_b).

# Antes de escribir, pensá: ¿qué pasaría si los dos nodos devolvieran la
# misma clave "resultado"?

# RESPUESTA:
# Si las dos ramas escribieran la misma clave "resultado" al mismo tiempo,
# sus valores entrarían en conflicto: LangGraph no sabría cuál conservar y
# produciría un error. Por eso cada rama escribe en claves distintas:
# resultado_a y resultado_b.


def nodo_analizar_a(estado):
    resultado = analizar_tela(
        estado["tela_a"],
        estado["anio_base"],
        estado["anio_comparado"],
    )
    return {
        "resultado_a": resultado,
        "mensaje_a": f"analizar_a: análisis de {estado['tela_a']} terminado.",
    }


def nodo_analizar_b(estado):
    resultado = analizar_tela(
        estado["tela_b"],
        estado["anio_base"],
        estado["anio_comparado"],
    )
    return {
        "resultado_b": resultado,
        "mensaje_b": f"analizar_b: análisis de {estado['tela_b']} terminado.",
    }


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 8.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def nodo_comparar(estado):
    # SINCRONIZACIÓN: este nodo se ejecuta cuando las dos ramas terminaron,
    # por eso puede leer resultado_a y resultado_b a la vez.

    # TODO 8.2
    # Devolvé DOS claves. La primera, "comparacion", con un diccionario que tenga:
    #   "mejor_evolucion":   el tela_id con mayor variacion_pct
    #   "diferencia_puntos": variación de A menos variación de B, en valor
    #                        absoluto (pista: abs()), redondeada a 2 decimales
    #   "conclusion":        un texto que nombre las dos telas, sus líneas
    #                        textiles y sus variaciones
    # La segunda, "bitacora": una lista con cuatro textos, en este orden:
    # mensaje_inicio, mensaje_a, mensaje_b y un mensaje propio de este nodo.
    # Recién acá se juntan los mensajes de las dos ramas.
    resultado_a = estado["resultado_a"]
    resultado_b = estado["resultado_b"]
    variacion_a = resultado_a["variacion_pct"]
    variacion_b = resultado_b["variacion_pct"]

    mejor_evolucion = (
        resultado_a["tela_id"]
        if variacion_a > variacion_b
        else resultado_b["tela_id"]
    )
    diferencia_puntos = round(abs(variacion_a - variacion_b), 2)

    conclusion = (
        f"{resultado_a['nombre']} (línea {resultado_a['linea_textil']}) "
        f"tuvo una variación de {variacion_a}%; "
        f"{resultado_b['nombre']} (línea {resultado_b['linea_textil']}) "
        f"tuvo una variación de {variacion_b}%."
    )

    return {
        "comparacion": {
            "mejor_evolucion": mejor_evolucion,
            "diferencia_puntos": diferencia_puntos,
            "conclusion": conclusion,
        },
        "bitacora": [
            estado["mensaje_inicio"],
            estado["mensaje_a"],
            estado["mensaje_b"],
            f"comparar: diferencia de variación = {diferencia_puntos} puntos.",
        ],
    }


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 8.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def construir_grafo_paralelo():
    grafo = StateGraph(EstadoComparacion)

    # TODO 8.3
    # Armá el grafo del dibujo de arriba:
    # 1) Registrá los cuatro nodos con add_node. Usá estos nombres:
    #    "preparar", "analizar_a", "analizar_b", "comparar".
    # 2) Conectá START con "preparar".
    # 3) Abrí las dos ramas: desde "preparar" salen DOS aristas.
    # 4) Cerrá las ramas en "comparar". Para indicar que debe esperar a las
    #    dos, pasá una lista como origen:
    #        grafo.add_edge(["analizar_a", "analizar_b"], "comparar")
    # 5) Conectá "comparar" con END.
    grafo.add_node("preparar", nodo_preparar)
    grafo.add_node("analizar_a", nodo_analizar_a)
    grafo.add_node("analizar_b", nodo_analizar_b)
    grafo.add_node("comparar", nodo_comparar)

    grafo.add_edge(START, "preparar")
    grafo.add_edge("preparar", "analizar_a")
    grafo.add_edge("preparar", "analizar_b")
    grafo.add_edge(["analizar_a", "analizar_b"], "comparar")
    grafo.add_edge("comparar", END)

    return grafo.compile()


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 8.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 8
# ============================================================
# Qué comprobar:
# - En la bitácora aparecen las dos ramas y recién después la comparación.
# - Las dos telas evolucionaron de manera muy distinta. Mirá los números
#   antes de responder la reflexión.
# - Mirá sucursales_base y sucursales_comparado: la cobertura no es la misma
#   en los dos años.


def validar_parte_8():
    grafo = construir_grafo_paralelo()
    final = grafo.invoke(
        {"tela_a": "TELA-001", "tela_b": "TELA-009", "anio_base": 2019, "anio_comparado": 2020}
    )

    print("Rama A:", final["resultado_a"])
    print("Rama B:", final["resultado_b"])
    print("Comparación:", final["comparacion"])
    for linea in final["bitacora"]:
        print("  >", linea)

    assert final["resultado_a"]["tela_id"] == "TELA-001", "TODO 8.1: rama A"
    assert final["resultado_b"]["tela_id"] == "TELA-009", "TODO 8.1: rama B"
    assert abs(final["resultado_a"]["variacion_pct"] - (-74.24)) < 0.01, "TODO 8.1: rama A"
    assert abs(final["resultado_b"]["variacion_pct"] - 43.68) < 0.02, "TODO 8.1: rama B"
    assert final["comparacion"]["mejor_evolucion"] == "TELA-009", "TODO 8.2: mejor evolución"
    assert abs(final["comparacion"]["diferencia_puntos"] - 117.92) < 0.05, "TODO 8.2: diferencia"
    assert len(final["bitacora"]) == 4, "TODO 8.2: la bitácora debe reunir los cuatro mensajes"
    assert final["bitacora"][0].startswith("preparar"), "el primer mensaje es de preparar"


ejecutar_validacion(8, "Paralelismo y sincronización", validar_parte_8)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Por qué las dos ramas pueden ir en paralelo y la comparación no.
# - Cómo se evita que dos ramas se pisen al escribir el estado.
# - Qué significa "sincronizar" en este grafo y qué nodo lo hace.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 8
# ============================================================

# ¿Qué condición cumplen analizar_a y analizar_b que permite ejecutarlas en
# paralelo? Comparalo con los nodos de la Parte 7.
# RESPUESTA: Las dos ramas trabajan con telas distintas y cada análisis puede
# hacerse sin conocer el resultado de la otra. Ambas reciben de antemano los
# códigos de tela y los años. En la Parte 7, en cambio, analizar depende de
# que identificador encuentre primero el tela_id.

# ¿Por qué cada rama escribe en una clave distinta? ¿Qué pasaría si ambas
# devolvieran {"resultado": ...}?
# RESPUESTA: Cada rama escribe en una clave distinta para conservar su propio
# resultado: resultado_a o resultado_b. Si ambas escribieran "resultado" al
# mismo tiempo, sus escrituras entrarían en conflicto y LangGraph daría un
# error porque no sabría cómo combinar los dos valores.

# Los mensajes de las ramas se guardan en mensaje_a y mensaje_b, y recién el
# nodo comparar arma la bitácora. ¿Por qué no escriben las dos ramas
# directamente en "bitacora"?
# RESPUESTA: Porque las ramas se ejecutan en paralelo y escribirían al mismo
# tiempo en la misma clave. Al guardar cada mensaje en su propia clave se
# evita el conflicto; después de que terminan las dos ramas, comparar reúne
# los mensajes en un orden definido.

# Si conectaras "analizar_a" directo a END, sin pasar por "comparar", ¿qué
# faltaría en el resultado? ¿Qué papel cumple entonces el nodo comparar?
# RESPUESTA: Faltaría la comparación de las dos telas y la bitácora completa.
# El nodo comparar espera los resultados de ambas ramas, calcula cuál tuvo
# mejor evolución y la diferencia entre sus variaciones, y reúne los mensajes.

# Mirá la variación de cada tela y la descripción que AgenteProductos da de
# cada una. ¿Qué diferencias entre los dos productos podrían estar
# relacionadas con el resultado? ¿Qué otra consulta harías al sistema para
# poner a prueba tu idea?
# RESPUESTA: TELA-001 es de la línea indumentaria y tiene las etiquetas
# "jean" y "ropa de trabajo"; TELA-009 es de la línea hogar y tiene las
# etiquetas "cortina" y "almohadón". Aunque ambas son 100% algodón, de origen
# vegetal y nacionales, pueden tener usos y demandas diferentes. Esto podría
# relacionarse con que entre 2019 y 2020 TELA-001 bajó 74.24% y TELA-009 subió
# 43.68%, pero estos datos por sí solos no demuestran que la línea o el uso
# hayan causado esos cambios. Consultaría la variación de ambas telas en
# otros períodos y revisaría sus ventas por sucursal para ver si la tendencia
# se repite y no depende de una sucursal en particular.

# ¿Cuándo NO conviene paralelizar? Dá un ejemplo con los datos de Ortelana.
# RESPUESTA: No conviene paralelizar tareas cuando una necesita el resultado
# de la otra. Por ejemplo, en la Parte 7 no se puede analizar la venta de la
# tela encontrada antes de identificarla por su tag, porque el análisis
# necesita el tela_id que produce el nodo identificar.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 8
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 9 - EVENTOS Y DECISIONES EN EL FLUJO
# ============================================================

# ¿QUÉ VAMOS A HACER?
# Hacer que el flujo cambie de camino según lo que encuentre:
#
#   "Analizá las ventas de una tela en una sucursal y un año."
#
#                       START
#                         |
#                   verificar_datos
#                    /          \
#          hay datos              no hay datos
#              |                       |
#           analizar            informar_problema
#              |                       |
#             END                     END

# ¿POR QUÉ?
# En la Parte 1 contaste 315 combinaciones que no existen. Un sistema que
# siempre sigue el mismo camino va a "analizar" datos que no están y devolver
# ceros que parecen ventas. El flujo tiene que reaccionar a lo que ocurre.

# CONCEPTOS TRABAJADOS
# - Evento: algo que ocurre durante la ejecución y cambia lo que sigue
#   (acá: descubrir que no hay datos).
# - Decisión dentro del flujo.
# - LangGraph: aristas condicionales.
#
# CÓMO FUNCIONA UNA ARISTA CONDICIONAL
#
#   grafo.add_conditional_edges(
#       "nodo_origen",
#       funcion_que_decide,                  # recibe el estado, devuelve un texto
#       {"texto_1": "nodo_destino_1",        # si devuelve "texto_1" va acá
#        "texto_2": "nodo_destino_2"},       # si devuelve "texto_2" va acá
#   )
#
# La función que decide NO modifica el estado: solo lo lee y elige camino.

# QUÉ ESTÁ RESUELTO
# El estado, el nodo que verifica y el nodo que analiza.

# QUÉ TENÉS QUE COMPLETAR
# - TODO 9.1: la función que decide el camino.
# - TODO 9.2: el nodo que informa el problema de manera útil.
# - TODO 9.3: las aristas del grafo, incluida la condicional.


class EstadoDecision(TypedDict):
    sucursal_id: int   # entrada
    tela_id: str       # entrada
    anio: int          # entrada
    hay_datos: bool    # lo escribe verificar_datos
    resultado: dict    # lo escribe analizar (solo si hay datos)
    mensaje: str       # lo escribe analizar o informar_problema


def nodo_verificar_datos(estado):
    # Este nodo produce el dato sobre el que se va a decidir.
    total = agente_ventas.total_anual(estado["tela_id"], estado["anio"], estado["sucursal_id"])
    return {"hay_datos": total["hay_datos"]}


def decidir_camino(estado):
    # TODO 9.1
    # Devolvé el texto "analizar" si hay datos o "informar" si no los hay.
    if estado["hay_datos"]:
        return "analizar"
    else:
        return "informar"


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 9.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def nodo_analizar(estado):
    total = agente_ventas.total_anual(estado["tela_id"], estado["anio"], estado["sucursal_id"])
    sucursal = SUCURSALES[estado["sucursal_id"]]
    return {
        "resultado": total,
        "mensaje": f"{estado['tela_id']} en {sucursal}, {estado['anio']}: {total['total_metros']} m.",
    }


def nodo_informar_problema(estado):
    # TODO 9.2
    # Devolvé la clave "mensaje" con un texto que explique que no hay datos
    # para esa sucursal, tela y año, y que además sea ÚTIL: indicá en qué
    # años sí hay información para esa tela en esa sucursal.
    # Pista: agente_ventas.anios_con_datos(tela_id, sucursal_id)
    # Este nodo NO debe escribir la clave "resultado".
    anios_disponibles = agente_ventas.anios_con_datos(
        estado["tela_id"],
        estado["sucursal_id"],
    )
    sucursal = SUCURSALES[estado["sucursal_id"]]

    if anios_disponibles:
        anios_texto = ", ".join(str(anio) for anio in anios_disponibles)
    else:
        anios_texto = "ninguno"

    mensaje = (
        f"No hay datos para {estado['tela_id']} en {sucursal} "
        f"durante {estado['anio']}. Hay datos para esta tela en los años: "
        f"{anios_texto}."
    )
    return {"mensaje": mensaje}


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 9.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


def construir_grafo_decision():
    grafo = StateGraph(EstadoDecision)

    grafo.add_node("verificar_datos", nodo_verificar_datos)
    grafo.add_node("analizar", nodo_analizar)
    grafo.add_node("informar_problema", nodo_informar_problema)

    grafo.add_edge(START, "verificar_datos")

    # TODO 9.3
    # 1) Agregá la arista condicional que sale de "verificar_datos" usando
    #    decidir_camino. Los textos que devuelve tu función tienen que
    #    coincidir con las claves del diccionario de destinos.
    # 2) Conectá cada uno de los dos caminos con END.
    grafo.add_conditional_edges(
        "verificar_datos",
        decidir_camino,
        {
            "analizar": "analizar",
            "informar": "informar_problema",
        },
    )
    grafo.add_edge("analizar", END)
    grafo.add_edge("informar_problema", END)

    return grafo.compile()


# ------------------------------------------------------------
# FEEDBACK DOCENTE - TODO 9.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - PARTE 9
# ============================================================
# Qué comprobar: la misma consulta, con dos entradas distintas, recorre dos
# caminos distintos. En el caso sin datos NO debe existir "resultado".


def validar_parte_9():
    grafo = construir_grafo_decision()

    con_datos = grafo.invoke({"sucursal_id": 1, "tela_id": "TELA-001", "anio": 2020})
    sin_datos = grafo.invoke({"sucursal_id": 3, "tela_id": "TELA-001", "anio": 2019})

    print("Con datos:", con_datos["mensaje"])
    print("Sin datos:", sin_datos["mensaje"])

    assert con_datos["hay_datos"] is True
    assert con_datos["resultado"]["total_metros"] == 2733.99, "TODO 9.1 / 9.3: camino con datos"
    assert sin_datos["hay_datos"] is False
    assert "resultado" not in sin_datos, "TODO 9.2 / 9.3: sin datos no debe haber resultado"
    assert "2018" in sin_datos["mensaje"], "TODO 9.2: el mensaje debe listar los años disponibles"
    assert "2019" in sin_datos["mensaje"], "TODO 9.2: el mensaje debe decir qué año falta"


ejecutar_validacion(9, "Eventos y decisiones en el flujo", validar_parte_9)

# QUÉ DEBERÍAS PODER EXPLICAR DESPUÉS DE ESTA PARTE
# - Qué diferencia hay entre un nodo y la función que decide el camino.
# - Qué evento provoca el cambio de camino en este grafo.
# - Por qué informar el problema es un resultado válido del sistema.

# ============================================================
# REFLEXIÓN DEL ALUMNO - PARTE 9
# ============================================================

# ¿Qué nodo produce la información sobre la que se decide y qué función toma
# la decisión? ¿Por qué conviene que sean dos cosas separadas?
# RESPUESTA: El nodo verificar_datos consulta al agente de ventas y deja en
# el estado la clave hay_datos. La función decidir_camino lee ese valor y
# elige el camino. Conviene separarlos porque un nodo obtiene la información
# y la función router solo decide qué hacer con ella; así cada parte tiene
# una responsabilidad clara.

# Compará esta decisión con la del AgenteMonitorTela de la Parte 2. ¿Qué
# tienen en común y qué cambió al pasarla a un grafo?
# RESPUESTA: En ambos casos se observa información y se toma una decisión
# según una condición. En AgenteMonitorTela, decidir clasifica la variación
# como caída, crecimiento, estable o sin datos, y actuar arma un mensaje.
# En la Parte 9, verificar_datos produce hay_datos y el router decide qué
# nodo se ejecuta a continuación. La decisión pasa a controlar el recorrido
# del grafo.

# En la Parte 7, nodo_identificar_producto falla si el tag no existe.
# Describí (sin programarlo) cómo lo resolverías con lo que aprendiste acá.
# RESPUESTA: Haría que el nodo de identificación busque las telas del tag y
# guarde en el estado si encontró alguna. Después, una función router elegiría
# entre continuar al análisis si hay coincidencias o ir a otro nodo que
# informe que no se encontró ninguna tela. Así se evita intentar usar
# encontrados[0] cuando la lista está vacía.

# Además de "no hay datos", ¿qué otro evento real de este dataset podría
# justificar un camino alternativo? Mirá tus respuestas de la Parte 3.
# RESPUESTA: Si se pide el total de enero a junio y la fila tiene formato C
# (temporadas), hay datos, pero no se pueden separar exactamente esos meses:
# Metros1 y Metros2 agrupan temporadas que no coinciden con enero-junio.
# El flujo podría elegir una rama que informe que la consulta no es
# respondible con ese formato, en vez de presentar un cálculo incorrecto.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - REFLEXIÓN PARTE 9
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# PARTE 10 - EJERCICIO INTEGRADOR: TELAS DE INVIERNO EN USHUAIA Y POSADAS
# ============================================================

# EL PEDIDO DE LA GERENCIA
# ------------------------
# "Queremos entender cómo se comportan nuestras telas de invierno en dos
#  sucursales: Ushuaia (sucursal 9) y Posadas (sucursal 10).
#  No sabemos de memoria qué telas son: en el catálogo están etiquetadas con
#  el tag 'invierno'. Necesitamos saber qué productos son, cuánto vendió cada
#  sucursal entre 2016 y 2025, si la información está completa y una
#  conclusión que compare las dos sucursales."
#
# Este ejercicio es mucho menos guiado. No hay una única solución correcta:
# vas a tomar decisiones de diseño y a justificarlas.
#
# QUÉ TENÉS QUE HACER
# -------------------
# 1. Interpretar el pedido: ¿qué se pregunta exactamente? ¿qué es dato de
#    entrada y qué hay que averiguar?
# 2. Decidir qué agentes participan y qué responsabilidad tiene cada uno.
#    Podés reutilizar AgenteDatos, AgenteVentas y AgenteProductos, agregarles
#    métodos o crear agentes nuevos.
# 3. Diseñar el estado compartido.
# 4. Identificar las dependencias: ¿qué tarea necesita el resultado de otra?
# 5. Identificar qué tareas son independientes y pueden ir en paralelo.
# 6. Implementar el flujo con LangGraph.
# 7. Probarlo con la validación.
# 8. Documentar tus decisiones al final.
#
# REQUISITOS
# ----------
# R1. Las telas se identifican a partir del tag. Los códigos de tela no
#     pueden estar escritos a mano en tu solución.
#     Usá los agentes especializados de la Parte 4.
# R2. Al menos dos tareas deben ejecutarse en paralelo y debe existir un nodo
#     de sincronización que combine sus resultados.
# R3. Debe existir al menos una decisión en el flujo: si el tag no corresponde
#     a ninguna tela, el sistema informa el problema y no analiza nada.
# R4. Los años sin registro NO se cuentan como cero: se informan. Para cada
#     sucursal y cada tela hay que indicar qué años del período faltan.
# R5. El informe final combina información comercial y de producto: debe
#     nombrar las telas y decir algo de sus características (composición,
#     origen de la fibra o línea textil).
# R6. Los nodos no leen archivos ni usan DataFrames directamente: le piden
#     la información a los agentes.
#
# FORMATO DEL RESULTADO (lo que revisa la validación)
# ---------------------------------------------------
# resolver_integrador(tag, sucursal_a, sucursal_b) devuelve un diccionario
# (el estado final) que contiene al menos:
#
#   "telas":      lista de tela_id encontrados, ordenada
#   "informe":    texto final para la gerencia
#
# y, cuando el tag existe, además:
#
#   "productos":  lista con la descripción de cada tela
#   "analisis_a": {"sucursal_id": ...,
#                  "total_metros": total del período sumando todas las telas,
#                  "anios_faltantes": {tela_id: [años sin registro], ...}}
#   "analisis_b": lo mismo para la otra sucursal
#   "comparacion": {"sucursal_mayor": sucursal_id de la que más vendió,
#                   "relacion": cuántas veces más vendió (mayor / menor)}
#
# Cuando el tag NO existe: "telas" es una lista vacía, "informe" explica el
# problema y las claves de análisis no deben estar presentes.
#
# PISTAS
# ------
# - Releé las Partes 7, 8 y 9: este ejercicio combina los tres grafos.
# - Empezá en papel: dibujá los nodos y las flechas antes de programar.
# - En LangGraph un nodo no puede llamarse igual que una clave del estado.
#   Usá verbos para los nodos ("comparar") y sustantivos para las claves
#   ("comparacion").
# - Los años del período están en ANIOS_INTEGRADOR.
# - AgenteVentas.anios_con_datos() ya existe. Si necesitás otro cálculo de
#   ventas, agregalo como método de AgenteVentas en la Parte 4, no dentro de
#   un nodo.
# - Si dos ramas escriben el estado, repasá la regla de la Parte 8.
#
# DESAFÍO OPCIONAL
# ----------------
# El total de cada sucursal incluye años distintos (a una le falta 2019, a la
# otra 2022...). Agregá a "comparacion" una clave "relacion_justa" calculada
# solo con los años y telas en que AMBAS sucursales tienen registro.

TAG_INTEGRADOR = "invierno"
SUCURSAL_A = 9    # Filial Ushuaia
SUCURSAL_B = 10   # Filial Posadas
ANIOS_INTEGRADOR = list(range(2016, 2026))


# TODO I.1 - DISEÑO (antes de programar)
# Completá esta tabla. Es parte de la entrega.
#
#   Agente / nodo | Responsabilidad | Lee del estado | Escribe en el estado
#   --------------|-----------------|----------------|---------------------
#   AgenteProductos| Buscar tag y describir telas | tag/tela_id | devuelve datos
#   AgenteVentas  | Sumar ventas y detectar años faltantes | tela_id, años, sucursal | devuelve análisis
#   identificar   | Buscar telas por tag y obtener fichas | tag | telas, productos
#   decidir       | Elegir informar o analizar | telas | (elige camino)
#   preparar      | Abrir las dos ramas de sucursales | tag, sucursal_a, sucursal_b | mensaje_inicio
#   analizar_a    | Analizar período de sucursal_a | telas, sucursal_a | analisis_a
#   analizar_b    | Analizar período de sucursal_b | telas, sucursal_b | analisis_b
#   combinar      | Comparar totales y redactar informe | productos, analisis_a, analisis_b | comparacion, informe
#   informar      | Informar que no hay telas para el tag | tag, telas | informe
#
# Flujo (dibujalo con texto, como los diagramas de las Partes 8 y 9):
# START → identificar → decisión
#   sin coincidencias → informar → END
#   con coincidencias → preparar → [analizar_a || analizar_b] → combinar → END
#

# ------------------------------------------------------------
# FEEDBACK DOCENTE - INTEGRADOR I.1
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# TODO I.2 - ESTADO
# Declará todas las claves que necesite tu grafo. Las tres primeras son las
# entradas; el resto lo definís vos según tu diseño y el formato del resultado.
class EstadoIntegrador(TypedDict):
    # Entradas que recibe resolver_integrador().
    tag: str
    sucursal_a: int
    sucursal_b: int
    # Las completa identificar y las usan las ramas comerciales.
    telas: list
    productos: list
    # Preparación y resultados: cada rama de sucursal escribe en su propia
    # clave, y combinar escribe la comparación y el texto final.
    mensaje_inicio: str
    analisis_a: dict
    analisis_b: dict
    comparacion: dict
    informe: str


# ------------------------------------------------------------
# FEEDBACK DOCENTE - INTEGRADOR I.2
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# TODO I.3 - NODOS
# Escribí acá las funciones de tus nodos y, si la necesitás, la función que
# decide el camino.


def nodo_identificar_telas(estado):
    # Busca por la etiqueta solicitada, sin dejar códigos de tela escritos
    # a mano. sorted() mantiene la lista en el orden que pide la consigna.
    telas = sorted(agente_productos.buscar_por_tag(estado["tag"]))
    productos = []

    for tela_id in telas:
        # Pedimos la descripción al agente especializado para que el informe
        # pueda incluir características de cada tela.
        productos.append(agente_productos.describir(tela_id))

    return {
        "telas": telas,
        "productos": productos,
    }


def decidir_camino_integrador(estado):
    # Igual que el router de la Parte 9, esta función solo elige el recorrido:
    # no modifica el estado ni hace cálculos comerciales.
    if estado["telas"]:
        return "analizar"
    else:
        return "informar"


def nodo_informar_tag_sin_telas(estado):
    # Este camino se usa cuando buscar_por_tag() devolvió una lista vacía.
    # Solo informa; no crea resultados de análisis porque no hay telas que
    # analizar.
    return {
        "informe": (
            f"No se encontraron telas con el tag '{estado['tag']}'. "
            "No se realizaron análisis de ventas."
        )
    }


def nodo_preparar_analisis(estado):
    # Se ejecuta únicamente si hay telas. Deja un mensaje común antes de
    # abrir las ramas independientes de las dos sucursales.
    return {
        "mensaje_inicio": (
            f"Se analizarán las telas del tag '{estado['tag']}' "
            f"en {SUCURSALES[estado['sucursal_a']]} y "
            f"{SUCURSALES[estado['sucursal_b']]}."
        )
    }


def nodo_analizar_sucursal_a(estado):
    # Rama A: analiza la misma lista de telas y período en sucursal_a.
    # resumen_periodo() pertenece a AgenteVentas; este nodo solo lo consulta.
    analisis = agente_ventas.resumen_periodo(
        estado["telas"],
        ANIOS_INTEGRADOR,
        estado["sucursal_a"],
    )
    return {"analisis_a": analisis}


def nodo_analizar_sucursal_b(estado):
    # Rama B hace el mismo tipo de análisis, pero para sucursal_b. No depende
    # de analisis_a y escribe otra clave para evitar choques entre ramas.
    analisis = agente_ventas.resumen_periodo(
        estado["telas"],
        ANIOS_INTEGRADOR,
        estado["sucursal_b"],
    )
    return {"analisis_b": analisis}


def nodo_combinar_resultados(estado):
    # Punto de sincronización: LangGraph llega acá después de que terminaron
    # las dos ramas. Por eso ya están disponibles sus totales y faltantes.
    analisis_a = estado["analisis_a"]
    analisis_b = estado["analisis_b"]
    total_a = analisis_a["total_metros"]
    total_b = analisis_b["total_metros"]

    # Elegimos la sucursal con mayor total registrado y guardamos ambos
    # importes para calcular la relación mayor/menor.
    if total_a >= total_b:
        sucursal_mayor = estado["sucursal_a"]
        total_mayor = total_a
        total_menor = total_b
    else:
        sucursal_mayor = estado["sucursal_b"]
        total_mayor = total_b
        total_menor = total_a

    # No se divide por cero. En ese caso la relación no se puede calcular y
    # se representa con None, en lugar de inventar un número.
    if total_menor == 0:
        relacion = None
    else:
        relacion = round(total_mayor / total_menor, 2)

    comparacion = {
        "sucursal_mayor": sucursal_mayor,
        "relacion": relacion,
    }

    # Construimos el informe línea por línea para que incluya la descripción
    # comercial, las características de producto y la cobertura de datos.
    lineas_informe = [
        estado["mensaje_inicio"],
        "Características de las telas identificadas:",
    ]

    for producto in estado["productos"]:
        lineas_informe.append(
            f"- {producto['nombre']}: línea {producto['linea_textil']}, "
            f"composición {producto['composicion']}, "
            f"origen de la fibra {producto['origen_fibra']}."
        )

    for analisis in [analisis_a, analisis_b]:
        sucursal_id = analisis["sucursal_id"]
        nombre_sucursal = SUCURSALES[sucursal_id]
        lineas_informe.append(
            f"Total registrado en {nombre_sucursal} entre "
            f"{ANIOS_INTEGRADOR[0]} y {ANIOS_INTEGRADOR[-1]}: "
            f"{analisis['total_metros']} metros."
        )

        for tela_id in estado["telas"]:
            faltantes = analisis["anios_faltantes"][tela_id]
            if faltantes:
                anios_texto = ", ".join(str(anio) for anio in faltantes)
                lineas_informe.append(
                    f"Años sin registro para {tela_id} en "
                    f"{nombre_sucursal}: {anios_texto}."
                )
            else:
                lineas_informe.append(
                    f"No hay años faltantes para {tela_id} en "
                    f"{nombre_sucursal}."
                )

    if relacion is None:
        comparacion_texto = (
            f"{SUCURSALES[sucursal_mayor]} tiene el mayor total registrado, "
            "pero no se puede calcular la relación porque el otro total es cero."
        )
    else:
        comparacion_texto = (
            f"{SUCURSALES[sucursal_mayor]} registró {relacion} veces el total "
            "de la otra sucursal en el período."
        )

    lineas_informe.append(comparacion_texto)
    lineas_informe.append(
        "La comparación usa los registros disponibles; los años faltantes "
        "no se contaron como cero."
    )

    return {
        "comparacion": comparacion,
        "informe": "\n".join(lineas_informe),
    }


# ------------------------------------------------------------
# FEEDBACK DOCENTE - INTEGRADOR I.3
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# TODO I.4 - GRAFO
def construir_grafo_integrador():
    grafo = StateGraph(EstadoIntegrador)

    # Registramos las funciones de trabajo con nombres de nodos.
    # Los nombres no coinciden con las claves del estado.
    grafo.add_node("identificar", nodo_identificar_telas)
    grafo.add_node("informar", nodo_informar_tag_sin_telas)
    grafo.add_node("preparar", nodo_preparar_analisis)
    grafo.add_node("analizar_a", nodo_analizar_sucursal_a)
    grafo.add_node("analizar_b", nodo_analizar_sucursal_b)
    grafo.add_node("combinar", nodo_combinar_resultados)

    # Primero identificamos las telas. La decisión usa la lista resultante
    # para elegir el camino: informar si está vacía o analizar si tiene telas.
    grafo.add_edge(START, "identificar")
    grafo.add_conditional_edges(
        "identificar",
        decidir_camino_integrador,
        {
            "informar": "informar",
            "analizar": "preparar",
        },
    )
    # Si no hubo coincidencias, informamos y terminamos sin ejecutar ramas.
    grafo.add_edge("informar", END)

    # Si hay telas, las dos sucursales se analizan en paralelo porque ninguna
    # necesita el resultado de la otra. combinar espera a las dos (sincroniza).
    grafo.add_edge("preparar", "analizar_a")
    grafo.add_edge("preparar", "analizar_b")
    grafo.add_edge(["analizar_a", "analizar_b"], "combinar")
    grafo.add_edge("combinar", END)

    return grafo.compile()


# ------------------------------------------------------------
# FEEDBACK DOCENTE - INTEGRADOR I.4
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# TODO I.5 - PUNTO DE ENTRADA
def resolver_integrador(tag, sucursal_a, sucursal_b):
    # Debe construir el grafo, ejecutarlo con las tres entradas y devolver el
    # estado final.
    grafo = construir_grafo_integrador()
    # invoke() inicia el flujo con el estado de entrada y devuelve el estado
    # final, incluidas las claves que fueron escribiendo los nodos.
    return grafo.invoke({
        "tag": tag,
        "sucursal_a": sucursal_a,
        "sucursal_b": sucursal_b,
    })


# ------------------------------------------------------------
# FEEDBACK DOCENTE - INTEGRADOR I.5
# Estado: [ ] Correcto  [ ] Revisar
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# VALIDACIÓN - EJERCICIO INTEGRADOR
# ============================================================
# Estos asserts revisan el FORMATO DEL RESULTADO, no tu diseño interno. Que
# pasen es necesario pero no suficiente: revisá también los requisitos R1 a
# R6 leyendo tu propio código.


def validar_integrador():
    final = resolver_integrador(TAG_INTEGRADOR, SUCURSAL_A, SUCURSAL_B)

    print("Telas:", final["telas"])
    print("Análisis A:", final["analisis_a"])
    print("Análisis B:", final["analisis_b"])
    print("Comparación:", final["comparacion"])
    print("Informe:", final["informe"])

    assert final["telas"] == ["TELA-007", "TELA-027"], "R1: telas identificadas por tag"
    assert len(final["productos"]) == 2, "R5: una descripción por tela"

    a = final["analisis_a"]
    b = final["analisis_b"]
    assert a["sucursal_id"] == 9 and b["sucursal_id"] == 10, "cada análisis indica su sucursal"
    assert abs(a["total_metros"] - 46656.11) < 1, "total de Ushuaia en el período"
    assert abs(b["total_metros"] - 627.27) < 1, "total de Posadas en el período"
    assert a["anios_faltantes"] == {"TELA-007": [2021, 2022], "TELA-027": [2019]}, "R4: faltantes en Ushuaia"
    assert b["anios_faltantes"] == {"TELA-007": [], "TELA-027": [2022]}, "R4: faltantes en Posadas"

    assert final["comparacion"]["sucursal_mayor"] == 9, "comparación: sucursal con más ventas"
    assert 70 < final["comparacion"]["relacion"] < 80, "comparación: relación entre totales"

    assert "Paño de Lana Cardado" in final["informe"], "R5: el informe nombra las telas"
    assert "Escocés de Lana Wellington" in final["informe"], "R5: el informe nombra las telas"

    # Segundo escenario: un tag que no existe (requisito R3).
    vacio = resolver_integrador("tag-que-no-existe", SUCURSAL_A, SUCURSAL_B)
    print("Informe con tag inexistente:", vacio["informe"])
    assert vacio["telas"] == [], "R3: sin coincidencias la lista queda vacía"
    assert isinstance(vacio["informe"], str) and len(vacio["informe"]) > 0, "R3: debe informar el problema"
    assert "analisis_a" not in vacio and "comparacion" not in vacio, "R3: sin telas no se analiza nada"


ejecutar_validacion(10, "Ejercicio integrador", validar_integrador)

# ============================================================
# TODO I.6 - DOCUMENTACIÓN DE DECISIONES (parte de la entrega)
# ============================================================

# ¿Qué agentes participan y qué responsabilidad tiene cada uno? ¿Tuviste que
# agregar algún método o agente? ¿Por qué lo pusiste donde lo pusiste?
# RESPUESTA: Participan AgenteProductos, que busca las telas por tag y
# devuelve sus descripciones, y AgenteVentas, que calcula las ventas por
# sucursal y los años faltantes. Agregué resumen_periodo() a AgenteVentas
# porque sumar ventas y revisar cobertura son tareas de análisis comercial;
# los nodos le piden ese resultado al agente y no acceden directamente al CSV.

# ¿Qué tareas dependen de otras? Indicá para cada dependencia qué dato la
# origina.
# RESPUESTA: Primero identificar_telas busca coincidencias y guarda telas y
# productos. La decisión depende de si la lista telas quedó vacía. Si hay
# telas, preparar abre las dos ramas; cada análisis depende de telas, de los
# años del período y de su sucursal. combinar depende de que estén disponibles
# analisis_a y analisis_b para comparar los totales y redactar el informe.

# ¿Qué tareas pusiste en paralelo y qué te asegura que son independientes?
# ¿Había otra forma válida de dividir el trabajo en ramas?
# RESPUESTA: Puse en paralelo el análisis de todas las telas para sucursal_a
# y el análisis para sucursal_b. Ambos usan las mismas telas y años, pero cada
# uno consulta una sucursal distinta y ninguno necesita el resultado del otro.
# Otra división válida sería hacer una rama por cada tela y analizar ambas
# sucursales dentro de cada rama.

# ¿Qué nodo sincroniza y qué necesita tener disponible para poder hacerlo?
# RESPUESTA: nodo_combinar_resultados sincroniza las ramas. Necesita recibir
# analisis_a y analisis_b, además de telas y productos, para calcular la
# comparación entre sucursales y escribir el informe completo.

# ¿Cómo evitaste que las ramas paralelas se pisaran al escribir el estado?
# RESPUESTA: Cada rama escribe en una clave distinta: analizar_a escribe
# analisis_a y analizar_b escribe analisis_b. El nodo de sincronización es el
# único que escribe comparacion e informe después de que terminan las dos.

# ¿Qué decisión tiene tu flujo y qué evento la dispara?
# RESPUESTA: Después de buscar el tag, la función decidir_camino_integrador
# revisa si telas está vacía. Si no hay coincidencias, el flujo informa el
# problema y termina; si hay telas, continúa con los análisis de las dos
# sucursales. El evento que determina el camino es encontrar o no telas con
# ese tag.

# Tu arquitectura, ¿es centralizada, distribuida o híbrida? Justificá.
# RESPUESTA: Es centralizada: el grafo define el orden, aplica la decisión
# sobre el tag y coordina las dos ramas. Los agentes especializados realizan
# las consultas de productos y ventas, pero no se coordinan entre sí ni eligen
# el recorrido del flujo.

# ¿Dónde aparecen en tu solución percepción, decisión y acción?
# RESPUESTA: La percepción aparece cuando AgenteProductos busca qué telas
# tienen el tag y cuando AgenteVentas consulta los registros de cada sucursal.
# La decisión aparece en decidir_camino_integrador, que revisa si se encontró
# alguna tela. La acción aparece en las ramas que calculan las ventas y en
# nodo_combinar_resultados, que redacta el informe para la gerencia.

# Conclusión de negocio: ¿qué le dirías a la gerencia sobre las telas de
# invierno en Ushuaia y Posadas? ¿Cómo afectan los años faltantes a la
# confianza en esa conclusión?
# RESPUESTA: En los registros disponibles para 2016–2025, las telas con tag
# invierno suman más metros en Ushuaia que en Posadas. La comparación no es
# completamente pareja porque faltan registros: en Ushuaia faltan 2021 y
# 2022 para TELA-007 y 2019 para TELA-027; en Posadas falta 2022 para
# TELA-027. Esos años no se interpretan como ventas cero, pero su ausencia
# reduce la confianza en la comparación de los totales. El informe permite
# identificar qué datos faltan antes de sacar una conclusión más firme.

# ------------------------------------------------------------
# FEEDBACK DOCENTE - DOCUMENTACIÓN INTEGRADOR
# Comentario:
#
# ------------------------------------------------------------


# ============================================================
# RESUMEN DE AVANCE
# ============================================================

if PARTE_A_EJECUTAR is None:
    print()
    print("=" * 64)
    print("RESUMEN DE AVANCE")
    print("=" * 64)
    for numero in sorted(RESULTADOS):
        nombre = "Integrador" if numero == 10 else f"Parte {numero}"
        print(f"  {nombre:<11} {RESULTADOS[numero]}")


# ============================================================
# FEEDBACK DOCENTE - EVALUACIÓN GENERAL
# ============================================================

# Resultado general:
#
# Aspectos logrados:
#
# Aspectos a revisar:
#
# Recomendaciones:
#
# Comentario final:
#
