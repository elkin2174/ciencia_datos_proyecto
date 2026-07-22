"""
ETL Fast and Safe
=================
Extrae la información de la base operacional aquitoy_2 y carga la
bodega olap_fastandsafe.

Granularidad de salida:
- ft_servicio: una fila por servicio.
- ft_fase_servicio: una fila por servicio y fase registrada.
- ft_novedad_servicio: una fila por novedad registrada.

Requisitos:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv

Variables requeridas en .env:
    OLTP_URI=postgresql+psycopg2://USUARIO:CONTRASENA@localhost:5433/aquitoy_2
    OLAP_URI=postgresql+psycopg2://USUARIO:CONTRASENA@localhost:5433/olap_fastandsafe
"""

from __future__ import annotations

import os
import sys
import traceback
from dataclasses import dataclass

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Connection, Engine, create_engine, text


# ============================================================
# CONFIGURACIÓN
# ============================================================

load_dotenv()

OLTP_URI = os.getenv("OLTP_URI")
OLAP_URI = os.getenv("OLAP_URI")

# Estado real en la OLTP -> fase dimensional.
# El estado 3 ("Con novedad") no es una fase del ciclo principal.
ESTADO_TO_FASE: dict[int, int] = {
    1: 1,  # Iniciado
    2: 2,  # Con mensajero asignado
    4: 3,  # Recogido por mensajero
    5: 4,  # Entregado en destino
    6: 5,  # Terminado completo / Cerrado
}

MESES_ES = [
    "",
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]

DIAS_ES = [
    "Lunes",
    "Martes",
    "Miércoles",
    "Jueves",
    "Viernes",
    "Sábado",
    "Domingo",
]


@dataclass(frozen=True)
class EtlStats:
    servicios_fuente: int
    estados_fuente: int
    novedades_fuente: int
    fases_esperadas: int
    fases_repetidas: int
    fases_con_secuencia_invalida: int
    sedes_cliente_inconsistentes: int
    sedes_sin_relacion: int
    clientes_desconocidos: int
    sedes_desconocidas: int
    mensajeros_desconocidos: int


@dataclass(frozen=True)
class SourceIds:
    servicios_reales: frozenset[int]
    servicios_prueba: frozenset[int]
    novedades_validas: frozenset[int]
    novedades_prueba: frozenset[int]
    novedades_servicio_prueba: frozenset[int]


# ============================================================
# UTILIDADES
# ============================================================

def require_configuration() -> tuple[str, str]:
    missing: list[str] = []

    if not OLTP_URI:
        missing.append("OLTP_URI")

    if not OLAP_URI:
        missing.append("OLAP_URI")

    if missing:
        raise RuntimeError(
            "Faltan variables de configuración: " + ", ".join(missing)
        )

    # Después de las validaciones, ambas variables son str.
    assert OLTP_URI is not None
    assert OLAP_URI is not None

    return OLTP_URI, OLAP_URI


def test_connections(oltp: Engine, olap: Engine) -> None:
    with oltp.connect() as conn:
        conn.execute(text("SELECT 1"))
    with olap.connect() as conn:
        conn.execute(text("SELECT 1"))


def combine_date_time(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    """Combina una columna DATE y una columna TIME en timestamps."""
    return pd.to_datetime(
        date_series.astype(str) + " " + time_series.astype(str),
        errors="coerce",
        format="mixed",
    )


def make_timestamp_naive(series: pd.Series) -> pd.Series:
    """Convierte timestamps con zona horaria a timestamps sin zona."""
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.tz_convert("America/Bogota").dt.tz_localize(None)


def load_df(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    *,
    chunksize: int = 1000,
) -> None:
    if df.empty:
        print(f"    {table_name}: 0 registros")
        return

    df.to_sql(
        table_name,
        conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=chunksize,
    )
    print(f"    {table_name}: {len(df):,} registros")


def read_sk_map(
    conn: Connection,
    table: str,
    natural_key_col: str,
    surrogate_key_col: str,
) -> dict[str, int]:
    df = pd.read_sql(
        text(
            f"SELECT {surrogate_key_col}, {natural_key_col} "
            f"FROM {table}"
        ),
        conn,
    )
    return {
        str(natural): int(surrogate)
        for natural, surrogate in zip(
            df[natural_key_col], df[surrogate_key_col], strict=False
        )
    }


# ============================================================
# EXTRACT
# ============================================================

def extract_servicios(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT
                id::bigint AS id_servicio,
                cliente_id,
                mensajero_id,
                usuario_id,
                fecha_solicitud,
                hora_solicitud
            FROM mensajeria_servicio
            WHERE es_prueba = FALSE
            ORDER BY id
            """
        ),
        engine,
    )


def extract_estados(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT
                e.id::bigint AS id_estado_evento,
                e.servicio_id::bigint AS id_servicio,
                e.fecha,
                e.hora,
                e.estado_id
            FROM mensajeria_estadosservicio e
            JOIN mensajeria_servicio s
                ON s.id = e.servicio_id
            WHERE e.es_prueba = FALSE
              AND s.es_prueba = FALSE
            ORDER BY e.id
            """
        ),
        engine,
    )


def extract_novedades(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT
                n.id::bigint AS id_novedad_evento,
                n.servicio_id::bigint AS id_servicio,
                n.tipo_novedad_id,
                n.fecha_novedad,
                n.descripcion
            FROM mensajeria_novedadesservicio n
            JOIN mensajeria_servicio s
                ON s.id = n.servicio_id
            WHERE n.es_prueba = FALSE
              AND s.es_prueba = FALSE
            ORDER BY n.id
            """
        ),
        engine,
    )


def extract_clientes(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text("SELECT cliente_id, nombre FROM cliente ORDER BY cliente_id"),
        engine,
    )


def extract_sedes(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT
                s.sede_id,
                s.cliente_id,
                s.nombre,
                s.direccion,
                c.nombre AS ciudad
            FROM sede s
            LEFT JOIN ciudad c
                ON c.ciudad_id = s.ciudad_id
            ORDER BY s.sede_id
            """
        ),
        engine,
    )


def extract_usuarios_sede(engine: Engine) -> pd.DataFrame:
    # mensajeria_servicio.usuario_id referencia clientes_usuarioaquitoy.id.
    return pd.read_sql(
        text(
            """
            SELECT
                id::bigint AS usuario_id,
                cliente_id,
                sede_id
            FROM clientes_usuarioaquitoy
            ORDER BY id
            """
        ),
        engine,
    )


def extract_mensajeros(engine: Engine) -> pd.DataFrame:
    """Extrae solo la clave natural; los nombres se seudonimizan por diseño."""
    return pd.read_sql(
        text(
            """
            SELECT DISTINCT id
            FROM clientes_mensajeroaquitoy
            ORDER BY id
            """
        ),
        engine,
    )


def extract_tipos_novedad(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT id AS id_tipo_novedad, nombre AS nom_tipo_novedad
            FROM mensajeria_tiponovedad
            ORDER BY id
            """
        ),
        engine,
    )


def extract_source_ids(engine: Engine) -> SourceIds:
    """Obtiene identificadores de control para validar exclusiones y cobertura."""
    servicios = pd.read_sql(
        text("SELECT id::bigint AS id_servicio, es_prueba FROM mensajeria_servicio"),
        engine,
    )
    novedades = pd.read_sql(
        text(
            """
            SELECT
                n.id::bigint AS id_novedad_evento,
                n.es_prueba,
                s.es_prueba AS servicio_es_prueba
            FROM mensajeria_novedadesservicio n
            JOIN mensajeria_servicio s
                ON s.id = n.servicio_id
            """
        ),
        engine,
    )

    return SourceIds(
        servicios_reales=frozenset(
            servicios.loc[~servicios["es_prueba"], "id_servicio"].astype(int)
        ),
        servicios_prueba=frozenset(
            servicios.loc[servicios["es_prueba"], "id_servicio"].astype(int)
        ),
        novedades_validas=frozenset(
            novedades.loc[
                ~novedades["es_prueba"] & ~novedades["servicio_es_prueba"],
                "id_novedad_evento",
            ].astype(int)
        ),
        novedades_prueba=frozenset(
            novedades.loc[
                novedades["es_prueba"], "id_novedad_evento"
            ].astype(int)
        ),
        novedades_servicio_prueba=frozenset(
            novedades.loc[
                novedades["servicio_es_prueba"], "id_novedad_evento"
            ].astype(int)
        ),
    )


# ============================================================
# TRANSFORMACIÓN DE DIMENSIONES
# ============================================================

def build_dim_fecha(servicios: pd.DataFrame) -> pd.DataFrame:
    fechas = pd.to_datetime(
        servicios["fecha_solicitud"],
        errors="coerce",
    )

    fechas_validas = fechas.dropna()

    if fechas_validas.empty:
        raise ValueError("No hay fechas válidas para construir dim_fecha.")

    rango = pd.date_range(
        start=fechas_validas.min(),
        end=fechas_validas.max(),
        freq="D",
    )

    dim = pd.DataFrame({"fecha": rango})

    # Clave de fecha con formato YYYYMMDD.
    dim["sk_fecha"] = (
        dim["fecha"]
        .dt.strftime("%Y%m%d")
        .astype("int64")
    )

    dim["dia"] = dim["fecha"].dt.day
    dim["dia_semana_num"] = dim["fecha"].dt.weekday + 1

    mapa_dias = dict(enumerate(DIAS_ES))
    dim["dia_semana"] = dim["fecha"].dt.weekday.map(mapa_dias)

    dim["mes"] = dim["fecha"].dt.month

    mapa_meses = dict(enumerate(MESES_ES))
    dim["mes_nombre"] = dim["mes"].map(mapa_meses)

    dim["trimestre"] = dim["fecha"].dt.quarter
    dim["anio"] = dim["fecha"].dt.year

    # Actualmente el proyecto no carga un calendario de festivos.
    dim["es_festivo"] = False

    # PostgreSQL recibe la fecha sin componente de hora.
    dim["fecha"] = dim["fecha"].dt.date

    return dim[
        [
            "sk_fecha",
            "fecha",
            "dia",
            "dia_semana_num",
            "dia_semana",
            "mes",
            "mes_nombre",
            "trimestre",
            "anio",
            "es_festivo",
        ]
    ]

def build_dim_cliente(clientes: pd.DataFrame) -> pd.DataFrame:
    result = clientes.rename(
        columns={
            "cliente_id": "id_cliente_nk",
            "nombre": "nom_cliente",
        }
    ).copy()
    result["id_cliente_nk"] = result["id_cliente_nk"].astype(str)
    result["nom_cliente"] = result["nom_cliente"].fillna("Sin nombre")
    return result[["id_cliente_nk", "nom_cliente"]].drop_duplicates(
        subset=["id_cliente_nk"]
    )


def build_dim_mensajero(mensajeros: pd.DataFrame) -> pd.DataFrame:
    ids = mensajeros["id"].astype("Int64").astype(str)

    return pd.DataFrame(
        {
            "id_mensajero_nk": ids,
            "nom_mensajero": "Mensajero " + ids,
        }
    )

def build_dim_sede(
    sedes: pd.DataFrame,
    sk_cliente_map: dict[str, int],
) -> pd.DataFrame:
    result = sedes.copy()
    result["id_sede_nk"] = result["sede_id"].astype(str)
    result["sk_cliente"] = (
        result["cliente_id"].astype(str).map(sk_cliente_map).fillna(0).astype(int)
    )
    result["nom_sede"] = result["nombre"].fillna("Sin nombre")

    return result[
        [
            "id_sede_nk",
            "sk_cliente",
            "nom_sede",
            "ciudad",
            "direccion",
        ]
    ].drop_duplicates(subset=["id_sede_nk"])


def build_dim_tipo_novedad(tipos: pd.DataFrame) -> pd.DataFrame:
    result = tipos.rename(
        columns={"id_tipo_novedad": "id_tipo_novedad_nk"}
    ).copy()
    result["id_tipo_novedad_nk"] = result["id_tipo_novedad_nk"].astype(str)
    result["nom_tipo_novedad"] = result["nom_tipo_novedad"].fillna(
        "Sin nombre"
    )
    return result[
        ["id_tipo_novedad_nk", "nom_tipo_novedad"]
    ].drop_duplicates(subset=["id_tipo_novedad_nk"])


# ============================================================
# TRANSFORMACIÓN DE ESTADOS Y HECHOS
# ============================================================

def normalize_estados(estados: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    result = estados.copy()
    result["sk_fase"] = result["estado_id"].map(ESTADO_TO_FASE)
    result = result.dropna(subset=["sk_fase"])
    result["sk_fase"] = result["sk_fase"].astype(int)
    result["fecha_hora_inicio"] = combine_date_time(
        result["fecha"], result["hora"]
    )
    result = result.dropna(subset=["fecha_hora_inicio"])

    result = result.sort_values(
        ["id_servicio", "sk_fase", "fecha_hora_inicio", "id_estado_evento"]
    )

    duplicate_count = int(
        result.duplicated(subset=["id_servicio", "sk_fase"]).sum()
    )

    # La bodega admite una sola ocurrencia por servicio y fase. Se conserva
    # la primera transición registrada hacia cada fase.
    result = result.drop_duplicates(
        subset=["id_servicio", "sk_fase"], keep="first"
    )

    return result[
        [
            "id_estado_evento",
            "id_servicio",
            "sk_fase",
            "fecha_hora_inicio",
        ]
    ], duplicate_count


def build_ft_servicio(
    servicios: pd.DataFrame,
    usuarios_sede: pd.DataFrame,
    estados_normalizados: pd.DataFrame,
    sk_cliente_map: dict[str, int],
    sk_sede_map: dict[str, int],
    sk_mensajero_map: dict[str, int],
) -> tuple[pd.DataFrame, int, int, int, int]:
    fact = servicios.copy()
    fact["fecha_hora_solicitud"] = combine_date_time(
        fact["fecha_solicitud"], fact["hora_solicitud"]
    )

    if fact["fecha_hora_solicitud"].isna().any():
        invalid = int(fact["fecha_hora_solicitud"].isna().sum())
        raise ValueError(
            f"Hay {invalid} servicios con fecha/hora de solicitud inválida."
        )

    # Sede directa: servicio.usuario_id -> clientes_usuarioaquitoy.id -> sede_id.
    user_location = usuarios_sede[
        ["usuario_id", "cliente_id", "sede_id"]
    ].rename(columns={"cliente_id": "cliente_usuario_id"})
    fact = fact.merge(
        user_location,
        on="usuario_id",
        how="left",
        validate="many_to_one",
    )

    cierre = (
    estados_normalizados.loc[
        estados_normalizados["sk_fase"].eq(5),
        ["id_servicio", "fecha_hora_inicio"],
    ]
    .groupby("id_servicio", as_index=False)
    .agg(fecha_hora_cierre=("fecha_hora_inicio", "max"))
)
    fact = fact.merge(cierre, on="id_servicio", how="left")

    fact["sk_fecha_solicitud"] = (
        fact["fecha_hora_solicitud"].dt.strftime("%Y%m%d").astype(int)
    )
    fact["id_hora_solicitud"] = fact["fecha_hora_solicitud"].dt.hour.astype(int)

    cliente_nk = fact["cliente_id"].astype("Int64").astype(str)
    cliente_sk = cliente_nk.map(sk_cliente_map)
    clientes_desconocidos = int(cliente_sk.isna().sum())
    if clientes_desconocidos:
        raise ValueError(
            "Hay servicios cuyo cliente directo no existe en dim_cliente: "
            f"{clientes_desconocidos}."
        )
    fact["sk_cliente"] = cliente_sk.astype(int)

    cliente_servicio = fact["cliente_id"].astype("Int64")
    cliente_usuario = fact["cliente_usuario_id"].astype("Int64")
    sede_presente = fact["sede_id"].notna()
    relacion_presente = cliente_usuario.notna() & sede_presente
    sede_cliente_inconsistente = relacion_presente & (
        cliente_servicio != cliente_usuario
    )
    sede_sin_relacion = ~relacion_presente
    sede_cliente_valida = relacion_presente & ~sede_cliente_inconsistente

    sede_nk = fact["sede_id"].astype("Int64").astype(str)
    sede_sk = sede_nk.map(sk_sede_map)
    sedes_no_catalogadas = int((sede_cliente_valida & sede_sk.isna()).sum())
    if sedes_no_catalogadas:
        raise ValueError(
            "Hay sedes válidas de la fuente que no existen en dim_sede: "
            f"{sedes_no_catalogadas}."
        )

    # Política de calidad: una sede solo se conserva si pertenece al mismo
    # cliente directo del servicio. Las relaciones ausentes o inconsistentes
    # se envían al miembro desconocido sin eliminar el servicio.
    fact["sk_sede"] = sede_sk.where(sede_cliente_valida, 0).fillna(0).astype(int)

    mensajero_nk = fact["mensajero_id"].astype("Int64").astype(str)
    mensajero_sk = mensajero_nk.map(sk_mensajero_map)
    mensajeros_no_catalogados = int(
        (fact["mensajero_id"].notna() & mensajero_sk.isna()).sum()
    )
    if mensajeros_no_catalogados:
        raise ValueError(
            "Hay mensajeros informados que no existen en dim_mensajero: "
            f"{mensajeros_no_catalogados}."
        )
    fact["sk_mensajero"] = mensajero_sk.fillna(0).astype(int)

    fact["tiempo_entrega_min"] = (
        fact["fecha_hora_cierre"] - fact["fecha_hora_solicitud"]
    ).dt.total_seconds() / 60

    # Un cierre anterior a la solicitud es un dato inválido, no un tiempo cero.
    fact.loc[fact["tiempo_entrega_min"] < 0, "tiempo_entrega_min"] = pd.NA
    fact["cantidad_servicios"] = 1

    result = fact[
        [
            "id_servicio",
            "sk_fecha_solicitud",
            "id_hora_solicitud",
            "sk_cliente",
            "sk_sede",
            "sk_mensajero",
            "fecha_hora_solicitud",
            "fecha_hora_cierre",
            "tiempo_entrega_min",
            "cantidad_servicios",
        ]
    ].copy()

    if result["id_servicio"].duplicated().any():
        raise ValueError("La transformación produjo servicios duplicados.")

    return (
        result,
        int(sede_cliente_inconsistente.sum()),
        int(sede_sin_relacion.sum()),
        clientes_desconocidos,
        int((result["sk_mensajero"] == 0).sum()),
    )


def build_ft_fase_servicio(
    estados_normalizados: pd.DataFrame,
    servicios_validos: set[int],
) -> tuple[pd.DataFrame, int]:
    fact = estados_normalizados[
        estados_normalizados["id_servicio"].isin(servicios_validos)
    ].copy()
    fact = fact.sort_values(["id_servicio", "sk_fase"])

    fact["siguiente_fase"] = fact.groupby("id_servicio")["sk_fase"].shift(-1)
    fact["siguiente_timestamp"] = fact.groupby("id_servicio")[
        "fecha_hora_inicio"
    ].shift(-1)

    # Solo se calcula una duración cuando existe la siguiente fase consecutiva.
    consecutive = fact["siguiente_fase"] == (fact["sk_fase"] + 1)
    chronological = fact["siguiente_timestamp"] >= fact["fecha_hora_inicio"]
    valid_end = consecutive & chronological

    # La secuencia es inválida si cualquier fase posterior en el orden del
    # proceso tiene una marca de tiempo anterior, aunque falte una fase intermedia.
    has_next = fact["siguiente_timestamp"].notna()
    invalid_sequence_count = int((has_next & ~chronological).sum())

    fact["fecha_hora_fin"] = fact["siguiente_timestamp"].where(valid_end)
    fact["duracion_fase_min"] = (
        fact["fecha_hora_fin"] - fact["fecha_hora_inicio"]
    ).dt.total_seconds() / 60

    return fact[
        [
            "id_servicio",
            "sk_fase",
            "fecha_hora_inicio",
            "fecha_hora_fin",
            "duracion_fase_min",
        ]
    ], invalid_sequence_count


def build_ft_novedad_servicio(
    novedades: pd.DataFrame,
    servicios_validos: set[int],
    sk_tipo_novedad_map: dict[str, int],
) -> pd.DataFrame:
    fact = novedades[novedades["id_servicio"].isin(servicios_validos)].copy()
    fact["sk_tipo_novedad"] = (
        fact["tipo_novedad_id"]
        .astype(str)
        .map(sk_tipo_novedad_map)
        .fillna(0)
        .astype(int)
    )
    fact["fecha_hora_novedad"] = make_timestamp_naive(fact["fecha_novedad"])
    fact["cantidad_novedades"] = 1

    result = fact[
        [
            "id_novedad_evento",
            "id_servicio",
            "sk_tipo_novedad",
            "fecha_hora_novedad",
            "descripcion",
            "cantidad_novedades",
        ]
    ].copy()

    if result["id_novedad_evento"].duplicated().any():
        raise ValueError("La transformación produjo novedades duplicadas.")

    return result


# ============================================================
# LOAD
# ============================================================

def clear_olap(conn: Connection) -> None:
    print("  Vaciando datos anteriores de la OLAP...")
    conn.execute(
        text(
            """
            TRUNCATE TABLE
                ft_novedad_servicio,
                ft_fase_servicio,
                ft_servicio
            RESTART IDENTITY;
            """
        )
    )
    conn.execute(
        text(
            """
            TRUNCATE TABLE
                dim_sede,
                dim_tipo_novedad,
                dim_mensajero,
                dim_cliente,
                dim_fecha
            RESTART IDENTITY CASCADE;
            """
        )
    )


def insert_unknown_members(conn: Connection) -> None:
    conn.execute(
        text(
            """
            INSERT INTO dim_cliente
                (sk_cliente, id_cliente_nk, nom_cliente)
            VALUES
                (0, 'DESCONOCIDO', 'Cliente desconocido');

            INSERT INTO dim_sede
                (sk_sede, id_sede_nk, sk_cliente, nom_sede, ciudad, direccion)
            VALUES
                (0, 'DESCONOCIDA', 0, 'Sede desconocida', NULL, NULL);

            INSERT INTO dim_mensajero
                (sk_mensajero, id_mensajero_nk, nom_mensajero)
            VALUES
                (0, 'DESCONOCIDO', 'Mensajero desconocido');

            INSERT INTO dim_tipo_novedad
                (sk_tipo_novedad, id_tipo_novedad_nk, nom_tipo_novedad)
            VALUES
                (0, 'DESCONOCIDA', 'Novedad desconocida');
            """
        )
    )


def validate_olap(
    conn: Connection,
    stats: EtlStats,
    source_ids: SourceIds,
) -> None:
    validation = pd.read_sql(
        text(
            """
            SELECT
                (SELECT COUNT(*) FROM ft_servicio) AS servicios,
                (SELECT COUNT(*) FROM ft_fase_servicio) AS fases,
                (SELECT COUNT(*) FROM ft_novedad_servicio) AS novedades,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT id_servicio
                        FROM ft_servicio
                        GROUP BY id_servicio
                        HAVING COUNT(*) > 1
                    ) duplicados
                ) AS servicios_duplicados,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT id_servicio, sk_fase
                        FROM ft_fase_servicio
                        GROUP BY id_servicio, sk_fase
                        HAVING COUNT(*) > 1
                    ) duplicados
                ) AS fases_duplicadas,
                (
                    SELECT COUNT(*)
                    FROM ft_fase_servicio
                    WHERE duracion_fase_min < 0
                ) AS fases_negativas,
                (
                    SELECT COUNT(*)
                    FROM ft_servicio
                    WHERE tiempo_entrega_min < 0
                ) AS entregas_negativas,
                (
                    SELECT COUNT(*)
                    FROM ft_servicio
                    WHERE fecha_hora_cierre < fecha_hora_solicitud
                ) AS cierres_anteriores,
                (
                    SELECT COUNT(*)
                    FROM ft_fase_servicio anterior
                    JOIN ft_fase_servicio posterior
                      ON posterior.id_servicio = anterior.id_servicio
                     AND posterior.sk_fase > anterior.sk_fase
                    WHERE posterior.fecha_hora_inicio < anterior.fecha_hora_inicio
                ) AS fases_fuera_orden,
                (
                    SELECT COUNT(*)
                    FROM ft_servicio fs
                    JOIN dim_sede ds ON ds.sk_sede = fs.sk_sede
                    WHERE fs.sk_sede <> 0
                      AND fs.sk_cliente <> ds.sk_cliente
                ) AS cliente_sede_inconsistente,
                (SELECT COUNT(*) FROM ft_servicio WHERE sk_cliente = 0)
                    AS clientes_desconocidos,
                (SELECT COUNT(*) FROM ft_servicio WHERE sk_sede = 0)
                    AS sedes_desconocidas,
                (SELECT COUNT(*) FROM ft_servicio WHERE sk_mensajero = 0)
                    AS mensajeros_desconocidos,
                (SELECT COUNT(*) FROM ft_novedad_servicio WHERE sk_tipo_novedad = 0)
                    AS tipos_novedad_desconocidos,
                (
                    SELECT COUNT(*)
                    FROM ft_servicio fs
                    LEFT JOIN dim_fecha df ON df.sk_fecha = fs.sk_fecha_solicitud
                    LEFT JOIN dim_hora dh ON dh.id_hora = fs.id_hora_solicitud
                    LEFT JOIN dim_cliente dc ON dc.sk_cliente = fs.sk_cliente
                    LEFT JOIN dim_sede ds ON ds.sk_sede = fs.sk_sede
                    LEFT JOIN dim_mensajero dm ON dm.sk_mensajero = fs.sk_mensajero
                    WHERE df.sk_fecha IS NULL OR dh.id_hora IS NULL
                       OR dc.sk_cliente IS NULL OR ds.sk_sede IS NULL
                       OR dm.sk_mensajero IS NULL
                ) + (
                    SELECT COUNT(*)
                    FROM ft_fase_servicio ffs
                    LEFT JOIN ft_servicio fs ON fs.id_servicio = ffs.id_servicio
                    LEFT JOIN dim_fase df ON df.sk_fase = ffs.sk_fase
                    WHERE fs.id_servicio IS NULL OR df.sk_fase IS NULL
                ) + (
                    SELECT COUNT(*)
                    FROM ft_novedad_servicio fns
                    LEFT JOIN ft_servicio fs ON fs.id_servicio = fns.id_servicio
                    LEFT JOIN dim_tipo_novedad dtn
                      ON dtn.sk_tipo_novedad = fns.sk_tipo_novedad
                    WHERE fs.id_servicio IS NULL OR dtn.sk_tipo_novedad IS NULL
                ) AS claves_foraneas_invalidas,
                (
                    SELECT COUNT(*)
                    FROM ft_servicio
                    WHERE id_servicio IS NULL OR sk_fecha_solicitud IS NULL
                       OR id_hora_solicitud IS NULL OR sk_cliente IS NULL
                       OR sk_sede IS NULL OR sk_mensajero IS NULL
                       OR fecha_hora_solicitud IS NULL OR cantidad_servicios IS NULL
                ) + (
                    SELECT COUNT(*)
                    FROM ft_fase_servicio
                    WHERE id_servicio IS NULL OR sk_fase IS NULL
                       OR fecha_hora_inicio IS NULL
                ) + (
                    SELECT COUNT(*)
                    FROM ft_novedad_servicio
                    WHERE id_novedad_evento IS NULL OR id_servicio IS NULL
                       OR sk_tipo_novedad IS NULL OR cantidad_novedades IS NULL
                ) AS nulos_inesperados,
                (
                    SELECT COUNT(*)
                    FROM (VALUES
                        ((SELECT COUNT(*) FROM dim_cliente WHERE sk_cliente = 0)),
                        ((SELECT COUNT(*) FROM dim_sede WHERE sk_sede = 0)),
                        ((SELECT COUNT(*) FROM dim_mensajero WHERE sk_mensajero = 0)),
                        ((SELECT COUNT(*) FROM dim_tipo_novedad
                          WHERE sk_tipo_novedad = 0))
                    ) AS miembros(cantidad)
                    WHERE cantidad <> 1
                ) AS miembros_desconocidos_invalidos
            """
        ),
        conn,
    ).iloc[0]

    loaded_service_ids = frozenset(
        pd.read_sql(text("SELECT id_servicio FROM ft_servicio"), conn)[
            "id_servicio"
        ].astype(int)
    )
    loaded_novelty_ids = frozenset(
        pd.read_sql(
            text("SELECT id_novedad_evento FROM ft_novedad_servicio"), conn
        )["id_novedad_evento"].astype(int)
    )

    missing_services = source_ids.servicios_reales - loaded_service_ids
    extra_services = loaded_service_ids - source_ids.servicios_reales
    loaded_test_services = loaded_service_ids & source_ids.servicios_prueba
    missing_novelties = source_ids.novedades_validas - loaded_novelty_ids
    extra_novelties = loaded_novelty_ids - source_ids.novedades_validas
    loaded_test_novelties = loaded_novelty_ids & source_ids.novedades_prueba
    loaded_novelties_from_test_services = (
        loaded_novelty_ids & source_ids.novedades_servicio_prueba
    )

    expected_unknown_sites = (
        stats.sedes_cliente_inconsistentes + stats.sedes_sin_relacion
    )

    print("\nValidación final:")
    print(
        f"    Servicios OLTP / OLAP: "
        f"{stats.servicios_fuente:,} / {int(validation['servicios']):,}"
    )
    print(
        f"    Novedades OLTP / OLAP: "
        f"{stats.novedades_fuente:,} / {int(validation['novedades']):,}"
    )
    print(f"    Fases cargadas: {int(validation['fases']):,}")
    print(f"    Servicios duplicados: {int(validation['servicios_duplicados']):,}")
    print(f"    Fases duplicadas: {int(validation['fases_duplicadas']):,}")
    print(f"    Duraciones de fase negativas: {int(validation['fases_negativas']):,}")
    print(f"    Tiempos de entrega negativos: {int(validation['entregas_negativas']):,}")
    print(f"    Cierres anteriores a la solicitud: {int(validation['cierres_anteriores']):,}")
    print(f"    Fases fuera de orden en OLAP: {int(validation['fases_fuera_orden']):,}")
    print(f"    Servicios faltantes / extras: {len(missing_services):,} / {len(extra_services):,}")
    print(f"    Servicios de prueba cargados: {len(loaded_test_services):,}")
    print(f"    Novedades faltantes / extras: {len(missing_novelties):,} / {len(extra_novelties):,}")
    print(f"    Novedades de prueba cargadas: {len(loaded_test_novelties):,}")
    print(
        "    Novedades de servicios de prueba cargadas: "
        f"{len(loaded_novelties_from_test_services):,}"
    )
    print(
        "    Relaciones cliente-sede inválidas conservadas: "
        f"{int(validation['cliente_sede_inconsistente']):,}"
    )
    print(
        "    Sedes enviadas a desconocida por política: "
        f"{int(validation['sedes_desconocidas']):,}"
    )
    print(
        "    Clientes / mensajeros desconocidos: "
        f"{int(validation['clientes_desconocidos']):,} / "
        f"{int(validation['mensajeros_desconocidos']):,}"
    )
    print(f"    Eventos de fase repetidos detectados: {stats.fases_repetidas:,}")
    print(
        "    Secuencias cronológicas inválidas detectadas: "
        f"{stats.fases_con_secuencia_invalida:,}"
    )

    errors: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    require(
        int(validation["servicios"]) == stats.servicios_fuente,
        "La cantidad de servicios cargados no coincide con la fuente.",
    )
    require(
        int(validation["fases"]) == stats.fases_esperadas,
        "La cantidad de fases cargadas no coincide con las fases normalizadas.",
    )
    require(
        int(validation["novedades"]) == stats.novedades_fuente,
        "La cantidad de novedades cargadas no coincide con la fuente válida.",
    )
    require(not missing_services, "Hay identificadores de servicio faltantes.")
    require(not extra_services, "Hay identificadores de servicio extras.")
    require(not loaded_test_services, "Se cargaron servicios de prueba.")
    require(not missing_novelties, "Hay identificadores de novedad faltantes.")
    require(not extra_novelties, "Hay identificadores de novedad extras.")
    require(not loaded_test_novelties, "Se cargaron novedades de prueba.")
    require(
        not loaded_novelties_from_test_services,
        "Se cargaron novedades pertenecientes a servicios de prueba.",
    )

    zero_validations = {
        "servicios_duplicados": "Se encontraron servicios duplicados.",
        "fases_duplicadas": "Se encontraron fases duplicadas por servicio.",
        "fases_negativas": "Se encontraron duraciones de fase negativas.",
        "entregas_negativas": "Se encontraron tiempos de entrega negativos.",
        "cierres_anteriores": "Se encontraron cierres anteriores a la solicitud.",
        "fases_fuera_orden": "Se encontraron fases fuera de orden.",
        "cliente_sede_inconsistente": (
            "Se conservaron relaciones cliente-sede inconsistentes."
        ),
        "clientes_desconocidos": "Se encontraron clientes desconocidos.",
        "tipos_novedad_desconocidos": (
            "Se encontraron tipos de novedad desconocidos."
        ),
        "claves_foraneas_invalidas": "Se encontraron claves foráneas inválidas.",
        "nulos_inesperados": "Se encontraron NULL inesperados.",
        "miembros_desconocidos_invalidos": (
            "Los miembros desconocidos no están definidos exactamente una vez."
        ),
    }
    for field, message in zero_validations.items():
        require(int(validation[field]) == 0, message)

    require(
        int(validation["sedes_desconocidas"]) == expected_unknown_sites,
        "La cantidad de sedes desconocidas no coincide con la política aplicada.",
    )
    require(
        int(validation["mensajeros_desconocidos"])
        == stats.mensajeros_desconocidos,
        "La cantidad de mensajeros desconocidos cambió durante la carga.",
    )
    require(
        stats.fases_con_secuencia_invalida == 0,
        "La fuente contiene secuencias cronológicas inválidas.",
    )

    if errors:
        raise RuntimeError("Validaciones críticas fallidas:\n- " + "\n- ".join(errors))


# ============================================================
# MAIN
# ============================================================

def run() -> None:
    print("=" * 68)
    print(" ETL Fast and Safe: aquitoy_2 -> olap_fastandsafe")
    print("=" * 68)

    oltp_uri, olap_uri = require_configuration()

    oltp = create_engine(oltp_uri, pool_pre_ping=True)
    olap = create_engine(olap_uri, pool_pre_ping=True)

    try:
        print("\n[1/6] Probando conexiones...")
        test_connections(oltp, olap)
        print("    Conexiones correctas.")

        print("\n[2/6] Extrayendo datos de la OLTP...")
        servicios = extract_servicios(oltp)
        estados = extract_estados(oltp)
        novedades = extract_novedades(oltp)
        clientes = extract_clientes(oltp)
        sedes = extract_sedes(oltp)
        usuarios_sede = extract_usuarios_sede(oltp)
        mensajeros = extract_mensajeros(oltp)
        tipos_novedad = extract_tipos_novedad(oltp)
        source_ids = extract_source_ids(oltp)

        print(f"    Servicios: {len(servicios):,}")
        print(f"    Estados: {len(estados):,}")
        print(f"    Novedades: {len(novedades):,}")
        print(f"    Clientes: {len(clientes):,}")
        print(f"    Sedes: {len(sedes):,}")
        print(f"    Mensajeros: {len(mensajeros):,}")
        print(f"    Tipos de novedad: {len(tipos_novedad):,}")

        print("\n[3/6] Construyendo dimensiones...")
        dim_fecha = build_dim_fecha(servicios)
        dim_cliente = build_dim_cliente(clientes)
        dim_mensajero = build_dim_mensajero(mensajeros)
        dim_tipo_novedad = build_dim_tipo_novedad(tipos_novedad)
        estados_normalizados, fases_repetidas = normalize_estados(estados)

        print(f"    dim_fecha: {len(dim_fecha):,}")
        print(f"    dim_cliente: {len(dim_cliente):,}")
        print(f"    dim_mensajero: {len(dim_mensajero):,}")
        print(f"    dim_tipo_novedad: {len(dim_tipo_novedad):,}")

        # Toda la carga se ejecuta en una única transacción. Si algo falla,
        # SQLAlchemy hace rollback y la OLAP no queda parcialmente cargada.
        with olap.begin() as conn:
            print("\n[4/6] Cargando dimensiones...")
            clear_olap(conn)
            insert_unknown_members(conn)

            load_df(conn, "dim_fecha", dim_fecha)
            load_df(conn, "dim_cliente", dim_cliente)
            load_df(conn, "dim_mensajero", dim_mensajero)
            load_df(conn, "dim_tipo_novedad", dim_tipo_novedad)

            sk_cliente_map = read_sk_map(
                conn, "dim_cliente", "id_cliente_nk", "sk_cliente"
            )
            dim_sede = build_dim_sede(sedes, sk_cliente_map)
            load_df(conn, "dim_sede", dim_sede)

            sk_sede_map = read_sk_map(
                conn, "dim_sede", "id_sede_nk", "sk_sede"
            )
            sk_mensajero_map = read_sk_map(
                conn,
                "dim_mensajero",
                "id_mensajero_nk",
                "sk_mensajero",
            )
            sk_tipo_novedad_map = read_sk_map(
                conn,
                "dim_tipo_novedad",
                "id_tipo_novedad_nk",
                "sk_tipo_novedad",
            )

            print("\n[5/6] Construyendo y cargando hechos...")
            (
                ft_servicio,
                sedes_cliente_inconsistentes,
                sedes_sin_relacion,
                clientes_desconocidos,
                mensajeros_desconocidos,
            ) = build_ft_servicio(
                servicios,
                usuarios_sede,
                estados_normalizados,
                sk_cliente_map,
                sk_sede_map,
                sk_mensajero_map,
            )
            if sedes_cliente_inconsistentes:
                print(
                    "    ADVERTENCIA: "
                    f"{sedes_cliente_inconsistentes:,} servicios tienen una sede "
                    "de otro cliente y fueron asignados a Sede desconocida."
                )
            if sedes_sin_relacion:
                print(
                    "    ADVERTENCIA: "
                    f"{sedes_sin_relacion:,} servicios no tienen una relación "
                    "de sede completa y fueron asignados a Sede desconocida."
                )
            servicios_validos = set(ft_servicio["id_servicio"].astype(int))

            ft_fase, secuencias_invalidas = build_ft_fase_servicio(
                estados_normalizados,
                servicios_validos,
            )
            ft_novedad = build_ft_novedad_servicio(
                novedades,
                servicios_validos,
                sk_tipo_novedad_map,
            )

            load_df(conn, "ft_servicio", ft_servicio)
            load_df(conn, "ft_fase_servicio", ft_fase)
            load_df(conn, "ft_novedad_servicio", ft_novedad)

            stats = EtlStats(
                servicios_fuente=len(servicios),
                estados_fuente=len(estados),
                novedades_fuente=len(novedades),
                fases_esperadas=len(ft_fase),
                fases_repetidas=fases_repetidas,
                fases_con_secuencia_invalida=secuencias_invalidas,
                sedes_cliente_inconsistentes=sedes_cliente_inconsistentes,
                sedes_sin_relacion=sedes_sin_relacion,
                clientes_desconocidos=clientes_desconocidos,
                sedes_desconocidas=(
                    sedes_cliente_inconsistentes + sedes_sin_relacion
                ),
                mensajeros_desconocidos=mensajeros_desconocidos,
            )

            print("\n[6/6] Validando la carga...")
            validate_olap(conn, stats, source_ids)

        print("\n" + "=" * 68)
        print(" ETL completado correctamente.")
        print("=" * 68)

    finally:
        oltp.dispose()
        olap.dispose()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"\nERROR: {exc}")
        traceback.print_exc()
        sys.exit(1)
