#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL para la Bodega de Datos OLAP - Fast and Safe
Extrae datos del OLTP (PostgreSQL) y carga en la OLAP

Requisitos:
    pip install pandas sqlalchemy psycopg2-binary

Uso:
    python etl_fastandsafe.py
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import calendar
import sys

# ============================================================
# CONFIGURACION
# ============================================================
OLTP_NAME = "OLTP"
OLAP_NAME = "olap_fastandsafe"
OLTP_URI = f"postgresql://postgres:postgres@localhost:5433/{OLTP_NAME}"
OLAP_URI = f"postgresql://postgres:postgres@localhost:5433/{OLAP_NAME}"

# Mapeo de estados OLTP -> SK_FASE OLAP
ESTADO_TO_FASE = {
    1: 1,   # Iniciado -> Iniciado
    2: 2,   # Con mensajero Asignado -> Con mensajero asignado
    4: 3,   # Recogido por mensajero -> Recogido en origen
    5: 4,   # Entregado en destino -> Entregado en destino
    6: 5,   # Terminado completo -> Cerrado
}

# ============================================================
# EXTRACT
# ============================================================
def extract_servicios(engine):
    return pd.read_sql("""
        SELECT id, cliente_id, mensajero_id, origen_id,
               fecha_solicitud, hora_solicitud
        FROM mensajeria_servicio
        WHERE es_prueba = FALSE
    """, engine)

def extract_estados(engine):
    return pd.read_sql("""
        SELECT es.servicio_id, es.fecha, es.hora, es.estado_id
        FROM mensajeria_estadosservicio es
        WHERE es.es_prueba = FALSE
    """, engine)

def extract_novedades(engine):
    return pd.read_sql("""
        SELECT servicio_id, tipo_novedad_id
        FROM mensajeria_novedadesservicio
        WHERE es_prueba = FALSE
    """, engine)

def extract_clientes(engine):
    return pd.read_sql("SELECT cliente_id, nombre FROM cliente", engine)

def extract_sedes(engine):
    return pd.read_sql("""
        SELECT s.sede_id, s.nombre, s.direccion, s.cliente_id,
               s.ciudad_id, c.nombre AS ciudad
        FROM sede s
        LEFT JOIN ciudad c ON s.ciudad_id = c.ciudad_id
    """, engine)

def extract_mensajeros(engine):
    return pd.read_sql("""
        SELECT DISTINCT m.id, u.username
        FROM clientes_mensajeroaquitoy m
        JOIN auth_user u ON m.user_id = u.id
    """, engine)

def extract_origenes(engine):
    return pd.read_sql("""
        SELECT id, cliente_id, ciudad_id
        FROM mensajeria_origenservicio
    """, engine)

# ============================================================
# TRANSFORM - DIMENSIONES
# ============================================================
def build_dim_fecha(servicios):
    dates = sorted(servicios['fecha_solicitud'].dropna().unique())
    records = []
    for d in dates:
        dt = pd.Timestamp(d)
        records.append({
            'sk_fecha': int(dt.strftime('%Y%m%d')),
            'fecha': dt.date(),
            'dia': dt.day,
            'dia_semana': calendar.day_name[dt.weekday()],
            'dia_semana_num': dt.weekday() + 1,
            'mes': dt.month,
            'mes_nombre': calendar.month_name[dt.month],
            'trimestre': (dt.month - 1) // 3 + 1,
            'anio': dt.year,
            'es_festivo': False
        })
    return pd.DataFrame(records)

def build_dim_cliente(clientes):
    return clientes.rename(columns={
        'cliente_id': 'id_cliente',
        'nombre': 'nom_cliente'
    })[['id_cliente', 'nom_cliente']]

def build_dim_mensajero(mensajeros):
    return pd.DataFrame({
        'id_mensajero_nk': mensajeros['id'].astype(str),
        'nom_mensajero': mensajeros['username']
    })

def build_dim_sede(sedes, sk_cliente_map):
    result = sedes.copy()
    result['sk_cliente'] = result['cliente_id'].astype(str).map(sk_cliente_map)
    result = result.dropna(subset=['sk_cliente'])
    return pd.DataFrame({
        'id_sede_nk': result['sede_id'].astype(str),
        'sk_cliente': result['sk_cliente'].astype(int),
        'nom_sede': result['nombre'],
        'ciudad': result['ciudad'],
        'direccion': result['direccion']
    })

# ============================================================
# TRANSFORM - HECHOS
# ============================================================
def find_sede(row, sk_sede_map):
    cliente_id = row['cliente_id']
    ciudad_id = row.get('ciudad_id')
    if pd.isna(cliente_id):
        return None
    cliente_id = int(cliente_id)
    if cliente_id in sk_sede_map:
        candidates = sk_sede_map[cliente_id]
        if ciudad_id is not None and not pd.isna(ciudad_id):
            ciudad_id = int(ciudad_id)
            if ciudad_id in candidates:
                return candidates[ciudad_id]
        if candidates:
            return list(candidates.values())[0]
    return None

def build_ft_servicios(servicios, estados, novedades, origenes,
                       sk_cliente_map, sk_mensajero_map, sk_sede_map):
    # 1. Mapear estados a fases
    estados = estados.copy()
    estados['sk_fase'] = estados['estado_id'].map(ESTADO_TO_FASE)
    estados = estados.dropna(subset=['sk_fase'])
    estados['sk_fase'] = estados['sk_fase'].astype(int)

    # 2. Timestamp
    estados['ts'] = pd.to_datetime(
        estados['fecha'].astype(str) + ' ' + estados['hora'].astype(str),
        format='mixed'
    )
    estados = estados.sort_values(['servicio_id', 'ts'])

    # 3. duracion_fase_min
    estados['next_ts'] = estados.groupby('servicio_id')['ts'].shift(-1)
    estados['duracion_fase_min'] = (
        (estados['next_ts'] - estados['ts']).dt.total_seconds() / 60
    ).fillna(0).astype(int)

    # 4. Timestamp solicitud
    servicios_ts = servicios[['id', 'fecha_solicitud', 'hora_solicitud']].copy()
    servicios_ts['ts_solicitud'] = pd.to_datetime(
        servicios_ts['fecha_solicitud'].astype(str) + ' '
        + servicios_ts['hora_solicitud'].astype(str),
        format='mixed'
    )

    # 5. Tiempo total
    cierre = estados[estados['sk_fase'] == 5][['servicio_id', 'ts']].rename(
        columns={'ts': 'ts_cierre'}
    )
    cierre = cierre.drop_duplicates(subset='servicio_id', keep='last')
    servicios_ts = servicios_ts.merge(
        cierre, left_on='id', right_on='servicio_id', how='left'
    )
    servicios_ts['tiempo_entrega_min'] = (
        (servicios_ts['ts_cierre'] - servicios_ts['ts_solicitud'])
        .dt.total_seconds() / 60
    ).fillna(0).astype(int)

    # 6. Novedades
    novedad_count = novedades.groupby('servicio_id').size().reset_index(
        name='cantidad_novedades'
    )
    novedad_first = novedades.groupby('servicio_id')['tipo_novedad_id'].first().reset_index()
    novedad_first.columns = ['servicio_id', 'tipo_novedad_id']

    # 7. Sede
    servicio_sede = servicios[['id', 'cliente_id', 'origen_id']].merge(
        origenes[['id', 'ciudad_id']],
        left_on='origen_id', right_on='id',
        how='left', suffixes=('', '_origen')
    )
    servicio_sede = servicio_sede.rename(columns={'id': 'id_servicio'})
    servicio_sede['sk_sede'] = servicio_sede.apply(
        lambda r: find_sede(r, sk_sede_map), axis=1
    )

    # 8. Construir hechos
    fact = estados[['servicio_id', 'sk_fase', 'duracion_fase_min']].copy()
    fact = fact.rename(columns={'servicio_id': 'id_servicio'})
    fact = fact.merge(
        servicios[['id', 'cliente_id', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud']],
        left_on='id_servicio', right_on='id', how='left'
    )
    fact = fact.drop(columns=['id'], errors='ignore')

    fact['sk_fecha'] = pd.to_datetime(fact['fecha_solicitud'], errors='coerce').dt.strftime('%Y%m%d')
    fact['sk_fecha'] = fact['sk_fecha'].fillna('0').astype(int)
    fact['id_hora'] = pd.to_datetime(fact['hora_solicitud'].fillna('00:00:00').astype(str), format='mixed').dt.hour
    fact['sk_cliente'] = fact['cliente_id'].apply(
        lambda x: '' if pd.isna(x) else str(int(x))
    ).map(sk_cliente_map)
    fact['sk_mensajero'] = fact['mensajero_id'].apply(
        lambda x: '0' if pd.isna(x) else str(int(float(x)))
    ).map(sk_mensajero_map)

    fact = fact.merge(servicio_sede[['id_servicio', 'sk_sede']], on='id_servicio', how='left')
    fact = fact.merge(servicios_ts[['id', 'tiempo_entrega_min']], left_on='id_servicio', right_on='id', how='left', suffixes=('', '_ts'))
    fact = fact.drop(columns=['id_ts'], errors='ignore')
    fact = fact.merge(novedad_count, left_on='id_servicio', right_on='servicio_id', how='left', suffixes=('', '_nov'))
    fact = fact.merge(novedad_first, left_on='id_servicio', right_on='servicio_id', how='left', suffixes=('', '_nftype'))

    # Map tipo_novedad_id to sk_novedad (OLTP tipo 1 -> dim SK 1, tipo 2 -> dim SK 2)
    TIPO_TO_SK_NOVEDAD = {1: 1, 2: 2}
    fact['sk_novedad'] = fact['tipo_novedad_id'].map(TIPO_TO_SK_NOVEDAD).fillna(0).astype(int)

    fact['cantidad_servicios'] = 1
    fact['cantidad_novedades'] = fact['cantidad_novedades'].fillna(0).astype(int)
    fact['tiempo_entrega_min'] = fact['tiempo_entrega_min'].fillna(0).astype(int)

    result = fact[[
        'sk_fecha', 'id_hora', 'sk_cliente', 'sk_sede', 'sk_mensajero',
        'sk_fase', 'sk_novedad', 'cantidad_servicios', 'cantidad_novedades',
        'tiempo_entrega_min', 'duracion_fase_min'
    ]].copy()

    result = result.dropna(subset=['sk_cliente', 'sk_sede', 'sk_mensajero'])
    result['sk_cliente'] = result['sk_cliente'].astype(int)
    result['sk_sede'] = result['sk_sede'].astype(int)
    result['sk_mensajero'] = result['sk_mensajero'].astype(int)

    return result

# ============================================================
# LOAD
# ============================================================
def truncate_all(engine):
    print("  Vaciando tablas OLAP...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE ft_servicios CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_sede CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_cliente CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_mensajero CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_fecha CASCADE"))
        conn.execute(text("ALTER TABLE dim_mensajero ALTER COLUMN sk_mensajero DROP IDENTITY IF EXISTS"))
        conn.execute(text("INSERT INTO dim_mensajero (sk_mensajero, id_mensajero_nk, nom_mensajero) VALUES (0, '0', 'Desconocido')"))
        conn.execute(text("ALTER TABLE dim_mensajero ALTER COLUMN sk_mensajero ADD GENERATED ALWAYS AS IDENTITY"))
        conn.commit()

def load_df(engine, table_name, df):
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"    {table_name}: {len(df)} registros")

def read_sk(engine, table, natural_key_col, sk_col):
    df = pd.read_sql(
        f"SELECT {sk_col}, {natural_key_col} FROM {table}", engine
    )
    return dict(zip(df[natural_key_col], df[sk_col]))

def read_sede_map(engine, sedes_orig):
    sk_df = pd.read_sql("SELECT sk_sede, id_sede_nk FROM dim_sede", engine)
    nk_to_sk = dict(zip(sk_df['id_sede_nk'].astype(int), sk_df['sk_sede']))
    sede_map = {}
    for _, row in sedes_orig.iterrows():
        cliente_id = row['cliente_id']
        ciudad_id = row.get('ciudad_id')
        sk = nk_to_sk.get(row['sede_id'])
        if sk is not None and not pd.isna(cliente_id):
            cliente_id = int(cliente_id)
            if cliente_id not in sede_map:
                sede_map[cliente_id] = {}
            if not pd.isna(ciudad_id):
                sede_map[cliente_id][int(ciudad_id)] = sk
            else:
                sede_map[cliente_id][None] = sk
    return sede_map

# ============================================================
# MAIN
# ============================================================
def run():
    print("=" * 60)
    print(" ETL Fast and Safe - Bodega de Datos OLAP")
    print("=" * 60)

    try:
        oltp = create_engine(OLTP_URI)
        olap = create_engine(OLAP_URI, isolation_level="AUTOCOMMIT")

        # 1. Extract
        print("\n[1/5] Extrayendo datos del OLTP...")
        servicios = extract_servicios(oltp)
        estados = extract_estados(oltp)
        novedades = extract_novedades(oltp)
        clientes = extract_clientes(oltp)
        sedes = extract_sedes(oltp)
        mensajeros = extract_mensajeros(oltp)
        origenes = extract_origenes(oltp)
        print(f"    Servicios: {len(servicios)}")
        print(f"    Estados: {len(estados)}")
        print(f"    Novedades: {len(novedades)}")
        print(f"    Clientes: {len(clientes)}")
        print(f"    Sedes: {len(sedes)}")
        print(f"    Mensajeros: {len(mensajeros)}")

        # 2. Build dimensiones
        print("\n[2/5] Construyendo dimensiones...")
        dim_fecha = build_dim_fecha(servicios)
        dim_cliente = build_dim_cliente(clientes)
        dim_mensajero = build_dim_mensajero(mensajeros)
        print(f"    dim_fecha: {len(dim_fecha)} registros")
        print(f"    dim_cliente: {len(dim_cliente)} registros")
        print(f"    dim_mensajero: {len(dim_mensajero)} registros")

        # 3. Load dimensiones
        print("\n[3/5] Cargando dimensiones...")
        truncate_all(olap)
        load_df(olap, 'dim_fecha', dim_fecha)
        load_df(olap, 'dim_cliente', dim_cliente)
        load_df(olap, 'dim_mensajero', dim_mensajero)

        sk_cliente_map = read_sk(olap, 'dim_cliente', 'id_cliente', 'sk_cliente')
        sk_mensajero_map = read_sk(olap, 'dim_mensajero', 'id_mensajero_nk', 'sk_mensajero')

        dim_sede = build_dim_sede(sedes, sk_cliente_map)
        load_df(olap, 'dim_sede', dim_sede)

        sk_sede_map = read_sede_map(olap, sedes)

        # 4. Build hechos
        print("\n[4/5] Construyendo tabla de hechos...")
        ft = build_ft_servicios(servicios, estados, novedades, origenes,
                                sk_cliente_map, sk_mensajero_map, sk_sede_map)
        print(f"    ft_servicios: {len(ft)} registros")

        # 5. Load hechos
        print("\n[5/5] Cargando tabla de hechos...")
        load_df(olap, 'ft_servicios', ft)

        print("\n" + "=" * 60)
        print("  ETL completado exitosamente!")
        print("=" * 60)

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    run()
