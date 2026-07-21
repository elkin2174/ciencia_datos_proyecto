-- ============================================================
-- FAST AND SAFE
-- Validaciones cruzadas OLTP / OLAP
-- Ejecutar con psql dentro del contenedor fastsafe-postgres.
-- Este archivo solo lee aquitoy_2 y crea tablas temporales en la sesión.
-- ============================================================

\set ON_ERROR_STOP on
\pset pager off

-- ------------------------------------------------------------
-- 1. Métricas e identificadores de referencia desde la OLTP
-- Las variables de psql sobreviven al cambio de base de datos.
-- ------------------------------------------------------------
\connect aquitoy_2

SELECT
    COUNT(*) FILTER (WHERE es_prueba = FALSE)::text
        AS oltp_servicios_reales,
    COALESCE(
        array_agg(id::bigint ORDER BY id) FILTER (WHERE es_prueba = FALSE),
        '{}'::bigint[]
    )::text AS oltp_servicios_reales_ids,
    COALESCE(
        array_agg(id::bigint ORDER BY id) FILTER (WHERE es_prueba = TRUE),
        '{}'::bigint[]
    )::text AS oltp_servicios_prueba_ids,
    COUNT(*) FILTER (
        WHERE es_prueba = FALSE AND mensajero_id IS NULL
    )::text AS oltp_mensajeros_ausentes
FROM mensajeria_servicio
\gset

SELECT
    COUNT(*) FILTER (
        WHERE n.es_prueba = FALSE AND s.es_prueba = FALSE
    )::text AS oltp_novedades_validas,
    COALESCE(
        array_agg(n.id::bigint ORDER BY n.id) FILTER (
            WHERE n.es_prueba = FALSE AND s.es_prueba = FALSE
        ),
        '{}'::bigint[]
    )::text AS oltp_novedades_validas_ids,
    COALESCE(
        array_agg(n.id::bigint ORDER BY n.id) FILTER (
            WHERE n.es_prueba = TRUE
        ),
        '{}'::bigint[]
    )::text AS oltp_novedades_prueba_ids,
    COALESCE(
        array_agg(n.id::bigint ORDER BY n.id) FILTER (
            WHERE s.es_prueba = TRUE
        ),
        '{}'::bigint[]
    )::text AS oltp_novedades_servicio_prueba_ids
FROM mensajeria_novedadesservicio n
JOIN mensajeria_servicio s
    ON s.id = n.servicio_id
\gset

SELECT
    COUNT(*)::text AS oltp_fases_mapeadas,
    COUNT(DISTINCT (e.servicio_id, e.estado_id))::text
        AS oltp_fases_unicas,
    (
        COUNT(*) - COUNT(DISTINCT (e.servicio_id, e.estado_id))
    )::text AS oltp_fases_repetidas
FROM mensajeria_estadosservicio e
JOIN mensajeria_servicio s
    ON s.id = e.servicio_id
WHERE e.es_prueba = FALSE
  AND s.es_prueba = FALSE
  AND e.estado_id IN (1, 2, 4, 5, 6)
\gset

SELECT
    COUNT(*) FILTER (
        WHERE u.id IS NOT NULL
          AND s.cliente_id IS DISTINCT FROM u.cliente_id
    )::text AS oltp_sedes_cliente_inconsistentes,
    COUNT(*) FILTER (
        WHERE u.id IS NULL OR u.sede_id IS NULL OR u.cliente_id IS NULL
    )::text AS oltp_sedes_sin_relacion,
    COALESCE(
        array_agg(s.id::bigint ORDER BY s.id) FILTER (
            WHERE u.id IS NOT NULL
              AND s.cliente_id IS DISTINCT FROM u.cliente_id
        ),
        '{}'::bigint[]
    )::text AS oltp_servicios_sede_inconsistente_ids
FROM mensajeria_servicio s
LEFT JOIN clientes_usuarioaquitoy u
    ON u.id = s.usuario_id
WHERE s.es_prueba = FALSE
\gset


-- ------------------------------------------------------------
-- 2. Validaciones sobre la bodega
-- ------------------------------------------------------------
\connect olap_fastandsafe

CREATE TEMP TABLE resultados_validacion (
    orden       INTEGER NOT NULL,
    validacion  TEXT NOT NULL,
    resultado   TEXT NOT NULL,
    casos       BIGINT NOT NULL,
    estado      TEXT NOT NULL CHECK (estado IN ('OK', 'REVISAR'))
);

INSERT INTO resultados_validacion
SELECT
    1,
    'Conteo de servicios reales en OLTP',
    :'oltp_servicios_reales' || ' servicios reales',
    :'oltp_servicios_reales'::bigint,
    CASE WHEN :'oltp_servicios_reales'::bigint > 0 THEN 'OK' ELSE 'REVISAR' END;

INSERT INTO resultados_validacion
SELECT
    2,
    'Conteo de servicios en OLAP',
    COUNT(*)::text || ' servicios cargados',
    COUNT(*),
    CASE
        WHEN COUNT(*) = :'oltp_servicios_reales'::bigint THEN 'OK'
        ELSE 'REVISAR'
    END
FROM ft_servicio;

INSERT INTO resultados_validacion
SELECT
    3,
    'Comparación de conteos OLTP contra OLAP',
    'OLTP=' || :'oltp_servicios_reales'
        || ', OLAP=' || COUNT(*)::text,
    ABS(COUNT(*) - :'oltp_servicios_reales'::bigint),
    CASE
        WHEN COUNT(*) = :'oltp_servicios_reales'::bigint THEN 'OK'
        ELSE 'REVISAR'
    END
FROM ft_servicio;

INSERT INTO resultados_validacion
SELECT
    4,
    'Duplicados en ft_servicio',
    'Identificadores duplicados',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM (
    SELECT id_servicio
    FROM ft_servicio
    GROUP BY id_servicio
    HAVING COUNT(*) > 1
) duplicados;

INSERT INTO resultados_validacion
SELECT
    5,
    'Duplicados por servicio y fase',
    'Combinaciones id_servicio + sk_fase duplicadas',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM (
    SELECT id_servicio, sk_fase
    FROM ft_fase_servicio
    GROUP BY id_servicio, sk_fase
    HAVING COUNT(*) > 1
) duplicados;

INSERT INTO resultados_validacion
SELECT
    6,
    'Servicios faltantes en OLAP',
    'IDs reales de OLTP no encontrados en ft_servicio',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM unnest(:'oltp_servicios_reales_ids'::bigint[]) fuente(id_servicio)
LEFT JOIN ft_servicio destino
    ON destino.id_servicio = fuente.id_servicio
WHERE destino.id_servicio IS NULL;

INSERT INTO resultados_validacion
SELECT
    7,
    'Servicios extras en OLAP',
    'IDs de ft_servicio que no son servicios reales de OLTP',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_servicio
WHERE NOT (id_servicio = ANY(:'oltp_servicios_reales_ids'::bigint[]));

INSERT INTO resultados_validacion
SELECT
    8,
    'Claves foráneas inválidas',
    'Hechos sin una dimensión o servicio relacionado',
    SUM(casos),
    CASE WHEN SUM(casos) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM (
    SELECT COUNT(*) AS casos
    FROM ft_servicio fs
    LEFT JOIN dim_fecha df ON df.sk_fecha = fs.sk_fecha_solicitud
    LEFT JOIN dim_hora dh ON dh.id_hora = fs.id_hora_solicitud
    LEFT JOIN dim_cliente dc ON dc.sk_cliente = fs.sk_cliente
    LEFT JOIN dim_sede ds ON ds.sk_sede = fs.sk_sede
    LEFT JOIN dim_mensajero dm ON dm.sk_mensajero = fs.sk_mensajero
    WHERE df.sk_fecha IS NULL OR dh.id_hora IS NULL
       OR dc.sk_cliente IS NULL OR ds.sk_sede IS NULL
       OR dm.sk_mensajero IS NULL
    UNION ALL
    SELECT COUNT(*)
    FROM ft_fase_servicio ffs
    LEFT JOIN ft_servicio fs ON fs.id_servicio = ffs.id_servicio
    LEFT JOIN dim_fase df ON df.sk_fase = ffs.sk_fase
    WHERE fs.id_servicio IS NULL OR df.sk_fase IS NULL
    UNION ALL
    SELECT COUNT(*)
    FROM ft_novedad_servicio fns
    LEFT JOIN ft_servicio fs ON fs.id_servicio = fns.id_servicio
    LEFT JOIN dim_tipo_novedad dtn
        ON dtn.sk_tipo_novedad = fns.sk_tipo_novedad
    WHERE fs.id_servicio IS NULL OR dtn.sk_tipo_novedad IS NULL
) controles;

INSERT INTO resultados_validacion
SELECT
    9,
    'Clientes desconocidos',
    'Servicios con sk_cliente = 0',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_servicio
WHERE sk_cliente = 0;

INSERT INTO resultados_validacion
SELECT
    10,
    'Sedes desconocidas',
    'Esperadas por política=' || (
        :'oltp_sedes_cliente_inconsistentes'::bigint
        + :'oltp_sedes_sin_relacion'::bigint
    )::text || ', cargadas=' || COUNT(*)::text,
    COUNT(*),
    CASE
        WHEN COUNT(*) = (
            :'oltp_sedes_cliente_inconsistentes'::bigint
            + :'oltp_sedes_sin_relacion'::bigint
        ) THEN 'OK'
        ELSE 'REVISAR'
    END
FROM ft_servicio
WHERE sk_sede = 0;

INSERT INTO resultados_validacion
SELECT
    11,
    'Mensajeros desconocidos',
    'Esperados por mensajero_id NULL=' || :'oltp_mensajeros_ausentes'
        || ', cargados=' || COUNT(*)::text,
    COUNT(*),
    CASE
        WHEN COUNT(*) = :'oltp_mensajeros_ausentes'::bigint THEN 'OK'
        ELSE 'REVISAR'
    END
FROM ft_servicio
WHERE sk_mensajero = 0;

INSERT INTO resultados_validacion
WITH invalidas_conservadas AS (
    SELECT COUNT(*) AS casos
    FROM ft_servicio fs
    JOIN dim_sede ds ON ds.sk_sede = fs.sk_sede
    WHERE fs.sk_sede <> 0
      AND fs.sk_cliente <> ds.sk_cliente
),
politica_no_aplicada AS (
    SELECT COUNT(*) AS casos
    FROM unnest(
        :'oltp_servicios_sede_inconsistente_ids'::bigint[]
    ) fuente(id_servicio)
    JOIN ft_servicio fs ON fs.id_servicio = fuente.id_servicio
    WHERE fs.sk_sede <> 0
)
SELECT
    12,
    'Consistencia cliente-sede',
    'Fuente inconsistentes=' || :'oltp_sedes_cliente_inconsistentes'
        || ', relaciones inválidas conservadas='
        || invalidas_conservadas.casos::text
        || ', política no aplicada=' || politica_no_aplicada.casos::text,
    invalidas_conservadas.casos + politica_no_aplicada.casos,
    CASE
        WHEN invalidas_conservadas.casos = 0
         AND politica_no_aplicada.casos = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM invalidas_conservadas, politica_no_aplicada;

INSERT INTO resultados_validacion
SELECT
    13,
    'Duraciones negativas',
    'Fases con duracion_fase_min < 0',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_fase_servicio
WHERE duracion_fase_min < 0;

INSERT INTO resultados_validacion
SELECT
    14,
    'Cierres anteriores a la solicitud',
    'Servicios con cierre cronológicamente inválido',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_servicio
WHERE fecha_hora_cierre < fecha_hora_solicitud;

INSERT INTO resultados_validacion
SELECT
    15,
    'Fases cronológicamente inválidas',
    'Pares donde una fase posterior empieza antes que una anterior',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_fase_servicio anterior
JOIN ft_fase_servicio posterior
    ON posterior.id_servicio = anterior.id_servicio
   AND posterior.sk_fase > anterior.sk_fase
WHERE posterior.fecha_hora_inicio < anterior.fecha_hora_inicio;

INSERT INTO resultados_validacion
SELECT
    16,
    'Novedades de prueba cargadas',
    'IDs con n.es_prueba = TRUE presentes en OLAP',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_novedad_servicio
WHERE id_novedad_evento = ANY(:'oltp_novedades_prueba_ids'::bigint[]);

INSERT INTO resultados_validacion
SELECT
    17,
    'Servicios de prueba cargados',
    'IDs con s.es_prueba = TRUE presentes en OLAP',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_servicio
WHERE id_servicio = ANY(:'oltp_servicios_prueba_ids'::bigint[]);

INSERT INTO resultados_validacion
SELECT
    18,
    'Novedades asociadas a servicios de prueba',
    'Eventos cuyo servicio fuente es de prueba presentes en OLAP',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_novedad_servicio
WHERE id_novedad_evento = ANY(
    :'oltp_novedades_servicio_prueba_ids'::bigint[]
);

INSERT INTO resultados_validacion
SELECT
    19,
    'NULL inesperados',
    'NULL en claves o campos obligatorios de los hechos',
    SUM(casos),
    CASE WHEN SUM(casos) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM (
    SELECT COUNT(*) AS casos
    FROM ft_servicio
    WHERE id_servicio IS NULL OR sk_fecha_solicitud IS NULL
       OR id_hora_solicitud IS NULL OR sk_cliente IS NULL
       OR sk_sede IS NULL OR sk_mensajero IS NULL
       OR fecha_hora_solicitud IS NULL OR cantidad_servicios IS NULL
    UNION ALL
    SELECT COUNT(*)
    FROM ft_fase_servicio
    WHERE id_servicio IS NULL OR sk_fase IS NULL
       OR fecha_hora_inicio IS NULL
    UNION ALL
    SELECT COUNT(*)
    FROM ft_novedad_servicio
    WHERE id_novedad_evento IS NULL OR id_servicio IS NULL
       OR sk_tipo_novedad IS NULL OR cantidad_novedades IS NULL
) controles;

INSERT INTO resultados_validacion
WITH conteos AS (
    SELECT
        (SELECT COUNT(*) FROM dim_fecha) AS dim_fecha,
        (SELECT COUNT(*) FROM dim_hora) AS dim_hora,
        (SELECT COUNT(*) FROM dim_cliente) AS dim_cliente,
        (SELECT COUNT(*) FROM dim_sede) AS dim_sede,
        (SELECT COUNT(*) FROM dim_mensajero) AS dim_mensajero,
        (SELECT COUNT(*) FROM dim_fase) AS dim_fase,
        (SELECT COUNT(*) FROM dim_tipo_novedad) AS dim_tipo_novedad,
        (SELECT COUNT(*) FROM ft_servicio) AS ft_servicio,
        (SELECT COUNT(*) FROM ft_fase_servicio) AS ft_fase_servicio,
        (SELECT COUNT(*) FROM ft_novedad_servicio) AS ft_novedad_servicio
)
SELECT
    20,
    'Conteos de dimensiones y hechos',
    'dim_fecha=' || dim_fecha || ', dim_hora=' || dim_hora
        || ', dim_cliente=' || dim_cliente || ', dim_sede=' || dim_sede
        || ', dim_mensajero=' || dim_mensajero || ', dim_fase=' || dim_fase
        || ', dim_tipo_novedad=' || dim_tipo_novedad
        || ', ft_servicio=' || ft_servicio
        || ', ft_fase_servicio=' || ft_fase_servicio
        || ', ft_novedad_servicio=' || ft_novedad_servicio,
    0,
    CASE
        WHEN ft_servicio = :'oltp_servicios_reales'::bigint
         AND ft_fase_servicio = :'oltp_fases_unicas'::bigint
         AND ft_novedad_servicio = :'oltp_novedades_validas'::bigint
         AND dim_fecha > 0 AND dim_hora = 24 AND dim_cliente > 0
         AND dim_sede > 0 AND dim_mensajero > 0 AND dim_fase = 5
         AND dim_tipo_novedad > 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM conteos;

INSERT INTO resultados_validacion
SELECT
    21,
    'Repeticiones servicio-fase normalizadas',
    'Eventos mapeados=' || :'oltp_fases_mapeadas'
        || ', únicos=' || :'oltp_fases_unicas'
        || ', repeticiones retiradas=' || :'oltp_fases_repetidas'
        || ', cargados=' || COUNT(*)::text,
    ABS(COUNT(*) - :'oltp_fases_unicas'::bigint),
    CASE
        WHEN COUNT(*) = :'oltp_fases_unicas'::bigint THEN 'OK'
        ELSE 'REVISAR'
    END
FROM ft_fase_servicio;

INSERT INTO resultados_validacion
SELECT
    22,
    'Cobertura exacta de novedades válidas',
    'Válidas OLTP=' || :'oltp_novedades_validas'
        || ', faltantes=' || COUNT(*)::text,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM unnest(:'oltp_novedades_validas_ids'::bigint[]) fuente(id_novedad_evento)
LEFT JOIN ft_novedad_servicio destino
    ON destino.id_novedad_evento = fuente.id_novedad_evento
WHERE destino.id_novedad_evento IS NULL;

INSERT INTO resultados_validacion
SELECT
    23,
    'Novedades extras en OLAP',
    'IDs no pertenecientes al conjunto válido de OLTP',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'REVISAR' END
FROM ft_novedad_servicio
WHERE NOT (
    id_novedad_evento = ANY(:'oltp_novedades_validas_ids'::bigint[])
);

INSERT INTO resultados_validacion
SELECT
    24,
    'Tamaño de muestra - ' || df.nom_fase,
    'Observaciones con duración calculable',
    COUNT(ffs.duracion_fase_min),
    CASE
        WHEN COUNT(ffs.duracion_fase_min) >= 30 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM dim_fase df
LEFT JOIN ft_fase_servicio ffs
    ON ffs.sk_fase = df.sk_fase
   AND ffs.duracion_fase_min IS NOT NULL
GROUP BY df.sk_fase, df.nom_fase;


-- ------------------------------------------------------------
-- 3. Informe final de validaciones
-- REVISAR es esperado únicamente en las fases con muestra insuficiente.
-- ------------------------------------------------------------
SELECT
    validacion,
    resultado,
    casos,
    estado
FROM resultados_validacion
ORDER BY orden, validacion;
