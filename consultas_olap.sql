-- ============================================================
-- FAST AND SAFE
-- Consultas OLAP para las 9 preguntas del proyecto
-- Base: olap_fastandsafe
-- ============================================================

\pset pager off
\timing on

-- ------------------------------------------------------------
-- Validación rápida antes de ejecutar las consultas
-- ------------------------------------------------------------

SELECT 'ft_servicio' AS tabla, COUNT(*) AS registros
FROM ft_servicio
UNION ALL
SELECT 'ft_fase_servicio', COUNT(*)
FROM ft_fase_servicio
UNION ALL
SELECT 'ft_novedad_servicio', COUNT(*)
FROM ft_novedad_servicio
ORDER BY tabla;


-- ============================================================
-- P1. ¿En qué meses del año se solicitan más servicios?
-- ============================================================

SELECT
    df.mes,
    df.mes_nombre,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
GROUP BY
    df.mes,
    df.mes_nombre
ORDER BY
    cantidad_servicios DESC,
    df.mes;


-- ============================================================
-- P2. ¿Cuáles son los días de la semana con más solicitudes?
-- ============================================================

SELECT
    df.dia_semana_num,
    df.dia_semana,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
GROUP BY
    df.dia_semana_num,
    df.dia_semana
ORDER BY
    cantidad_servicios DESC,
    df.dia_semana_num;


-- ============================================================
-- P3. ¿A qué hora los mensajeros reciben más asignaciones?
--
-- Se usa el inicio de la fase "Con mensajero asignado",
-- no la hora de solicitud del servicio.
-- ============================================================

-- P3. Horas en las que se solicitan más servicios
SELECT
    dh.hora_24,
    dh.franja_horaria,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_hora dh
    ON dh.id_hora = fs.id_hora_solicitud
GROUP BY
    dh.hora_24,
    dh.franja_horaria
ORDER BY
    cantidad_servicios DESC,
    dh.hora_24;

-- ============================================================
-- P4. Número de servicios solicitados por cliente y por mes
-- ============================================================

SELECT
    dc.nom_cliente,
    df.anio,
    df.mes,
    df.mes_nombre,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_cliente dc
    ON dc.sk_cliente = fs.sk_cliente
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
WHERE fs.sk_cliente <> 0
GROUP BY
    dc.nom_cliente,
    df.anio,
    df.mes,
    df.mes_nombre
ORDER BY
    dc.nom_cliente,
    df.anio,
    df.mes;


-- ============================================================
-- P5. Mensajeros con mayor cantidad de servicios prestados
-- ============================================================

-- P5. Mensajeros con mayor cantidad de servicios
SELECT
    dm.id_mensajero_nk,
    dm.nom_mensajero,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_mensajero dm
    ON dm.sk_mensajero = fs.sk_mensajero
WHERE fs.sk_mensajero <> 0
GROUP BY
    dm.id_mensajero_nk,
    dm.nom_mensajero
ORDER BY
    cantidad_servicios DESC,
    dm.id_mensajero_nk;

-- ============================================================
-- P6. Sede que más servicios solicita para cada cliente
-- ============================================================

WITH servicios_por_sede AS (
    SELECT
        dc.nom_cliente,
        ds.nom_sede,
        ds.ciudad,
        COUNT(*) AS cantidad_servicios
    FROM ft_servicio fs
    JOIN dim_cliente dc
        ON dc.sk_cliente = fs.sk_cliente
    JOIN dim_sede ds
        ON ds.sk_sede = fs.sk_sede
    WHERE fs.sk_cliente <> 0
      AND fs.sk_sede <> 0
    GROUP BY
        dc.nom_cliente,
        ds.nom_sede,
        ds.ciudad
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY nom_cliente
            ORDER BY cantidad_servicios DESC
        ) AS posicion
    FROM servicios_por_sede
)
SELECT
    nom_cliente,
    nom_sede,
    ciudad,
    cantidad_servicios
FROM ranking
WHERE posicion = 1
ORDER BY
    nom_cliente,
    nom_sede;


-- ============================================================
-- P7. Tiempo promedio desde la solicitud hasta el cierre
-- ============================================================

SELECT
    ROUND(AVG(tiempo_entrega_min), 2) AS promedio_minutos,
    ROUND(AVG(tiempo_entrega_min) / 60.0, 2) AS promedio_horas,
    COUNT(*) AS servicios_cerrados
FROM ft_servicio
WHERE fecha_hora_cierre IS NOT NULL
  AND tiempo_entrega_min IS NOT NULL;


-- ============================================================
-- P8. Tiempo promedio por fase
-- ============================================================

SELECT
    dfase.sk_fase,
    dfase.nom_fase,
    ROUND(AVG(ffs.duracion_fase_min), 2) AS promedio_minutos,
    COUNT(*) AS fases_con_duracion
FROM ft_fase_servicio ffs
JOIN dim_fase dfase
    ON dfase.sk_fase = ffs.sk_fase
WHERE ffs.duracion_fase_min IS NOT NULL
GROUP BY
    dfase.sk_fase,
    dfase.nom_fase
ORDER BY
    promedio_minutos DESC,
    dfase.sk_fase;


-- Fase con mayor demora promedio
SELECT
    dfase.nom_fase,
    ROUND(AVG(ffs.duracion_fase_min), 2) AS promedio_minutos
FROM ft_fase_servicio ffs
JOIN dim_fase dfase
    ON dfase.sk_fase = ffs.sk_fase
WHERE ffs.duracion_fase_min IS NOT NULL
GROUP BY
    dfase.nom_fase
ORDER BY
    promedio_minutos DESC
LIMIT 1;


-- ============================================================
-- P9. Novedades más frecuentes
--
-- La fuente solo contiene 7 novedades válidas asociadas
-- a servicios reales, por lo que el resultado tiene una
-- muestra pequeña.
-- ============================================================

SELECT
    dtn.nom_tipo_novedad,
    COUNT(*) AS cantidad_novedades
FROM ft_novedad_servicio fns
JOIN dim_tipo_novedad dtn
    ON dtn.sk_tipo_novedad = fns.sk_tipo_novedad
GROUP BY
    dtn.nom_tipo_novedad
ORDER BY
    cantidad_novedades DESC,
    dtn.nom_tipo_novedad;-- ============================================================
-- FAST AND SAFE
-- Consultas OLAP para las 9 preguntas del proyecto
-- Base: olap_fastandsafe
-- ============================================================

\pset pager off
\timing on

-- ------------------------------------------------------------
-- Validación rápida antes de ejecutar las consultas
-- ------------------------------------------------------------

SELECT 'ft_servicio' AS tabla, COUNT(*) AS registros
FROM ft_servicio
UNION ALL
SELECT 'ft_fase_servicio', COUNT(*)
FROM ft_fase_servicio
UNION ALL
SELECT 'ft_novedad_servicio', COUNT(*)
FROM ft_novedad_servicio
ORDER BY tabla;


-- ============================================================
-- P1. ¿En qué meses del año se solicitan más servicios?
-- ============================================================

SELECT
    df.mes,
    df.mes_nombre,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
GROUP BY
    df.mes,
    df.mes_nombre
ORDER BY
    cantidad_servicios DESC,
    df.mes;


-- ============================================================
-- P2. ¿Cuáles son los días de la semana con más solicitudes?
-- ============================================================

SELECT
    df.dia_semana_num,
    df.dia_semana,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
GROUP BY
    df.dia_semana_num,
    df.dia_semana
ORDER BY
    cantidad_servicios DESC,
    df.dia_semana_num;


-- ============================================================
-- P3. ¿A qué hora los mensajeros reciben más asignaciones?
--
-- Se usa el inicio de la fase "Con mensajero asignado",
-- no la hora de solicitud del servicio.
-- ============================================================

SELECT
    dh.hora_24,
    dh.franja_horaria,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_hora dh
    ON dh.id_hora = fs.id_hora_solicitud
WHERE fs.sk_mensajero <> 0
GROUP BY
    dh.hora_24,
    dh.franja_horaria
ORDER BY
    cantidad_servicios DESC,
    dh.hora_24;


-- ============================================================
-- P4. Número de servicios solicitados por cliente y por mes
-- ============================================================

SELECT
    dc.nom_cliente,
    df.anio,
    df.mes,
    df.mes_nombre,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_cliente dc
    ON dc.sk_cliente = fs.sk_cliente
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
WHERE fs.sk_cliente <> 0
GROUP BY
    dc.nom_cliente,
    df.anio,
    df.mes,
    df.mes_nombre
ORDER BY
    dc.nom_cliente,
    df.anio,
    df.mes;


-- ============================================================
-- P5. Mensajeros con mayor cantidad de servicios prestados
-- ============================================================

SELECT
    dm.nom_mensajero,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_mensajero dm
    ON dm.sk_mensajero = fs.sk_mensajero
WHERE fs.sk_mensajero <> 0
GROUP BY
    dm.nom_mensajero
ORDER BY
    cantidad_servicios DESC,
    dm.nom_mensajero;


-- ============================================================
-- P6. Sede que más servicios solicita para cada cliente
-- ============================================================

WITH servicios_por_sede AS (
    SELECT
        dc.nom_cliente,
        ds.nom_sede,
        ds.ciudad,
        COUNT(*) AS cantidad_servicios
    FROM ft_servicio fs
    JOIN dim_cliente dc
        ON dc.sk_cliente = fs.sk_cliente
    JOIN dim_sede ds
        ON ds.sk_sede = fs.sk_sede
    WHERE fs.sk_cliente <> 0
      AND fs.sk_sede <> 0
    GROUP BY
        dc.nom_cliente,
        ds.nom_sede,
        ds.ciudad
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY nom_cliente
            ORDER BY cantidad_servicios DESC
        ) AS posicion
    FROM servicios_por_sede
)
SELECT
    nom_cliente,
    nom_sede,
    ciudad,
    cantidad_servicios
FROM ranking
WHERE posicion = 1
ORDER BY
    nom_cliente,
    nom_sede;


-- ============================================================
-- P7. Tiempo promedio desde la solicitud hasta el cierre
-- ============================================================

SELECT
    ROUND(AVG(tiempo_entrega_min), 2) AS promedio_minutos,
    ROUND(AVG(tiempo_entrega_min) / 60.0, 2) AS promedio_horas,
    COUNT(*) AS servicios_cerrados
FROM ft_servicio
WHERE fecha_hora_cierre IS NOT NULL
  AND tiempo_entrega_min IS NOT NULL;


-- ============================================================
-- P8. Tiempo promedio por fase
-- ============================================================

SELECT
    dfase.sk_fase,
    dfase.nom_fase,
    ROUND(AVG(ffs.duracion_fase_min), 2) AS promedio_minutos,
    COUNT(*) AS fases_con_duracion
FROM ft_fase_servicio ffs
JOIN dim_fase dfase
    ON dfase.sk_fase = ffs.sk_fase
WHERE ffs.duracion_fase_min IS NOT NULL
GROUP BY
    dfase.sk_fase,
    dfase.nom_fase
ORDER BY
    promedio_minutos DESC,
    dfase.sk_fase;


-- Fase con mayor demora promedio
SELECT
    dfase.nom_fase,
    ROUND(AVG(ffs.duracion_fase_min), 2) AS promedio_minutos
FROM ft_fase_servicio ffs
JOIN dim_fase dfase
    ON dfase.sk_fase = ffs.sk_fase
WHERE ffs.duracion_fase_min IS NOT NULL
GROUP BY
    dfase.nom_fase
ORDER BY
    promedio_minutos DESC
LIMIT 1;


-- ============================================================
-- P9. Novedades más frecuentes
--
-- La fuente solo contiene 7 novedades válidas asociadas
-- a servicios reales, por lo que el resultado tiene una
-- muestra pequeña.
-- ============================================================

SELECT
    dtn.nom_tipo_novedad,
    COUNT(*) AS cantidad_novedades
FROM ft_novedad_servicio fns
JOIN dim_tipo_novedad dtn
    ON dtn.sk_tipo_novedad = fns.sk_tipo_novedad
GROUP BY
    dtn.nom_tipo_novedad
ORDER BY
    cantidad_novedades DESC,
    dtn.nom_tipo_novedad;