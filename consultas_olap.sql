-- ============================================================
-- FAST AND SAFE
-- Consultas OLAP para las nueve preguntas del proyecto
-- Base: olap_fastandsafe
-- ============================================================

\set ON_ERROR_STOP on
\pset pager off
\timing on

-- Conteos de control. Esta sección se ejecuta una sola vez.
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
-- P1. Meses con mayor cantidad de servicios
-- Se separan año y mes para no mezclar períodos de años distintos.
-- Grano usado: una fila de ft_servicio por servicio.
-- ============================================================
SELECT
    df.anio,
    df.mes,
    df.mes_nombre,
    COUNT(*) AS cantidad_servicios
FROM ft_servicio fs
JOIN dim_fecha df
    ON df.sk_fecha = fs.sk_fecha_solicitud
GROUP BY
    df.anio,
    df.mes,
    df.mes_nombre
ORDER BY
    cantidad_servicios DESC,
    df.anio,
    df.mes;


-- ============================================================
-- P2. Días de la semana con mayor cantidad de servicios
-- COUNT(*) es correcto porque ft_servicio tiene una fila por servicio.
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
-- P3. Demanda de mensajeros por hora de solicitud
-- Aproximación: la OLTP solo conserva un evento real de asignación y
-- hora_visto_por_mensajero está informada en dos servicios. Por ello se usa
-- la hora de solicitud únicamente en servicios con mensajero asignado; no es
-- la hora exacta de asignación.
-- ============================================================
SELECT
    dh.hora_24,
    dh.franja_horaria,
    COUNT(*) AS cantidad_servicios_con_mensajero
FROM ft_servicio fs
JOIN dim_hora dh
    ON dh.id_hora = fs.id_hora_solicitud
WHERE fs.sk_mensajero <> 0
GROUP BY
    dh.hora_24,
    dh.franja_horaria
ORDER BY
    cantidad_servicios_con_mensajero DESC,
    dh.hora_24;


-- ============================================================
-- P4. Servicios por cliente y mes
-- Se agrupa por la clave natural y el nombre del cliente.
-- ============================================================
SELECT
    dc.id_cliente_nk,
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
    dc.id_cliente_nk,
    dc.nom_cliente,
    df.anio,
    df.mes,
    df.mes_nombre
ORDER BY
    dc.nom_cliente,
    dc.id_cliente_nk,
    df.anio,
    df.mes;


-- ============================================================
-- P5. Mensajeros con mayor cantidad de servicios
-- Los nombres técnicos Mensajero <id> evitan exponer datos personales.
-- ============================================================
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
-- P6. Sede con mayor cantidad de servicios para cada cliente
-- Las claves del cliente y de la sede evitan fusionar entidades con nombres
-- iguales. Se excluyen miembros desconocidos y relaciones inconsistentes.
-- DENSE_RANK conserva empates en la primera posición.
-- ============================================================
WITH servicios_por_sede AS (
    SELECT
        dc.sk_cliente,
        dc.id_cliente_nk,
        dc.nom_cliente,
        ds.sk_sede,
        ds.id_sede_nk,
        ds.nom_sede,
        ds.ciudad,
        COUNT(*) AS cantidad_servicios
    FROM ft_servicio fs
    JOIN dim_cliente dc
        ON dc.sk_cliente = fs.sk_cliente
    JOIN dim_sede ds
        ON ds.sk_sede = fs.sk_sede
       AND ds.sk_cliente = fs.sk_cliente
    WHERE fs.sk_cliente <> 0
      AND fs.sk_sede <> 0
    GROUP BY
        dc.sk_cliente,
        dc.id_cliente_nk,
        dc.nom_cliente,
        ds.sk_sede,
        ds.id_sede_nk,
        ds.nom_sede,
        ds.ciudad
),
ranking AS (
    SELECT
        servicios_por_sede.*,
        DENSE_RANK() OVER (
            PARTITION BY sk_cliente
            ORDER BY cantidad_servicios DESC
        ) AS posicion
    FROM servicios_por_sede
)
SELECT
    id_cliente_nk,
    nom_cliente,
    id_sede_nk,
    nom_sede,
    ciudad,
    cantidad_servicios
FROM ranking
WHERE posicion = 1
ORDER BY
    nom_cliente,
    id_cliente_nk,
    nom_sede,
    id_sede_nk;


-- ============================================================
-- P7. Tiempo promedio entre solicitud y cierre
-- Solo se consideran servicios que tienen un cierre válido disponible.
-- ============================================================
SELECT
    ROUND(AVG(tiempo_entrega_min), 2) AS promedio_minutos,
    ROUND(AVG(tiempo_entrega_min) / 60.0, 2) AS promedio_horas,
    COUNT(*) AS servicios_cerrados
FROM ft_servicio
WHERE fecha_hora_cierre IS NOT NULL
  AND fecha_hora_cierre >= fecha_hora_solicitud
  AND tiempo_entrega_min IS NOT NULL;


-- ============================================================
-- P8. Tiempo promedio por fase y principal cuello de botella
-- El resultado incluye la muestra. Una fase con menos de 30 observaciones se
-- marca como insuficiente y no puede ser declarada cuello de botella robusto.
-- En la fuente actual, "Con mensajero asignado" solo tiene una observación.
-- ============================================================
WITH promedio_fase AS (
    SELECT
        dfase.id_fase_nk,
        dfase.nom_fase,
        ROUND(AVG(ffs.duracion_fase_min), 2) AS promedio_minutos,
        COUNT(*) AS observaciones
    FROM ft_fase_servicio ffs
    JOIN dim_fase dfase
        ON dfase.sk_fase = ffs.sk_fase
    WHERE ffs.duracion_fase_min IS NOT NULL
    GROUP BY
        dfase.sk_fase,
        dfase.id_fase_nk,
        dfase.nom_fase
),
evaluacion AS (
    SELECT
        promedio_fase.*,
        MAX(promedio_minutos) FILTER (WHERE observaciones >= 30) OVER ()
            AS mayor_promedio_con_muestra_suficiente
    FROM promedio_fase
)
SELECT
    id_fase_nk,
    nom_fase,
    promedio_minutos,
    observaciones,
    CASE
        WHEN observaciones < 30 THEN 'Muestra insuficiente'
        ELSE 'Muestra suficiente'
    END AS calidad_muestra,
    CASE
        WHEN observaciones >= 30
         AND promedio_minutos = mayor_promedio_con_muestra_suficiente
        THEN 'Sí'
        ELSE 'No'
    END AS es_principal_cuello_botella
FROM evaluacion
ORDER BY
    promedio_minutos DESC,
    id_fase_nk;


-- ============================================================
-- P9. Tipos de novedades más frecuentes
-- Se cuenta una fila por evento, se excluye el tipo desconocido y se muestra
-- su participación sobre las novedades válidas. La muestra actual es de siete.
-- ============================================================
WITH novedades_por_tipo AS (
    SELECT
        dtn.id_tipo_novedad_nk,
        dtn.nom_tipo_novedad,
        COUNT(*) AS cantidad_novedades
    FROM ft_novedad_servicio fns
    JOIN dim_tipo_novedad dtn
        ON dtn.sk_tipo_novedad = fns.sk_tipo_novedad
    WHERE fns.sk_tipo_novedad <> 0
    GROUP BY
        dtn.id_tipo_novedad_nk,
        dtn.nom_tipo_novedad
)
SELECT
    id_tipo_novedad_nk,
    nom_tipo_novedad,
    cantidad_novedades,
    ROUND(
        100.0 * cantidad_novedades
        / NULLIF(SUM(cantidad_novedades) OVER (), 0),
        2
    ) AS porcentaje_total
FROM novedades_por_tipo
ORDER BY
    cantidad_novedades DESC,
    id_tipo_novedad_nk;
