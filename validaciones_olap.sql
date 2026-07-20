-- Cantidad de servicios
SELECT COUNT(*) AS servicios
FROM ft_servicio;

-- Servicios duplicados
SELECT id_servicio, COUNT(*)
FROM ft_servicio
GROUP BY id_servicio
HAVING COUNT(*) > 1;

-- Tiempos negativos
SELECT COUNT(*)
FROM ft_servicio
WHERE tiempo_entrega_min < 0;

-- Duraciones de fase negativas
SELECT COUNT(*)
FROM ft_fase_servicio
WHERE duracion_fase_min < 0;

-- Conteo de novedades
SELECT COUNT(*)
FROM ft_novedad_servicio;

-- Conteo por tabla
SELECT 'dim_fecha' AS tabla, COUNT(*) FROM dim_fecha
UNION ALL
SELECT 'dim_cliente', COUNT(*) FROM dim_cliente
UNION ALL
SELECT 'dim_sede', COUNT(*) FROM dim_sede
UNION ALL
SELECT 'dim_mensajero', COUNT(*) FROM dim_mensajero
UNION ALL
SELECT 'ft_servicio', COUNT(*) FROM ft_servicio
UNION ALL
SELECT 'ft_fase_servicio', COUNT(*) FROM ft_fase_servicio
UNION ALL
SELECT 'ft_novedad_servicio', COUNT(*) FROM ft_novedad_servicio;