
-- Fast and Safe - Bodega de Datos OLAP (PostgreSQL)
-- Esquema Estrella

-- DIMENSIONES

-- 1. DIM_FECHA
CREATE TABLE dim_fecha (
    sk_fecha        INT PRIMARY KEY,           
    fecha           DATE NOT NULL,
    dia             INT NOT NULL,               
    dia_semana      VARCHAR(15) NOT NULL,       
    dia_semana_num  INT NOT NULL,               
    mes             INT NOT NULL,               
    mes_nombre      VARCHAR(15) NOT NULL,       
    trimestre       INT NOT NULL,               
    anio            INT NOT NULL,
    es_festivo      BOOLEAN NOT NULL DEFAULT FALSE
);

-- 2. DIM_HORA
CREATE TABLE dim_hora (
    ID_hora             INT PRIMARY KEY,        
    HORA_24             INT NOT NULL,
    franja_horaria      VARCHAR(20) NOT NULL    
);

-- 3. DIM_CLIENTE
CREATE TABLE dim_cliente (
    SK_CLIENTE      INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_cliente      VARCHAR(20) NOT NULL,       
    nom_cliente     VARCHAR(100) NOT NULL
);

-- 4. DIM_SEDE
CREATE TABLE dim_sede (
    SK_SEDE         INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ID_SEDE_NK      VARCHAR(20) NOT NULL,      
    SK_CLIENTE      INT NOT NULL REFERENCES dim_cliente(SK_CLIENTE),
    NOM_SEDE        VARCHAR(100) NOT NULL,
    CIUDAD          VARCHAR(60),
    DIRECCION       VARCHAR(200)
);

-- 5. DIM_MENSAJERO
CREATE TABLE dim_mensajero (
    SK_MENSAJERO        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ID_MENSAJERO_NK     VARCHAR(20) NOT NULL,   -- llave natural OLTP
    NOM_MENSAJERO       VARCHAR(100) NOT NULL
);

-- 6. DIM_FASE
CREATE TABLE dim_fase (
    SK_FASE         INT PRIMARY KEY,
    ID_FASE_NK      VARCHAR(20) NOT NULL,
    nom_fase        VARCHAR(60) NOT NULL
);

-- 7. DIM_NOVEDAD
CREATE TABLE dim_novedad (
    SK_NOVEDAD              INT PRIMARY KEY,
    ID_NOVEDAD_NK           VARCHAR(20),
    NOM_NOVEDAD             VARCHAR(60) NOT NULL,
    CATEGORIA_NOVEDAD       INT,                
    RESPONSABLE_NOVEDAD     VARCHAR(200),
    IMPACTO_ESTIMADO_MIN    INT,
    DESCRIPCION_NOVEDAD     VARCHAR(200)
);

-- TABLA DE HECHOS


CREATE TABLE ft_servicios (
    sk_fecha            INT NOT NULL,
    ID_hora             INT NOT NULL,
    SK_CLIENTE          INT NOT NULL,
    SK_SEDE             INT NOT NULL,
    SK_MENSAJERO        INT NOT NULL,
    SK_FASE             INT NOT NULL,
    SK_NOVEDAD          INT NOT NULL,
    cantidad_servicios  INT NOT NULL DEFAULT 1,
    cantidad_novedades  INT NOT NULL DEFAULT 0,
    tiempo_entrega_min  INT,
    duracion_fase_min   INT
);

-- INSERTS: DIMENSIONES DE REFERENCIA

-- DIM_FASE (5 fases del ciclo de vida del servicio)
INSERT INTO dim_fase (SK_FASE, ID_FASE_NK, nom_fase) VALUES
(1, 'FASE_01', 'Iniciado'),
(2, 'FASE_02', 'Con mensajero asignado'),
(3, 'FASE_03', 'Recogido en origen'),
(4, 'FASE_04', 'Entregado en destino'),
(5, 'FASE_05', 'Cerrado');

-- DIM_HORA (24 horas del dia)
INSERT INTO dim_hora (ID_hora, HORA_24, franja_horaria) VALUES
(0,  0,  'Madrugada'),
(1,  1,  'Madrugada'),
(2,  2,  'Madrugada'),
(3,  3,  'Madrugada'),
(4,  4,  'Madrugada'),
(5,  5,  'Madrugada'),
(6,  6,  'Mañana'),
(7,  7,  'Mañana'),
(8,  8,  'Mañana'),
(9,  9,  'Mañana'),
(10, 10, 'Mañana'),
(11, 11, 'Mañana'),
(12, 12, 'Tarde'),
(13, 13, 'Tarde'),
(14, 14, 'Tarde'),
(15, 15, 'Tarde'),
(16, 16, 'Tarde'),
(17, 17, 'Tarde'),
(18, 18, 'Noche'),
(19, 19, 'Noche'),
(20, 20, 'Noche'),
(21, 21, 'Noche'),
(22, 22, 'Noche'),
(23, 23, 'Noche');

-- DIM_NOVEDAD (registro especial SK=0 + tipos comunes)
INSERT INTO dim_novedad 
(SK_NOVEDAD, ID_NOVEDAD_NK, NOM_NOVEDAD, CATEGORIA_NOVEDAD, RESPONSABLE_NOVEDAD, IMPACTO_ESTIMADO_MIN, DESCRIPCION_NOVEDAD) VALUES
(1,  'NOV_01', 'Novedades del servicio',         1,    'Mensajero',    15, 'Vehiculo con falla mecanica durante el servicio'),
(2,  'NOV_02', 'No puedo continuar',    2,    'Mensajero',      10, 'El cliente no se encuentra o tarda en entregar/recibir');
