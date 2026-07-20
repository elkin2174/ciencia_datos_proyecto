# Fast and Safe — Bodega de Datos OLAP

Proyecto del curso **Introducción a la Ciencia de Datos** de la Universidad del Valle.

El proyecto implementa una bodega de datos para analizar los servicios de mensajería de la empresa **Fast and Safe**. El flujo parte de una base operacional PostgreSQL, ejecuta un proceso ETL en Python y carga los resultados en una base OLAP preparada para consultas y visualización.

---

## 1. Arquitectura del proyecto

```text
Archivo bd-OLTP
      ↓ restauración
PostgreSQL en Docker
      ↓
Base OLTP: aquitoy_2
      ↓ extracción y transformación
ETL en Python
      ↓ carga
Base OLAP: olap_fastandsafe
      ↓
Consultas SQL y Power BI
```

La base `aquitoy_2` contiene los datos operacionales originales. La base `olap_fastandsafe` contiene las dimensiones y tablas de hechos generadas por el ETL.

---

## 2. Requisitos

Antes de ejecutar el proyecto se necesita:

- Docker Desktop.
- Docker Compose.
- Python 3.10 o superior.
- PowerShell.
- El archivo de respaldo `bd-OLTP`.
- PostgreSQL 16 ejecutándose mediante Docker.

Comandos para verificar las herramientas:

```powershell
docker --version
docker compose version
python --version
```

---

## 3. Variables de entorno

El archivo `.env.example` debe contener la configuración general:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=CAMBIAR_CONTRASENA

OLTP_URI=postgresql+psycopg2://postgres:CAMBIAR_CONTRASENA@localhost:5433/aquitoy_2
OLAP_URI=postgresql+psycopg2://postgres:CAMBIAR_CONTRASENA@localhost:5433/olap_fastandsafe
```

Crear una copia llamada `.env`:

```powershell
Copy-Item .env.example .env
```

Luego reemplazar `CAMBIAR_CONTRASENA` por la contraseña configurada en `docker-compose.yml`.

El archivo `.env` no debe subirse a Git.

---

## 3. Levantar PostgreSQL con Docker

Desde la raíz del repositorio:

```powershell
docker compose up -d
```

Verificar que el contenedor esté funcionando:

```powershell
docker ps
```

Debe aparecer un contenedor llamado:

```text
fastsafe-postgres
```

El servidor PostgreSQL queda disponible en:

```text
localhost:5433
```

Para detener el entorno:

```powershell
docker compose down
```

No usar el siguiente comando salvo que se quiera eliminar también las bases de datos:

```powershell
docker compose down -v
```

La opción `-v` borra el volumen de PostgreSQL.

---

## 3. Restaurar la base OLTP

La base operacional original se restaura a partir de:

```text
backup/bd-OLTP
```

### 6.1 Copiar el respaldo al contenedor

```powershell
docker cp ".\backup\bd-OLTP" fastsafe-postgres:/tmp/bd-OLTP
```

### 6.2 Crear la base operacional

```powershell
docker exec -it fastsafe-postgres createdb -U postgres aquitoy_2
```

Si PostgreSQL informa que la base ya existe, se puede continuar.

### 6.3 Restaurar los datos

```powershell
docker exec -it fastsafe-postgres pg_restore -U postgres -d aquitoy_2 --no-owner --no-privileges --exit-on-error /tmp/bd-OLTP
```

### 6.4 Verificar la restauración

```powershell
docker exec -it fastsafe-postgres psql -U postgres -d aquitoy_2 -c "SELECT COUNT(*) FROM mensajeria_servicio;"
```

Resultado total esperado en la copia restaurada:

```text
28.430 servicios
```

También se puede verificar:

```powershell
docker exec -it fastsafe-postgres psql -U postgres -d aquitoy_2 -c "SELECT COUNT(*) FROM mensajeria_estadosservicio;"
```

```powershell
docker exec -it fastsafe-postgres psql -U postgres -d aquitoy_2 -c "SELECT COUNT(*) FROM mensajeria_novedadesservicio;"
```

---

## 3. Crear la base OLAP

### 7.1 Crear la base vacía

```powershell
docker exec -it fastsafe-postgres createdb -U postgres olap_fastandsafe
```

### 7.2 Copiar el script SQL al contenedor

```powershell
docker cp ".\fast_and_safe_olap.sql" fastsafe-postgres:/tmp/fast_and_safe_olap.sql
```

### 7.3 Crear dimensiones y tablas de hechos

```powershell
docker exec -it fastsafe-postgres psql -U postgres -d olap_fastandsafe -f /tmp/fast_and_safe_olap.sql
```

### 7.4 Verificar las tablas

```powershell
docker exec -it fastsafe-postgres psql -U postgres -d olap_fastandsafe -c "\dt"
```

Las tablas principales son:

```text
dim_fecha
dim_hora
dim_cliente
dim_sede
dim_mensajero
dim_fase
dim_tipo_novedad
ft_servicio
ft_fase_servicio
ft_novedad_servicio
```

---

## 3. Modelo dimensional implementado

### 8.1 `ft_servicio`

Granularidad:

```text
Una fila por servicio.
```

Contiene:

- Identificador del servicio.
- Cliente.
- Sede.
- Mensajero.
- Fecha y hora de solicitud.
- Fecha y hora de cierre.
- Tiempo total de entrega.
- Cantidad de servicios igual a uno.

Esta tabla permite responder preguntas de cantidad de servicios, clientes, sedes, mensajeros y tiempo promedio total.

### 8.2 `ft_fase_servicio`

Granularidad:

```text
Una fila por servicio y fase.
```

Contiene:

- Identificador del servicio.
- Fase del servicio.
- Fecha y hora de inicio.
- Fecha y hora de finalización.
- Duración de la fase.

Esta tabla permite analizar en cuál fase se presentan más demoras.

### 8.3 `ft_novedad_servicio`

Granularidad:

```text
Una fila por novedad registrada.
```

Contiene:

- Identificador del evento de novedad.
- Servicio relacionado.
- Tipo de novedad.
- Fecha y hora.
- Descripción.
- Cantidad de novedades igual a uno.

Esta tabla conserva varias novedades para un mismo servicio.

---

## 3. Instalar el entorno de Python

### 9.1 Crear entorno virtual

```powershell
py -m venv .venv
```

### 9.2 Activar el entorno

```powershell
.\.venv\Scripts\Activate.ps1
```

### 9.3 Instalar dependencias

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

El archivo `requirements.txt` debe incluir:

```text
pandas
numpy
SQLAlchemy
psycopg2-binary
python-dotenv
```

---

## 3. Ejecutar el ETL

Con PostgreSQL funcionando y las dos bases creadas:

```powershell
python .\etl_fastandsafe.py
```

El ETL realiza estas etapas:

1. Prueba las conexiones.
2. Extrae servicios, estados, novedades, clientes, sedes y mensajeros.
3. Construye las dimensiones.
4. Vacía los datos anteriores de la OLAP.
5. Carga las dimensiones.
6. Construye las tablas de hechos.
7. Carga los hechos.
8. Ejecuta validaciones finales.

---

## 3. Tratamiento de datos de prueba

Las tablas operacionales contienen una columna:

```text
es_prueba
```

Sus valores significan:

```text
FALSE = registro real
TRUE  = registro de prueba
```

El ETL carga únicamente registros reales.

Distribución encontrada en la base fuente:

| Tabla | Reales | De prueba |
|---|---:|---:|
| Servicios | 28.328 | 102 |
| Estados | 63.180 | 65.222 |
| Novedades | 8 | 5.200 |

Una de las ocho novedades reales pertenece a un servicio marcado como prueba, por lo tanto se excluye. La OLAP carga siete novedades válidas asociadas a servicios reales.

---

## 3. Tratamiento de fases repetidas

La fuente contiene varios registros donde un servicio presenta la misma fase más de una vez.

El ETL aplica la siguiente regla:

```text
Para cada combinación servicio + fase se conserva una sola ocurrencia cronológica.
```

Durante la ejecución actual se detectaron:

```text
1.027 eventos de fase repetidos
```

Los registros repetidos no se cuentan como fases adicionales.

---

## 3. Resultados de la ejecución actual

Resultado obtenido al ejecutar el ETL:

```text
Servicios extraídos:                 28.328
Estados extraídos:                   62.960
Novedades válidas extraídas:              7
Clientes:                                27
Sedes:                                   52
Mensajeros:                              50
Tipos de novedad:                         2
```

Carga OLAP:

```text
dim_fecha:                    311 registros
dim_cliente:                   27 registros
dim_mensajero:                 50 registros
dim_tipo_novedad:               2 registros
dim_sede:                      52 registros
ft_servicio:               28.328 registros
ft_fase_servicio:          61.933 registros
ft_novedad_servicio:            7 registros
```

Validaciones:

```text
Servicios OLTP / OLAP:                 28.328 / 28.328
Servicios duplicados:                       0
Duraciones de fase negativas:               0
Tiempos de entrega negativos:               0
Secuencias cronológicas inválidas:           0
Eventos de fase repetidos detectados:    1.027
```

---

## 3. Consultas de validación

### Cantidad de servicios

```sql
SELECT COUNT(*) AS servicios
FROM ft_servicio;
```

### Servicios duplicados

```sql
SELECT id_servicio, COUNT(*)
FROM ft_servicio
GROUP BY id_servicio
HAVING COUNT(*) > 1;
```

Resultado esperado:

```text
0 filas
```

### Tiempos totales negativos

```sql
SELECT COUNT(*)
FROM ft_servicio
WHERE tiempo_entrega_min < 0;
```

Resultado esperado:

```text
0
```

### Duraciones de fase negativas

```sql
SELECT COUNT(*)
FROM ft_fase_servicio
WHERE duracion_fase_min < 0;
```

Resultado esperado:

```text
0
```

### Cantidad de novedades

```sql
SELECT COUNT(*)
FROM ft_novedad_servicio;
```

Resultado esperado:

```text
7
```

---

## 3. Consultas analíticas

La bodega está preparada para responder:

1. Meses con mayor cantidad de servicios.
2. Días con más solicitudes.
3. Horas con mayor ocupación de mensajeros.
4. Servicios solicitados por cliente y mes.
5. Mensajeros con mayor cantidad de servicios.
6. Sedes con mayor cantidad de servicios por cliente.
7. Tiempo promedio desde solicitud hasta cierre.
8. Duración promedio por fase y fase con mayor demora.
9. Novedades más frecuentes.

Las consultas correspondientes se encuentran en:

```text
consultas_olap.md
```

---

## 3. Conexión desde Power BI

Para conectar Power BI a la bodega:

```text
Servidor: localhost:5433
Base de datos: olap_fastandsafe
Usuario: postgres
Contraseña: la configurada en .env
```

Power BI debe conectarse a la base OLAP, no a la OLTP.

---

## 3. Reiniciar el proyecto desde cero

Para reiniciar sin eliminar los datos:

```powershell
docker compose down
docker compose up -d
```

Para reconstruir únicamente la OLAP:

```powershell
docker exec -it fastsafe-postgres dropdb -U postgres --if-exists olap_fastandsafe
docker exec -it fastsafe-postgres createdb -U postgres olap_fastandsafe
docker cp ".\fast_and_safe_olap.sql" fastsafe-postgres:/tmp/fast_and_safe_olap.sql
docker exec -it fastsafe-postgres psql -U postgres -d olap_fastandsafe -f /tmp/fast_and_safe_olap.sql
python .\etl_fastandsafe.py
```

---