# Fast and Safe — Entregable 2: ETL y bodega de datos

Este repositorio implementa el segundo entregable del proyecto de Ciencia de
Datos de Fast and Safe: restauración de la fuente operacional, creación del
modelo dimensional, ejecución del ETL y consultas de validación y análisis.

## 1. Arquitectura

```text
Backup OLTP externo
        │ restauración
        ▼
PostgreSQL 16.10 en Docker
        │
        ├── aquitoy_2          (fuente OLTP, solo lectura para el ETL)
        │         │
        │         ▼
        │   etl_fastandsafe.py
        │         │
        │         ▼
        └── olap_fastandsafe   (bodega dimensional)
                  │
                  ├── validaciones_olap.sql
                  └── consultas_olap.sql
```

Docker crea el servidor PostgreSQL, pero **no incluye automáticamente los
datos de `aquitoy_2`**. El backup OLTP debe obtenerse y compartirse por un medio
externo al repositorio.

## 2. Archivos del entregable

| Archivo | Propósito |
|---|---|
| `docker-compose.yml` | PostgreSQL y pgAdmin opcional |
| `.env.example` | Plantilla de configuración sin credenciales reales |
| `fast_and_safe_olap.sql` | Creación transaccional del modelo dimensional |
| `etl_fastandsafe.py` | Extracción, transformación, carga y validaciones |
| `validaciones_olap.sql` | Controles cruzados OLTP/OLAP |
| `consultas_olap.sql` | Consultas analíticas P1–P9 |
| `requirements.txt` | Dependencias directas del ETL |

## 3. Requisitos previos

- Docker Desktop con Docker Compose v2.
- Python 3.10 o superior.
- PowerShell.
- El backup externo `bd-OLTP`.
- Acceso a Internet en la primera ejecución para descargar imágenes y paquetes.

Comprobar herramientas:

```powershell
docker --version
docker compose version
py --version
```

## 4. Preparar el entorno Python

Desde la raíz del repositorio:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Las únicas dependencias directas son `pandas`, `SQLAlchemy`,
`psycopg2-binary` y `python-dotenv`. `numpy` no se declara directamente porque
el ETL no lo importa.

## 5. Configurar las variables de entorno

Crear el archivo local `.env`:

```powershell
Copy-Item .env.example .env
```

Antes de continuar, reemplazar todos los marcadores de usuario y contraseña.
La contraseña incluida en `POSTGRES_PASSWORD` debe coincidir con la usada en
`OLTP_URI` y `OLAP_URI`.

| Variable | Uso |
|---|---|
| `POSTGRES_USER` | Usuario administrador del contenedor PostgreSQL |
| `POSTGRES_PASSWORD` | Contraseña local de PostgreSQL |
| `POSTGRES_DB` | Base inicial creada por la imagen, normalmente `postgres` |
| `PGADMIN_EMAIL` | Usuario de pgAdmin; debe ser un correo válido |
| `PGADMIN_PASSWORD` | Contraseña local de pgAdmin |
| `OLTP_URI` | URI SQLAlchemy para `aquitoy_2` |
| `OLAP_URI` | URI SQLAlchemy para `olap_fastandsafe` |

Si el volumen de PostgreSQL ya existía, cambiar `POSTGRES_PASSWORD` en `.env`
no cambia automáticamente la contraseña almacenada en la base. En ese caso se
debe conservar la contraseña con la que se inicializó el volumen o actualizarla
de forma controlada dentro de PostgreSQL.

`.env` contiene credenciales locales y nunca debe subirse a Git.

## 6. Levantar Docker

Validar la configuración y levantar los servicios:

```powershell
docker compose config
docker compose up -d
docker compose ps -a
```

Verificar PostgreSQL:

```powershell
docker exec fastsafe-postgres pg_isready -U postgres -d postgres
```

PostgreSQL queda disponible en `localhost:5433`. pgAdmin queda disponible en
`http://localhost:5050` y es opcional: el ETL y los scripts SQL no dependen de
su interfaz.

El volumen `postgres_data` conserva las bases cuando los contenedores se
detienen o recrean.

## 7. Obtener y restaurar la OLTP

El backup contiene datos que no deben versionarse. Debe compartirse por un
medio externo y guardarse localmente en:

```text
backup/bd-OLTP
```

Crear la carpeta si hace falta:

```powershell
New-Item -ItemType Directory -Force .\backup
```

Copiar el backup al contenedor:

```powershell
docker cp ".\backup\bd-OLTP" fastsafe-postgres:/tmp/bd-OLTP
```

Comprobar si la base ya existe:

```powershell
docker exec fastsafe-postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='aquitoy_2';"
```

Si el comando no devuelve `1`, crearla y restaurar el backup:

```powershell
docker exec fastsafe-postgres createdb -U postgres aquitoy_2
docker exec fastsafe-postgres pg_restore -U postgres -d aquitoy_2 --no-owner --no-privileges --exit-on-error /tmp/bd-OLTP
```

No se debe ejecutar `pg_restore` sobre una restauración parcial o una base con
objetos existentes. Ante esa situación, revisar la base antes de decidir si se
recrea; el ETL nunca elimina ni modifica `aquitoy_2`.

Verificar la restauración:

```powershell
docker exec fastsafe-postgres psql -U postgres -d aquitoy_2 -c "SELECT COUNT(*) AS servicios FROM mensajeria_servicio;"
```

El backup usado en este proyecto contiene 28.430 servicios totales: 28.328
reales y 102 de prueba.

## 8. Crear la base y el esquema OLAP

Comprobar si la base existe:

```powershell
docker exec fastsafe-postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='olap_fastandsafe';"
```

Si no existe, crearla:

```powershell
docker exec fastsafe-postgres createdb -U postgres olap_fastandsafe
```

Copiar y ejecutar el modelo:

```powershell
docker cp ".\fast_and_safe_olap.sql" fastsafe-postgres:/tmp/fast_and_safe_olap.sql
docker exec fastsafe-postgres psql -U postgres -d olap_fastandsafe -v ON_ERROR_STOP=1 -f /tmp/fast_and_safe_olap.sql
```

El script funciona como reconstrucción completa: elimina y vuelve a crear las
dimensiones y hechos dentro de una transacción. Si una sentencia falla, no se
confirma una reconstrucción parcial.

Granularidades:

- `ft_servicio`: una fila por servicio.
- `ft_fase_servicio`: una fila por servicio y fase normalizada.
- `ft_novedad_servicio`: una fila por evento de novedad.

Dimensiones:

- `dim_fecha`
- `dim_hora`
- `dim_cliente`
- `dim_sede`
- `dim_mensajero`
- `dim_fase`
- `dim_tipo_novedad`

## 9. Ejecutar el ETL

Con el entorno virtual activado y ambas bases disponibles:

```powershell
python .\etl_fastandsafe.py
```

El ETL ejecuta toda la recarga OLAP dentro de una única transacción. Primero
vacía los datos de la carga anterior, conserva los catálogos estáticos, vuelve
a insertar los miembros desconocidos y carga dimensiones y hechos. Si una
validación crítica falla, la transacción hace rollback.

Una segunda ejecución reemplaza la carga anterior y no acumula duplicados.

### Política de registros de prueba

Solo se extraen servicios, estados y novedades con `es_prueba = FALSE`. También
se excluyen novedades reales si pertenecen a un servicio de prueba.

### Política cliente–sede

El cliente autoritativo es `mensajeria_servicio.cliente_id`. La sede se obtiene
mediante `mensajeria_servicio.usuario_id → clientes_usuarioaquitoy.sede_id`,
pero solo se conserva cuando pertenece al mismo cliente. Si no coincide, el
servicio permanece en la bodega con `sk_sede = 0` (`Sede desconocida`) y el ETL
emite una advertencia cuantificada. La fuente actual presenta 32 casos.

### Política de fases repetidas

El estado 3, `Con novedad`, no se transforma en una fase de duración. Se mapean:

| Estado OLTP | Fase OLAP |
|---:|---:|
| 1 — Iniciado | 1 |
| 2 — Con mensajero asignado | 2 |
| 4 — Recogido por mensajero | 3 |
| 5 — Entregado en destino | 4 |
| 6 — Terminado completo | 5 |

Para cada combinación servicio–fase se conserva la primera ocurrencia
cronológica. En la fuente actual se normalizan 1.027 repeticiones.

### Política de nombres de mensajeros

La clave natural sigue siendo el identificador real. Los nombres se presentan
como `Mensajero <id>` —por ejemplo `Mensajero 30`— para distinguirlos de forma
estable y evitar exponer nombres fuente poco útiles o información personal
innecesaria.

## 10. Ejecutar las validaciones

`validaciones_olap.sql` usa comandos de `psql` para leer `aquitoy_2`, cambiar a
`olap_fastandsafe` y comparar conteos e identificadores sin modificar la OLTP.

```powershell
docker cp ".\validaciones_olap.sql" fastsafe-postgres:/tmp/validaciones_olap.sql
docker exec fastsafe-postgres psql -U postgres -d olap_fastandsafe -v ON_ERROR_STOP=1 -f /tmp/validaciones_olap.sql
```

Cada control muestra el nombre, resultado, casos y estado `OK` o `REVISAR`.
`REVISAR` es esperado únicamente en las fases cuya muestra es insuficiente.

## 11. Ejecutar P1–P9

```powershell
docker cp ".\consultas_olap.sql" fastsafe-postgres:/tmp/consultas_olap.sql
docker exec fastsafe-postgres psql -U postgres -d olap_fastandsafe -v ON_ERROR_STOP=1 -f /tmp/consultas_olap.sql
```

El archivo contiene una única sección de control y una consulta por cada
pregunta P1–P9.

### Limitaciones analíticas de la fuente

- **P3:** solo hay un evento real `Con mensajero asignado` y
  `hora_visto_por_mensajero` solo aparece en dos servicios. Por ello P3 usa la
  hora de solicitud de servicios que sí tienen mensajero como aproximación de
  demanda, no como hora exacta de asignación.
- **P8:** `Con mensajero asignado` solo tiene una duración calculable. Las
  muestras actuales con duración son: una para esa fase, 26.806 para
  `Recogido por mensajero` y 8.239 para `Entregado en destino`. Las fases con
  menos de 30 observaciones se marcan como insuficientes y no se consideran un
  cuello de botella robusto.
- **P9:** solo existen siete novedades válidas asociadas a servicios reales;
  los porcentajes deben interpretarse con cautela.

## 12. Resultados esperados

Después de una carga correcta:

| Tabla o control | Resultado esperado |
|---|---:|
| `dim_fecha` | 311 |
| `dim_hora` | 24 |
| `dim_cliente` | 28, incluyendo desconocido |
| `dim_sede` | 53, incluyendo desconocida |
| `dim_mensajero` | 51, incluyendo desconocido |
| `dim_fase` | 5 |
| `dim_tipo_novedad` | 3, incluyendo desconocida |
| `ft_servicio` | 28.328 |
| `ft_fase_servicio` | 61.933 |
| `ft_novedad_servicio` | 7 |
| Servicios con sede desconocida por inconsistencia | 32 |
| Servicios sin mensajero | 710 |
| Duplicados de servicio | 0 |
| Duplicados servicio–fase | 0 |
| Duraciones negativas | 0 |
| Cierres anteriores a solicitud | 0 |
| Servicios faltantes o extras | 0 |
| Servicios o novedades de prueba cargados | 0 |

## 13. Detener y reiniciar

Detener y retirar los contenedores sin borrar las bases:

```powershell
docker compose down
```

Volver a levantarlos:

```powershell
docker compose up -d
```

También se puede usar `docker compose stop` y después `docker compose start`.

> **No usar `docker compose down -v`**, salvo que se quiera eliminar
> deliberadamente el volumen y todas las bases PostgreSQL.

Para reconstruir solo la OLAP, volver a ejecutar `fast_and_safe_olap.sql` y
después `etl_fastandsafe.py`. No es necesario eliminar `aquitoy_2`.

## 14. Archivos que no deben subirse

`.gitignore` excluye:

- `.env`
- `.venv/`
- `backup/` y `backups/`
- `*.backup`, `*.dump`, `*.dmp` y `*.tar`
- `__pycache__/` y bytecode Python
- logs y archivos temporales
- archivos temporales de Power BI y Office

El backup, las credenciales y el entorno virtual deben permanecer fuera del
commit.
