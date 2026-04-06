# Sistema de Producción - versión Render + PostgreSQL

Esta versión está preparada para desplegarse en Render usando:

- **Render Web Service** para la app
- **Render Postgres** para la base principal
- **Google Drive** opcional para sincronizar el Excel maestro y respaldar evidencias

## Qué cambia en esta versión

- Ya no depende de SQLite como base principal cuando existe `DATABASE_URL`.
- En Render usará **PostgreSQL** automáticamente.
- `APP_DATA_DIR` queda solo para archivos temporales de trabajo (`/tmp/produccion-data` por defecto en Render).
- El historial, usuarios, estados y trazabilidad quedan en Postgres.
- Drive sigue siendo opcional.

## Estructura

```text
app.py
requirements.txt
render.yaml
Procfile
.env.example
README.md
static/
templates/
```

## Variables importantes

- `SECRET_KEY`
- `DATABASE_URL`
- `APP_DATA_DIR`
- `GOOGLE_DRIVE_SYNC_MODE`
- `GOOGLE_SERVICE_ACCOUNT_FILE` (opcional)
- `GOOGLE_DRIVE_FOLDER_ID` (opcional)

## Render

El `render.yaml` ya crea:

- una base **Postgres** llamada `produccion-db`
- un **Web Service** llamado `produccion-app`

## Nota importante sobre evidencias

Cuando no hay disco persistente, los archivos locales son temporales. Por eso, para producción real conviene activar Google Drive y sincronizar automáticamente las evidencias y el Excel maestro.

## Usuarios demo

- `admin / admin123`
- `supervisor / super123`
- `tecnico / tec123`
- `almacen / alm123`
