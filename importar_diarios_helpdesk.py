"""Herramienta para importar diarios del sistema Alfa a Odoo Helpdesk.

Este script conecta a SQL Server para leer la vista ``V_MV_Diarios`` y
sincronizar tickets en Odoo mediante XML-RPC. El flujo general es:

1. Obtener los datos de SQL Server.
2. Validar los registros contra los mapeos locales (técnicos) y Odoo
   (partners y tickets existentes).
3. Crear los tickets y ajustar las marcas temporales.
4. Registrar horas en ``account.analytic.line`` cuando corresponda.

Los parámetros de conexión se cargan desde ``.env`` a través de los
módulos :mod:`sqlserver_config` y :mod:`odoo_config`.

El script produce un log detallado en ``logs/import_diarios_<fecha>.log``
con toda la información de auditoría.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pyodbc
import xmlrpc.client

from odoo_config import db, password, url, username
from sqlserver_config import sql_server

# ----------------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------------

# Campo personalizado en helpdesk.ticket para almacenar el ID del diario.
CUSTOM_FIELD_NAME = os.getenv("ODOO_TICKET_DIARY_FIELD", "x_studio_iddiario")

# Directorio y formato de logs.
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"import_diarios_{dt.datetime.now():%Y%m%d_%H%M%S}.log"

# Diccionario estático de técnicos (IdTecnico -> user_id en Odoo).
# Debe completarse con los pares proporcionados por el equipo funcional.
TECHNICIAN_USER_MAP: Dict[str, int] = {
    # TODO: reemplazar por los mapeos reales, por ejemplo:
    # "10": 25,
    # "12": 37,
   1:2,
   4:10,
   6:9, 
   9:7,
   10:11,
   15:6,
   1000:8
}

# ----------------------------------------------------------------------------
# Utilidades de logging
# ----------------------------------------------------------------------------

logger = logging.getLogger("importar_diarios_helpdesk")
logger.setLevel(logging.INFO)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(_console_handler)

# ----------------------------------------------------------------------------
# Conexiones
# ----------------------------------------------------------------------------


def get_sql_connection() -> pyodbc.Connection:
    """Crear y devolver una conexión a SQL Server."""

    logger.debug("Creando conexión a SQL Server")
    conn = pyodbc.connect(
        f"DRIVER={sql_server['driver']};"
        f"SERVER={sql_server['server']};"
        f"DATABASE={sql_server['database']};"
        f"UID={sql_server['user']};"
        f"PWD={sql_server['password']}"
    )
    return conn


def get_odoo_clients() -> Tuple[int, xmlrpc.client.ServerProxy]:
    """Autenticar contra Odoo y devolver ``(uid, models)``."""

    if not url or not db:
        raise EnvironmentError(
            "Las variables de entorno ODOO_URL y ODOO_DB deben estar configuradas"
        )

    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise ConnectionError("No fue posible autenticarse en Odoo. Verifique credenciales.")
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return uid, models


# ----------------------------------------------------------------------------
# Funciones auxiliares
# ----------------------------------------------------------------------------


def ensure_custom_field_exists(
    models: xmlrpc.client.ServerProxy, uid: int
) -> None:
    """Verificar que el campo personalizado exista en ``helpdesk.ticket``.

    Si el campo no existe, se registra un error y se lanza ``RuntimeError``
    con instrucciones básicas para su creación desde la interfaz de Odoo.
    """

    field = models.execute_kw(
        db,
        uid,
        password,
        "ir.model.fields",
        "search_read",
        [["&", ["model", "=", "helpdesk.ticket"], ["name", "=", CUSTOM_FIELD_NAME]]],
        {"limit": 1, "fields": ["id", "ttype", "field_description"]},
    )
    if not field:
        message = (
            f"No se encontró el campo personalizado '{CUSTOM_FIELD_NAME}' en helpdesk.ticket. "
            "Créelo desde Odoo en Ajustes > Técnicas > Campos Personalizados "
            "con tipo 'Char' o 'Many2one' según corresponda y vuelva a ejecutar el script."
        )
        logger.error(message)
        raise RuntimeError(message)


def get_timesheet_ticket_field(
    models: xmlrpc.client.ServerProxy, uid: int
) -> Optional[str]:
    """Devolver el nombre del campo que vincula las timesheets con los tickets."""

    for candidate in ("helpdesk_ticket_id", "ticket_id"):
        result = models.execute_kw(
            db,
            uid,
            password,
            "ir.model.fields",
            "search_read",
            [["&", ["model", "=", "account.analytic.line"], ["name", "=", candidate]]],
            {"limit": 1, "fields": ["id"]},
        )
        if result:
            logger.info(
                "Se utilizará el campo '%s' para vincular timesheets con tickets.",
                candidate,
            )
            return candidate

    logger.warning(
        "No se encontró un campo para vincular timesheets con tickets en account.analytic.line. "
        "Las horas no podrán registrarse."
    )
    return None


def fetch_diarios(
    cursor: pyodbc.Cursor,
    limit: Optional[int] = None,
    target_date: Optional[dt.date] = None,
) -> List[Dict[str, object]]:
    """Leer la vista ``V_MV_Diarios`` filtrando por la fecha indicada."""

    if target_date is None:
        target_date = dt.date.today()

    logger.info("Filtrando diarios por fecha: %s", target_date.isoformat())

    query = "SELECT * FROM V_MV_Diarios WHERE CAST(FECHAINICIO AS DATE) = ?"
    cursor.execute(query, target_date)
    rows = cursor.fetchmany(limit) if limit else cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    logger.info("Registros obtenidos desde SQL Server: %s", len(data))
    return data


def normalize_datetime(value: object) -> Optional[dt.datetime]:
    """Convertir valores provenientes de SQL a ``datetime`` de Python."""

    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
            try:
                parsed = dt.datetime.strptime(value, fmt)
                if fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                    return dt.datetime.combine(parsed.date(), dt.time.min)
                return parsed
            except ValueError:
                continue
    logger.warning("No se pudo interpretar la fecha: %s", value)
    return None


def normalize_priority(value: object) -> Optional[str]:
    """Convertir el valor de prioridad al formato que usa Odoo."""

    if value is None:
        return None
    try:
        value_str = str(value).strip()
        if not value_str:
            return None
        # Odoo espera '0', '1', '2' o '3'.
        if value_str in {"0", "1", "2", "3"}:
            return value_str
        # Aceptar etiquetas comunes como "Low", "High", etc.
        mapping = {
            "baja": "0",
            "low": "0",
            "media": "1",
            "normal": "1",
            "alta": "2",
            "high": "2",
            "urgente": "3",
            "urgent": "3",
        }
        return mapping.get(value_str.lower())
    except Exception:  # pragma: no cover - fail safe
        return None


def to_float(value: object) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_identifier(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    value_str = str(value).strip()
    return value_str or None


def find_partner_id(
    models: xmlrpc.client.ServerProxy, uid: int, cuenta: str
) -> Optional[int]:
    """Buscar el partner por ``ref`` y devolver su ID."""

    if not cuenta:
        return None
    cuenta = cuenta.strip()
    if not cuenta:
        return None

    partner_ids = models.execute_kw(
        db,
        uid,
        password,
        "res.partner",
        "search",
        [[["ref", "=", cuenta]]],
        {"limit": 1},
    )
    return partner_ids[0] if partner_ids else None


def ticket_exists(
    models: xmlrpc.client.ServerProxy, uid: int, diario_id: str
) -> bool:
    """Verificar si ya existe un ticket con el ID de diario indicado."""

    if not diario_id:
        return False
    existing = models.execute_kw(
        db,
        uid,
        password,
        "helpdesk.ticket",
        "search",
        [[[CUSTOM_FIELD_NAME, "=", diario_id]]],
        {"limit": 1},
    )
    return bool(existing)


def create_ticket(
    models: xmlrpc.client.ServerProxy,
    uid: int,
    *,
    name: str,
    description: str,
    diario_id: str,
    partner_id: int,
    user_id: int,
    fechainicio: Optional[dt.datetime],
    prioridad: Optional[str],
) -> Optional[int]:
    """Crear un ticket en Odoo y devolver su ID."""

    vals = {
        "name": name,
        "description": description or "",
        "team_id": 1,
        "partner_id": partner_id,
        "user_id": user_id,
        CUSTOM_FIELD_NAME: diario_id,
    }
    if prioridad:
        vals["priority"] = prioridad
    try:
        ticket_id = models.execute_kw(
            db, uid, password, "helpdesk.ticket", "create", [vals]
        )
        logger.info("Ticket creado (ID: %s) para diario %s", ticket_id, diario_id)
        if fechainicio:
            update_ticket_dates(models, uid, ticket_id, fechainicio)
        return ticket_id
    except xmlrpc.client.Fault as exc:
        logger.error(
            "Error al crear ticket para diario %s: %s", diario_id, exc
        )
    except Exception as exc:  # pragma: no cover - seguridad
        logger.exception(
            "Excepción inesperada al crear ticket para diario %s: %s",
            diario_id,
            exc,
        )
    return None


def update_ticket_dates(
    models: xmlrpc.client.ServerProxy,
    uid: int,
    ticket_id: int,
    fechainicio: dt.datetime,
) -> None:
    """Intentar fijar ``create_date`` y ``write_date`` del ticket."""

    iso_value = fechainicio.strftime("%Y-%m-%d %H:%M:%S")
    try:
        models.execute_kw(
            db,
            uid,
            password,
            "helpdesk.ticket",
            "write",
            [[ticket_id], {"create_date": iso_value, "write_date": iso_value}],
            {"context": {"tz": "UTC"}},
        )
        logger.info(
            "Fechas del ticket %s actualizadas a %s", ticket_id, iso_value
        )
    except xmlrpc.client.Fault as exc:
        logger.warning(
            "No fue posible actualizar las fechas del ticket %s vía ORM: %s. "
            "Considere ajustar la fecha mediante una actualización SQL directa.",
            ticket_id,
            exc,
        )


def create_timesheet_line(
    models: xmlrpc.client.ServerProxy,
    uid: int,
    ticket_id: int,
    ticket_field: str,
    *,
    diario_id: str,
    descripcion: str,
    minutos: float,
    fecha: dt.date,
    user_id: int,
) -> None:
    """Crear una ``account.analytic.line`` asociada al ticket."""

    if minutos <= 0:
        return

    unit_amount = round(minutos / 60.0, 2)
    descripcion = descripcion or "Sin descripción"
    vals = {
        "name": f"Diario {diario_id} - {descripcion[:60]}",
        "unit_amount": unit_amount,
        "date": fecha.isoformat(),
        "user_id": user_id,
        ticket_field: ticket_id,
    }
    try:
        models.execute_kw(
            db,
            uid,
            password,
            "account.analytic.line",
            "create",
            [vals],
        )
        logger.info(
            "Timesheet creada para ticket %s (%s horas)", ticket_id, unit_amount
        )
    except xmlrpc.client.Fault as exc:
        logger.error(
            "Error al crear timesheet para diario %s (ticket %s): %s",
            diario_id,
            ticket_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "Excepción inesperada al crear timesheet para diario %s: %s",
            diario_id,
            exc,
        )


# ----------------------------------------------------------------------------
# Flujo principal
# ----------------------------------------------------------------------------


def process_diarios(limit: Optional[int] = None) -> None:
    if not TECHNICIAN_USER_MAP:
        logger.error(
            "El mapeo TECHNICIAN_USER_MAP está vacío. Complete los pares IdTecnico -> user_id antes de continuar."
        )
        return

    uid, models = get_odoo_clients()
    ensure_custom_field_exists(models, uid)
    ticket_timesheet_field = get_timesheet_ticket_field(models, uid)

    connection = get_sql_connection()
    cursor = connection.cursor()
    try:
        rows = fetch_diarios(cursor, limit=limit)
        procesados = 0
        creados = 0
        descartados = 0

        for row in rows:
            procesados += 1
            diario_id = str(row.get("IDDiario") or "").strip()
            descripcion = str(row.get("Descripcion") or "").strip()
            observaciones = str(row.get("OBSERVACIONES") or "").strip()
            cuenta = str(row.get("CUENTA") or "").strip()
            minutos = to_float(row.get("MINUTOS")) or 0.0
            fechainicio = normalize_datetime(row.get("FECHAINICIO"))
            prioridad = normalize_priority(row.get("PRIORIDAD"))
            tecnico_raw = normalize_identifier(row.get("IdTecnico"))

            if not diario_id:
                logger.warning(
                    "Registro sin IDDiario descartado: %s", row
                )
                descartados += 1
                continue

            if not tecnico_raw or tecnico_raw not in TECHNICIAN_USER_MAP:
                logger.info(
                    "Diario %s descartado: IdTecnico %s sin mapeo.",
                    diario_id,
                    tecnico_raw,
                )
                descartados += 1
                continue

            user_id = TECHNICIAN_USER_MAP[tecnico_raw]

            partner_id = find_partner_id(models, uid, cuenta)
            if not partner_id:
                logger.info(
                    "Diario %s descartado: sin partner con ref %s.",
                    diario_id,
                    cuenta,
                )
                descartados += 1
                continue

            if ticket_exists(models, uid, diario_id):
                logger.info(
                    "Diario %s omitido: ticket ya existe en Odoo.", diario_id
                )
                continue

            ticket_id = create_ticket(
                models,
                uid,
                name=descripcion or f"Diario {diario_id}",
                description=observaciones,
                diario_id=diario_id,
                partner_id=partner_id,
                user_id=user_id,
                fechainicio=fechainicio,
                prioridad=prioridad,
            )
            if ticket_id:
                creados += 1
                if ticket_timesheet_field and minutos > 0 and fechainicio:
                    try:
                        create_timesheet_line(
                            models,
                            uid,
                            ticket_id,
                            ticket_timesheet_field,
                            diario_id=diario_id,
                            descripcion=descripcion,
                            minutos=minutos,
                            fecha=fechainicio.date(),
                            user_id=user_id,
                        )
                    except Exception as exc:  # pragma: no cover
                        logger.exception(
                            "Excepción inesperada al crear timesheet para diario %s: %s",
                            diario_id,
                            exc,
                        )
            else:
                descartados += 1

        logger.info(
            "Proceso finalizado. Procesados: %s, Creados: %s, Descartados: %s",
            procesados,
            creados,
            descartados,
        )
        logger.info("Log detallado: %s", LOG_FILE)
    finally:
        cursor.close()
        connection.close()


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Importar diarios de SQL Server a Odoo Helpdesk"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cantidad máxima de registros a procesar (para pruebas).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    try:
        process_diarios(limit=args.limit)
    except Exception as exc:
        logger.exception("Fallo inesperado en la importación: %s", exc)
        logger.info("Revise el archivo de log para más detalles: %s", LOG_FILE)


if __name__ == "__main__":
    main()

