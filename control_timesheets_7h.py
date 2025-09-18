# -*- coding: utf-8 -*-
"""
control_timesheets_7h.py
Control diario de Hojas de Horas (Odoo v18, SaaS) y aviso por email si el usuario no llega al mínimo.

Funciones:
- Fecha objetivo: AYER por defecto, o --fecha YYYY-MM-DD.
- Suma horas por employee_id OR user_id en account.analytic.line.
- Objetivo mínimo fijo (--min, default 7) o según calendario laboral (--use-calendar).
- Envía email por cada usuario incumplido (o --dry-run para pruebas).

Requisitos:
  pip install python-dotenv

Uso típico:
  # Prueba sin enviar (ayer)
  python -u control_timesheets_7h.py --dry-run --debug

  # Ejecutar real (ayer)
  python -u control_timesheets_7h.py

  # Fecha puntual, mínimo 8 h
  python -u control_timesheets_7h.py --fecha 2025-09-11 --min 8

  # Tomar objetivo del calendario laboral (ignora --min si hay calendario y día hábil)
  python -u control_timesheets_7h.py --use-calendar
"""

import os
import sys
import argparse
import datetime as dt
import traceback
import xmlrpc.client
from dotenv import load_dotenv, dotenv_values

# =============== CONFIG ===============
# Lista de usuarios a controlar (IDs de res.users y email destino)
USUARIOS = [
    {"user_id": 8,  "notify_email": "faviolantunez@gmail.com"},
    {"user_id": 9,  "notify_email": "marcoslromero23@gmail.com"},
    {"user_id": 7,  "notify_email": "frankowtf98@gmail.com"},
    {"user_id": 11, "notify_email": "alejandrotorres_lp@hotmail.com"},
]
# =====================================

def log(msg):
    now = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

# ---------------- Odoo helpers ----------------
def odoo_connect(env_path=None, debug=False):
    if env_path:
        if not os.path.isfile(env_path):
            raise RuntimeError(f".env no encontrado: {env_path}")
        load_dotenv(dotenv_path=env_path, override=True)
        env_src = dotenv_values(env_path)
    else:
        load_dotenv()
        env_src = dotenv_values()

    if debug:
        safe = {k: ("***" if "PASS" in k else v) for k, v in (env_src or {}).items()}
        log(f".env cargado: {safe}")

    url = os.getenv("ODOO_URL"); db = os.getenv("ODOO_DB")
    username = os.getenv("ODOO_USERNAME"); password = os.getenv("ODOO_PASSWORD")
    if not all([url, db, username, password]):
        raise RuntimeError("Faltan ODOO_URL / ODOO_DB / ODOO_USERNAME / ODOO_PASSWORD en .env")

    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise RuntimeError("Autenticación en Odoo fallida")

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return url, db, uid, password, models

def execute_kw(models, db, uid, password, model, method, *args, **kwargs):
    return models.execute_kw(db, uid, password, model, method, list(args), kwargs or {})

def get_user_and_employee(models, db, uid, password, user_id):
    user = None
    # Intento 1: read directo por id (sin domain)
    try:
        users = execute_kw(models, db, uid, password, 'res.users', 'read',
                           [[int(user_id)]], fields=['id','name','email','employee_id'])
        if users:
            user = users[0]
    except Exception:
        # Fallback mínimo si no deja leer res.users
        user = {'id': int(user_id), 'name': f'Usuario {user_id}', 'email': None, 'employee_id': False}

    # Empleado por user_id (seguro en SaaS)
    emp = None
    try:
        emp_ids = execute_kw(models, db, uid, password, 'hr.employee', 'search',
                             [[('user_id','=', int(user_id))]], limit=1)
        if emp_ids:
            emps = execute_kw(models, db, uid, password, 'hr.employee', 'read',
                              [emp_ids],
                              fields=['id','name','user_id','resource_id','resource_calendar_id','company_id'])
            if emps:
                emp = emps[0]
    except Exception:
        emp = None

    return user, emp



# --------- Objetivo horario ----------
def horas_objetivo_calendar(models, db, uid, password, employee, d: dt.date) -> float:
    """Suma las horas del calendario laboral del empleado para el día d. 0 si no trabaja ese día."""
    if not employee or not employee.get('resource_calendar_id'):
        return 0.0
    cal_id = employee['resource_calendar_id'][0]
    weekday = d.weekday()  # 0..6
    attends = execute_kw(models, db, uid, password, 'resource.calendar.attendance', 'search_read',
                         [[('calendar_id','=',cal_id), ('dayofweek','=',str(weekday))]],
                         fields=['hour_from','hour_to'])
    total = 0.0
    for a in attends or []:
        hf = float(a.get('hour_from',0.0)); ht = float(a.get('hour_to',0.0))
        if ht > hf:
            total += (ht - hf)
    return round(total, 2)

def es_habil_calendar(models, db, uid, password, employee, d: dt.date) -> bool:
    """Día hábil considerando calendario + ausencias/feriados."""
    if not employee or not employee.get('resource_calendar_id'):
        return d.weekday() <= 4  # L-V por defecto
    cal_id = employee['resource_calendar_id'][0]
    weekday = d.weekday()
    attends = execute_kw(models, db, uid, password, 'resource.calendar.attendance', 'search_read',
                         [[('calendar_id','=',cal_id), ('dayofweek','=',str(weekday))]],
                         fields=['id'], limit=1)
    if not attends:
        return False
    # ¿Tiene licencia/feriado que cubra el día?
    start = dt.datetime.combine(d, dt.time.min).strftime('%Y-%m-%d %H:%M:%S')
    end   = dt.datetime.combine(d, dt.time.max).strftime('%Y-%m-%d %H:%M:%S')
    resource_id = employee.get('resource_id', [None])[0] if employee.get('resource_id') else None
    leave_dom = [
        ('calendar_id','=',cal_id),
        ('date_from','<=', end),
        ('date_to','>=', start),
        '|', ('resource_id','=',False), ('resource_id','=',resource_id)
    ]
    leaves = execute_kw(models, db, uid, password, 'resource.calendar.leaves', 'search_read',
                        [leave_dom], fields=['id'], limit=1)
    return not bool(leaves)

# --------- Cómputo de horas ---------
def horas_cargadas(models, db, uid, password, employee, user, d: dt.date, debug=False) -> float:
    emp_id = employee['id'] if employee else None
    user_id = user['id'] if user else None

    # Dominio (usamos tuplas para cada condición)
    base = [('date', '=', d.strftime('%Y-%m-%d')), ('unit_amount', '>', 0)]
    if emp_id and user_id:
        domain = ['|', ('employee_id', '=', emp_id), ('user_id', '=', user_id)] + base
    elif emp_id:
        domain = [('employee_id', '=', emp_id)] + base
    elif user_id:
        domain = [('user_id', '=', user_id)] + base
    else:
        return 0.0

    # 1) search -> ids (sin kwargs, evitando el bug del Domain)
    ids = execute_kw(models, db, uid, password,
                     'account.analytic.line', 'search', [domain])

    if not ids:
        if debug:
            log(f"    {d} -> 0 líneas encontradas")
        return 0.0

    # 2) read por lotes -> unit_amount
    total = 0.0
    step = 200
    for i in range(0, len(ids), step):
        batch_ids = ids[i:i+step]
        lines = execute_kw(models, db, uid, password,
                           'account.analytic.line', 'read', [batch_ids],
                           fields=['unit_amount'])
        total += sum(float(l.get('unit_amount') or 0.0) for l in lines)

    total = round(total, 2)
    if debug:
        log(f"    {d} -> {len(ids)} líneas | total={total:.2f} h")
    return total


# --------- Email ----------
def enviar_email(models, db, uid, password, to_email, asunto, body_html, dry_run=False, debug=False):
    if not to_email:
        if debug: log("  [WARN] sin destinatario -> omitido")
        return
    vals = {
        'subject': asunto,
        'body_html': f'<!doctype html><html><body>{body_html}</body></html>',
        'email_to': to_email,
        'auto_delete': True,
        'email_from': False,  # usa default envelope
    }
    if dry_run:
        log(f"[DRY-RUN] email a {to_email} | {asunto}")
        return
    mail_id = execute_kw(models, db, uid, password, 'mail.mail', 'create', [vals])
    execute_kw(models, db, uid, password, 'mail.mail', 'send', [[mail_id]])
    if debug:
        log(f"  mail.mail {mail_id} enviado")

# ===================== MAIN =====================
def main():
    ap = argparse.ArgumentParser("Control mínimo de horas por día y aviso por email")
    ap.add_argument('--fecha', help='Fecha objetivo (YYYY-MM-DD). Si no, AYER.')
    ap.add_argument('--ayer', action='store_true', help='Forzar AYER (ignora --fecha).')
    ap.add_argument('--min', type=float, default=7.0, help='Mínimo de horas si no se usa calendario.')
    ap.add_argument('--use-calendar', action='store_true', help='Usar horas del calendario laboral como objetivo.')
    ap.add_argument('--dry-run', action='store_true', help='No envía emails (modo prueba).')
    ap.add_argument('--debug', action='store_true', help='Salida detallada.')
    ap.add_argument('--env', help='Ruta a archivo .env (opcional).')
    args = ap.parse_args()

    try:
        # Fecha objetivo
        if args.ayer or not args.fecha:
            fecha = (dt.date.today() - dt.timedelta(days=1))
        else:
            fecha = dt.datetime.strptime(args.fecha, '%Y-%m-%d').date()
        log(f"Controlando fecha: {fecha.isoformat()}")

        # Conexión
        url, db, uid, password, models = odoo_connect(args.env, debug=args.debug)
        if args.debug: log(f"Conectado a {db} (uid={uid})")

        enviados = 0
        for u in USUARIOS:
            user_id = u['user_id']
            destino = u.get('notify_email')

            log(f"- Usuario {user_id} -> {destino or '(sin email)'}")
            user, emp = get_user_and_employee(models, db, uid, password, user_id)
            if not user:
                log("  [WARN] usuario no encontrado, se omite")
                continue

            # ¿Día exigible?
            habil = es_habil_calendar(models, db, uid, password, emp, fecha) if args.use_calendar else (fecha.weekday() <= 4)
            log(f"  Día laborable: {habil}")
            if not habil:
                continue

            # Objetivo
            objetivo = horas_objetivo_calendar(models, db, uid, password, emp, fecha) if args.use_calendar else args.min
            if objetivo <= 0:  # p.ej., sábado en calendario
                log("  Objetivo=0 (no exigible).")
                continue

            # Horas reales
            horas = horas_cargadas(models, db, uid, password, emp, user, fecha, debug=args.debug)
            log(f"  Horas {horas:.2f} / objetivo {objetivo:.2f}")

            if horas + 1e-6 < objetivo:
                asunto = f"[Recordatorio] Hojas de horas incompletas – {fecha.isoformat()}"
                body = (
                    f"<p>Hola {user.get('name','')},</p>"
                    f"<p>El {fecha.isoformat()} registraste <b>{horas:.2f} h</b> en Hojas de horas. "
                    f"El objetivo para ese día es <b>{objetivo:.2f} h</b>.</p>"
                    f"<p>Por favor, completá la carga en Odoo cuando puedas.</p>"
                    f"<p><a href='{url}'>Ir a Odoo</a></p>"
                )
                enviar_email(models, db, uid, password, destino, asunto, body,
                             dry_run=args.dry_run, debug=args.debug)
                enviados += 1

        log(f"Finalizado. Avisos gestionados: {enviados}")

    except Exception:
        log("ERROR no controlado:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
