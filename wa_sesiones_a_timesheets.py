# -*- coding: utf-8 -*-
"""
Genera partes de horas (account.analytic.line) desde sesiones de WhatsApp (Discuss) por día.
- Crea una línea por sesión y por operador que participó.
- Proyecto configurable; etiqueta analítica "Cliente: <canal>" para agrupar por cliente sin crear tareas.
- Idempotente mediante firma en el campo name: [WA[channel/session]@partner_id].

Requisitos:
  pip install pandas python-dotenv pytz

Variables .env:
  ODOO_URL=
  ODOO_DB=
  ODOO_USERNAME=
  ODOO_PASSWORD=


Parámetros clave

--inactividad: minutos para cortar una sesión (igual que en tus KPIs).

--modo:
equal:   divide el tiempo de la sesión en partes iguales entre operadores que hablaron.
by_msgs: reparte según # mensajes del operador en la sesión.
full:    a cada operador se le imputa el 100% (útil si querés registrar el total a todos).
--proyecto / --proyecto-id: dónde se cargan los tiempos (debe existir).
--tag-prefix: prefijo para la etiqueta analítica por cliente/canal (por defecto Cliente: ).
--dry-run: no crea; genera CSV --preview para validar.

Cómo usar
# Solo preview del día de ayer (no crea nada en Odoo)
python wa_sesiones_a_timesheets.py --proyecto "Soporte WhatsApp" --dry-run

# Crear partes de horas de AYER
python wa_sesiones_a_timesheets.py --proyecto "Soporte WhatsApp"

# Un día concreto y reparto proporcional por mensajes, con preview a medida
python wa_sesiones_a_timesheets.py --fecha 2025-08-31 --modo by_msgs --proyecto "Soporte WhatsApp" --dry-run --preview preview_2025-08-31.csv

# Rehacer (borra y recrea si ya existía la línea del mismo operador/sesión)
python wa_sesiones_a_timesheets.py --proyecto "Soporte WhatsApp" --recrear

"""

import os, sys, argparse, datetime as dt, re, hashlib, xmlrpc.client
import pandas as pd
import pytz
from dotenv import load_dotenv

# ================= Config =================
TZ_STR = "America/Argentina/Buenos_Aires"
INACTIVITY_MINUTES_DEFAULT = 45
OP_TIME_ATTRIB_DEFAULT = "equal"  # equal | by_msgs | full

# ================ Utils ===================
def odoo_connect():
    load_dotenv()
    url = os.getenv("ODOO_URL"); db = os.getenv("ODOO_DB")
    user = os.getenv("ODOO_USERNAME"); pwd = os.getenv("ODOO_PASSWORD")
    if not all([url, db, user, pwd]):
        print("Faltan ODOO_URL/DB/USERNAME/PASSWORD en .env", file=sys.stderr); sys.exit(1)
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, pwd, {})
    if not uid:
        print("No pude autenticar en Odoo.", file=sys.stderr); sys.exit(1)
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return url, db, uid, pwd, models

def model_exists(models, db, uid, pwd, name):
    return bool(models.execute_kw(db, uid, pwd, "ir.model", "search", [[("model","=",name)]], {"limit":1}))

def fields_get(models, db, uid, pwd, model):
    return models.execute_kw(db, uid, pwd, model, "fields_get", [[], {"attributes": ["type"]}])

def search(models, db, uid, pwd, model, domain, order="id asc", limit=None):
    kw = {"order": order}
    if limit is not None:
        kw["limit"] = limit
    return models.execute_kw(db, uid, pwd, model, "search", [domain], kw)

def read(models, db, uid, pwd, model, ids, fields):
    out = []
    for i in range(0, len(ids), 200):
        out.extend(models.execute_kw(db, uid, pwd, model, "read", [ids[i:i+200]], {"fields": fields}))
    return out

def to_local_day_bounds(date_local):
    """Retorna inicio y fin del día local en UTC."""
    tz = pytz.timezone(TZ_STR)
    y, m, d = map(int, date_local.split("-"))
    start_local = tz.localize(dt.datetime(y, m, d, 0, 0, 0))
    end_local   = tz.localize(dt.datetime(y, m, d, 23, 59, 59))
    return start_local.astimezone(dt.timezone.utc), end_local.astimezone(dt.timezone.utc)

def strip_html(t):
    if not t: return ""
    from html import unescape
    t = unescape(t)
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    return t.strip()

def digest(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

# ================ Core ===================
def build_sessions_from_messages(models, db, uid, pwd, date_local, inactivity_minutes):
    """Lee mensajes del día y arma sesiones por canal (con fallbacks)."""
    # Siempre inicializamos para evitar NameError
    sessions = pd.DataFrame()
    ops_per_sess = pd.DataFrame()

    ini_utc, fin_utc = to_local_day_bounds(date_local)

    # Detectar modelo de canal
    if model_exists(models, db, uid, pwd, "discuss.channel"):
        channel_model = "discuss.channel"
    elif model_exists(models, db, uid, pwd, "mail.channel"):
        channel_model = "mail.channel"
    else:
        raise RuntimeError("No existe discuss.channel ni mail.channel en tu base.")

    # Usuarios internos (para marcar operador)
    user_ids = search(models, db, uid, pwd, "res.users", [("share","=",False)])
    users = read(models, db, uid, pwd, "res.users", user_ids, ["partner_id","name"])
    internal_partner_ids = {u["partner_id"][0] for u in users if u.get("partner_id")}

    # Canales candidatos
    ch_fields = fields_get(models, db, uid, pwd, channel_model)
    has_channel_type = "channel_type" in ch_fields
    chan_ids_all = search(models, db, uid, pwd, channel_model, [])
    channels = read(models, db, uid, pwd, channel_model, chan_ids_all,
                    ["id","name"] + (["channel_type"] if has_channel_type else []))
    if has_channel_type:
        channels = [c for c in channels if c.get("channel_type") == "whatsapp"]
    if not channels:
        print(f"[INFO] {date_local}: no hay canales WhatsApp.")
        return sessions, ops_per_sess, channel_model

    ch_map = {c["id"]: c.get("name","") for c in channels}
    channel_ids = list(ch_map.keys())

    # Mensajes del día (con fallbacks)
    base = [
        ("date", ">=", ini_utc.strftime("%Y-%m-%d %H:%M:%S")),
        ("date", "<=", fin_utc.strftime("%Y-%m-%d %H:%M:%S")),
        ("res_id", "in", channel_ids),
    ]
    # 1) con model
    msg_ids = search(models, db, uid, pwd, "mail.message", [("model","=",channel_model)] + base, order="date asc")
    # 2) sin model
    if not msg_ids:
        msg_ids = search(models, db, uid, pwd, "mail.message", base, order="date asc")
    # 3) solo por fecha+model; filtramos res_id en python
    direct_filter_by_res_id = True
    if not msg_ids:
        direct_filter_by_res_id = False
        msg_ids = search(models, db, uid, pwd, "mail.message", [
            ("model","=",channel_model),
            ("date", ">=", ini_utc.strftime("%Y-%m-%d %H:%M:%S")),
            ("date", "<=", fin_utc.strftime("%Y-%m-%d %H:%M:%S")),
        ], order="date asc")

    if not msg_ids:
        print(f"[INFO] {date_local}: no hay mensajes.")
        return sessions, ops_per_sess, channel_model

    msgs = read(models, db, uid, pwd, "mail.message", msg_ids,
                ["id","date","author_id","body","model","res_id","message_type","subtype_id"])

    if not direct_filter_by_res_id:
        msgs = [m for m in msgs if m.get("res_id") in ch_map]

    # DataFrame de mensajes
    rows = []
    for m in msgs:
        if m.get("res_id") not in ch_map:
            continue
        rows.append({
            "msg_id": m.get("id"),
            "channel_id": m.get("res_id"),
            "canal": ch_map.get(m.get("res_id"), ""),
            "fecha_utc": pd.to_datetime(m.get("date"), utc=True, errors="coerce"),
            "autor_partner_id": (m["author_id"][0] if m.get("author_id") else None),
            "autor": (m["author_id"][1] if m.get("author_id") else ""),
            "es_operador": ((m["author_id"][0] in internal_partner_ids) if m.get("author_id") else False),
            "texto": strip_html(m.get("body","")),
        })
    df = pd.DataFrame(rows).dropna(subset=["fecha_utc"])
    if df.empty:
        print(f"[INFO] {date_local}: no hay mensajes con fecha válida.")
        return sessions, ops_per_sess, channel_model

    df["fecha_local"] = df["fecha_utc"].dt.tz_convert(TZ_STR)
    df = df.sort_values(["channel_id","fecha_local"])

    # Sesiones por inactividad
    df["session_break"] = (
        df.groupby("channel_id")["fecha_local"].diff().dt.total_seconds().div(60)
          .gt(inactivity_minutes).fillna(True)
    )
    df["session_id"] = df.groupby("channel_id")["session_break"].cumsum().astype(int)

    # Resumen de sesiones
    grp = df.groupby(["canal","channel_id","session_id"])
    sessions = grp.agg(
        inicio     = ("fecha_local","min"),
        fin        = ("fecha_local","max"),
        dur_min    = ("fecha_local", lambda s: (s.max()-s.min()).total_seconds()/60.0),
        msgs_total = ("msg_id","count"),
        msgs_oper  = ("es_operador", lambda s: int(s.sum())),
        msgs_user  = ("es_operador", lambda s: int((~s).sum())),
    ).reset_index()

    # Solo sesiones con usuario y operador
    sessions = sessions[(sessions["msgs_user"] > 0) & (sessions["msgs_oper"] > 0)]
    sessions["date_local"] = sessions["inicio"].dt.tz_convert(TZ_STR).dt.date

    # Operadores por sesión
    ops_per_sess = (
        df[df["es_operador"] == True]
        .groupby(["channel_id","session_id","autor_partner_id","autor"], as_index=False)
        .agg(msgs_oper=("msg_id","count"))
    )

    # Log (ahora sí, con variables definidas)
    print(f"[INFO] {date_local}: mensajes={len(df)} | sesiones={len(sessions)} | ops-sesion={len(ops_per_sess)}")

    return sessions, ops_per_sess, channel_model

def build_employee_map(models, db, uid, pwd):
    """Mapea partner_id -> (user_id, employee_id, employee_name) para operadores."""
    partner_to_user = {}
    user_to_employee = {}

    user_ids = search(models, db, uid, pwd, "res.users", [("share","=",False)])
    users = read(models, db, uid, pwd, "res.users", user_ids, ["partner_id","name","employee_ids"])

    if model_exists(models, db, uid, pwd, "hr.employee"):
        emp_ids = search(models, db, uid, pwd, "hr.employee", [])
        emps = read(models, db, uid, pwd, "hr.employee", emp_ids, ["id","name","user_id"])
        for e in emps:
            if e.get("user_id"):
                user_to_employee[e["user_id"][0]] = (e["id"], e["name"])

    for u in users:
        if u.get("partner_id"):
            partner_to_user[u["partner_id"][0]] = (u["id"], u.get("name",""))

    return partner_to_user, user_to_employee

def find_project(models, db, uid, pwd, project_name, crear=False):
    ids = search(models, db, uid, pwd, "project.project", [("name","=",project_name)], limit=1)
    if ids:
        return ids[0]
    if not crear:
        raise RuntimeError(f"No encontré el Proyecto '{project_name}'. Crealo o pasa --proyecto-id.")
    new_id = models.execute_kw(db, uid, pwd, "project.project", "create", [{
        "name": project_name,
        "allow_timesheets": True,
    }])
    return new_id

def find_project_by_id(models, db, uid, pwd, project_id):
    ids = search(models, db, uid, pwd, "project.project", [("id","=",project_id)], limit=1)
    if not ids:
        raise RuntimeError(f"No encontré el Proyecto ID {project_id}.")
    return ids[0]

def find_or_create_tag(models, db, uid, pwd, tag_name):
    ids = search(models, db, uid, pwd, "account.analytic.tag", [("name","=",tag_name)], limit=1)
    if ids:
        return ids[0]
    return models.execute_kw(db, uid, pwd, "account.analytic.tag", "create", [{"name": tag_name}])

def create_timesheet_if_needed(models, db, uid, pwd, line_vals, signature, recreate=False):
    """Evita duplicados con name ilike [signature] + date + project + employee."""
    name_marker = f"[{signature}]"
    dom = [
        ("name","ilike", name_marker),
        ("date","=", line_vals.get("date")),
        ("project_id","=", line_vals.get("project_id")),
        ("employee_id","=", line_vals.get("employee_id")),
    ]
    existing = search(models, db, uid, pwd, "account.analytic.line", dom, limit=1)
    if existing:
        if recreate:
            models.execute_kw(db, uid, pwd, "account.analytic.line", "unlink", [existing])
        else:
            return existing[0], False
    new_id = models.execute_kw(db, uid, pwd, "account.analytic.line", "create", [line_vals])
    return new_id, True

# ================ CLI ===================
def parse_args():
    ap = argparse.ArgumentParser("Crear partes de horas desde sesiones de WhatsApp (por día)")
    ap.add_argument("--fecha", help="Fecha LOCAL AAAA-MM-DD (default: ayer)", default=None)
    ap.add_argument("--inactividad", type=int, default=INACTIVITY_MINUTES_DEFAULT, help="Minutos de inactividad que cortan una sesión")
    ap.add_argument("--modo", choices=["equal","by_msgs","full"], default=OP_TIME_ATTRIB_DEFAULT, help="Reparto del tiempo entre operadores")
    ap.add_argument("--proyecto", help="Nombre de Proyecto donde cargar tiempos")
    ap.add_argument("--proyecto-id", type=int, help="ID de proyecto")
    ap.add_argument("--crear-proyecto", action="store_true", help="Crear proyecto si no existe (cuando se usa --proyecto)")
    ap.add_argument("--tag-prefix", default="Cliente: ", help="Prefijo para etiqueta analítica por canal/cliente")
    ap.add_argument("--dry-run", action="store_true", help="No crea nada; genera preview CSV")
    ap.add_argument("--preview", default="preview_wa_timesheets.csv", help="Ruta del CSV de preview")
    ap.add_argument("--recrear", action="store_true", help="Si existe la línea (misma firma), borrar y recrear")
    return ap.parse_args()

def main():
    args = parse_args()

    # Fecha por defecto: AYER local
    if not args.fecha:
        tz = pytz.timezone(TZ_STR)
        ayer = dt.datetime.now(tz).date() - dt.timedelta(days=1)
        args.fecha = ayer.isoformat()

    url, db, uid, pwd, models = odoo_connect()

    # Proyecto
    if args.proyecto_id:
        project_id = find_project_by_id(models, db, uid, pwd, args.proyecto_id)
    elif args.proyecto:
        project_id = find_project(models, db, uid, pwd, args.proyecto, crear=args.crear_proyecto)
    else:
        raise RuntimeError("Debés indicar --proyecto o --proyecto-id")

    # Sesiones y operadores
    sessions, ops_per_sess, channel_model = build_sessions_from_messages(
        models, db, uid, pwd, args.fecha, args.inactividad
    )

    # Si no hay sesiones, igual generamos CSV vacío y salimos
    preview_rows = []
    if sessions.empty or ops_per_sess.empty:
        cols = ["date","canal","operador","horas","minutos","project_id","employee_id","firma","name","tag"]
        pd.DataFrame(columns=cols).to_csv(args.preview, index=False, encoding="utf-8-sig")
        print(f"Preview vacío guardado en: {args.preview}")
        return

    # Map empleado (partner -> user -> employee)
    partner_to_user, user_to_employee = build_employee_map(models, db, uid, pwd)

    # Unir detalle operador-sesión con info de la sesión (sin pedir columnas que pueden faltar)
    base_cols = ["channel_id","session_id","canal","inicio","fin","dur_min","date_local"]
    avail_cols = [c for c in base_cols if c in sessions.columns]
    ops = ops_per_sess.merge(sessions[avail_cols], on=["channel_id","session_id"], how="left")


    # --- Saneamos tiempos y fecha ---
    ops["dur_min"] = pd.to_numeric(ops.get("dur_min"), errors="coerce").fillna(0.0)

    # date_local puede venir NaT si el merge no encontró match; usamos la fecha pedida
    try:
        default_date = pd.to_datetime(args.fecha).date()
    except Exception:
        import datetime as _dt
        default_date = _dt.datetime.now(pytz.timezone(TZ_STR)).date()

    if "date_local" in ops.columns:
        ops["date_local"] = ops["date_local"].fillna(default_date)
    else:
        ops["date_local"] = default_date

    # Peso de reparto
    mode = args.modo
    if mode == "by_msgs":
        tot_by_sess = ops.groupby(["channel_id","session_id"])["msgs_oper"].transform("sum")
        ops["weight"] = ops["msgs_oper"] / tot_by_sess.replace(0, 1)
    elif mode == "full":
        ops["weight"] = 1.0
    else:  # equal
        n_ops = ops.groupby(["channel_id","session_id"])["autor_partner_id"].transform("nunique")
        ops["weight"] = 1.0 / n_ops.replace(0, 1)

    # Métricas de tiempo por operador-sesión
    ops["dur_min"] = ops["dur_min"].astype(float)
    ops["min_asignados"] = ops["dur_min"] * ops["weight"]
    ops["horas_asignadas"] = ops["min_asignados"] / 60.0

    # ¿account.analytic.line tiene tags?
    aal_fields = fields_get(models, db, uid, pwd, "account.analytic.line")
    has_tags = "tag_ids" in aal_fields
    tag_cache = {}

    creados = 0
    ya_existian = 0
    omitidos_sin_empleado = {}

    # Armar plan + (opcional) crear
    for _, r in ops.iterrows():
        partner_id = int(r["autor_partner_id"]) if pd.notna(r["autor_partner_id"]) else None
        if partner_id is None:
            continue

        # Map partner -> user -> employee
        user_info = partner_to_user.get(partner_id)
        employee_id = None; user_id = None; emp_name = None
        if user_info:
            user_id = user_info[0]
            emp = user_to_employee.get(user_id)
            if emp:
                employee_id, emp_name = emp

        if not employee_id:
            k = r["autor"]
            omitidos_sin_empleado[k] = omitidos_sin_empleado.get(k, 0) + 1
            continue

        # Etiqueta analítica Cliente: <canal>
        tag_id = None
        if has_tags:
            tag_name = f"{args.tag_prefix}{r['canal']}".strip()
            if tag_name not in tag_cache:
                tag_cache[tag_name] = find_or_create_tag(models, db, uid, pwd, tag_name)
            tag_id = tag_cache[tag_name]

        # Firma idempotente y nombre
        signature = f"WA[{int(r['channel_id'])}/{int(r['session_id'])}]@{partner_id}"
        def _fmt_hhmm(x):
            try:
                if pd.isna(x):
                    return None
                return pd.to_datetime(x).tz_convert(TZ_STR).strftime("%H:%M")
            except Exception:
                return None

        start_h = _fmt_hhmm(r.get("inicio"))
        end_h   = _fmt_hhmm(r.get("fin"))

        if start_h and end_h:
            time_span = f"{start_h}-{end_h}"
        else:
            # fallback: mostramos duración en minutos cuando no hay timestamp
            time_span = f"{int(round(r.get('dur_min', 0)))}m"

        name = f"WhatsApp {r['canal']} ({time_span}) - {r['autor']} [{signature}]"

        line_vals = {
            "name": name,
            "date": str(r["date_local"]),
            "unit_amount": float(round(r["horas_asignadas"], 2)),
            "project_id": int(project_id),
            "employee_id": int(employee_id),
        }
        if has_tags and tag_id:
            line_vals["tag_ids"] = [(6, 0, [int(tag_id)])]

        preview_rows.append({
            "date": line_vals["date"],
            "canal": r["canal"],
            "operador": r["autor"],
            "horas": line_vals["unit_amount"],
            "minutos": int(round(r["min_asignados"])),
            "project_id": project_id,
            "employee_id": employee_id,
            "firma": signature,
            "name": name,
            "tag": f"{args.tag_prefix}{r['canal']}" if has_tags else "",
        })

        if not args.dry_run:
            _id, created = create_timesheet_if_needed(models, db, uid, pwd, line_vals, signature, recreate=args.recrear)
            if created:
                creados += 1
            else:
                ya_existian += 1

    # Preview CSV (siempre)
    cols = ["date","canal","operador","horas","minutos","project_id","employee_id","firma","name","tag"]
    df_prev = pd.DataFrame(preview_rows, columns=cols)
    df_prev.to_csv(args.preview, index=False, encoding="utf-8-sig")
    print(f"Preview guardado en: {args.preview}  (filas: {len(df_prev)})")

    if args.dry_run:
        print("DRY-RUN: no se creó ningún parte de horas. Revisá el CSV y ejecutá sin --dry-run para grabar.")
    else:
        print(f"Listo: creados {creados} timesheets. Ya existentes: {ya_existian}.")
        if omitidos_sin_empleado:
            print("Omitidos por falta de empleado:")
            for k,v in omitidos_sin_empleado.items():
                print(f"  - {k}: {v}")

if __name__ == "__main__":
    main()
