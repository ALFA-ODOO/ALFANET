# -*- coding: utf-8 -*-
import os, sys, argparse, datetime as dt, re, xmlrpc.client
import pandas as pd
from dotenv import load_dotenv
from html import unescape
import math  # <---- si no estaba

# ================== Config ==================
N_TOP_OPERADORES = 8      # para gráfico por operador/día (series)
N_TOP_CLIENTES   = 20     # slices en la torta de clientes
N_TOP_GENTE      = 20     # filas en ranking de operadores
PIE_METRIC       = "mensajes_totales"  # "mensajes_totales" o "mensajes_usuario"
INACTIVITY_MINUTES = 45                 # corte de sesión (min)
OPERATOR_TIME_ATTRIB = "equal"          # "equal" | "by_msgs" | "full"
WORKING_DAYS_PER_MONTH = 20   # días hábiles del mes
HOURS_PER_DAY = 8            # horas de jornada

# ================== Utils ==================
def strip_html(t):
    if not t: return ""
    t = unescape(t)
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    return t.strip()

def chunked(lst, n=200):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def parse_args():
    ap = argparse.ArgumentParser("Exportar WhatsApp (Discuss) desde Odoo a Excel con KPIs y gráficos")
    ap.add_argument("--desde", help="YYYY-MM-DD")
    ap.add_argument("--hasta", help="YYYY-MM-DD")
    ap.add_argument("--archivo", default="whatsapp_odoo.xlsx")
    return ap.parse_args()

def rango(args):
    tz = dt.timezone.utc
    if not args.desde and not args.hasta:
        hoy = dt.datetime.now(tz).date()
        ini = dt.datetime.combine(hoy, dt.time.min, tz)
        fin = dt.datetime.combine(hoy, dt.time.max, tz)
        return ini, fin
    ini = dt.datetime(1970,1,1,tzinfo=tz) if not args.desde else dt.datetime(*map(int,args.desde.split("-")), tzinfo=tz)
    fin = dt.datetime.now(tz) if not args.hasta else dt.datetime(*map(int,args.hasta.split("-")), 23,59,59, tzinfo=tz)
    return ini, fin

# ================== XML-RPC ==================
def odoo_connect():
    load_dotenv()
    url = os.getenv("ODOO_URL"); db = os.getenv("ODOO_DB")
    user = os.getenv("ODOO_USERNAME"); pwd = os.getenv("ODOO_PASSWORD")
    if not all([url, db, user, pwd]):
        print("Faltan ODOO_URL / ODOO_DB / ODOO_USERNAME / ODOO_PASSWORD en .env", file=sys.stderr); sys.exit(1)
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, pwd, {})
    if not uid:
        print("No pude autenticar en Odoo. Revisá credenciales.", file=sys.stderr); sys.exit(1)
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return db, uid, pwd, models

def model_exists(models, db, uid, pwd, name):
    ids = models.execute_kw(db, uid, pwd, "ir.model", "search", [[("model","=",name)]], {"limit":1})
    return bool(ids)

def fields_get(models, db, uid, pwd, model):
    return models.execute_kw(db, uid, pwd, model, "fields_get", [[], {"attributes": ["type"]}])

def search(models, db, uid, pwd, model, domain, order="id asc"):
    return models.execute_kw(db, uid, pwd, model, "search", [domain], {"order": order})

def read(models, db, uid, pwd, model, ids, fields):
    out = []
    for ch in chunked(ids, 200):
        out.extend(models.execute_kw(db, uid, pwd, model, "read", [ch], {"fields": fields}))
    return out

# ================== Excel helpers ==================
def xl_sheet(name, used):
    base = name[:31]
    if base not in used:
        used.add(base); return base
    i = 2
    while True:
        cand = (base[:31-len(f"~{i}")]) + f"~{i}"
        if cand not in used:
            used.add(cand); return cand
        i += 1

def autosize_and_style(ws, df, workbook):
    header_fmt = workbook.add_format({"bold": True})
    ws.set_row(0, None, header_fmt)
    if not df.empty:
        nrows, ncols = df.shape
        ws.autofilter(0, 0, nrows, ncols-1)
    ws.freeze_panes(1, 0)
    for idx, col in enumerate(df.columns):
        max_len = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).head(800)])
        max_len = min(max_len, 80)
        ws.set_column(idx, idx, max(10, max_len + 2))

# ================== Main ==================
def main():
    args = parse_args()
    ini_utc, fin_utc = rango(args)
    db, uid, pwd, models = odoo_connect()

    # Modelo canal
    channel_model = "discuss.channel" if model_exists(models, db, uid, pwd, "discuss.channel") else \
                    ("mail.channel" if model_exists(models, db, uid, pwd, "mail.channel") else None)
    if not channel_model:
        print("No existe discuss.channel ni mail.channel.", file=sys.stderr); sys.exit(1)

    ch_fields = fields_get(models, db, uid, pwd, channel_model)
    has_channel_type = "channel_type" in ch_fields
    channel_read_fields = ["id", "name", "create_date"] + (["channel_type"] if has_channel_type else [])

    # Partners internos
    user_ids = search(models, db, uid, pwd, "res.users", [("share","=",False)])
    users = read(models, db, uid, pwd, "res.users", user_ids, ["partner_id","name"])
    internal_partner_ids = {u["partner_id"][0] for u in users if u.get("partner_id")}

    # Mensajes del rango
    msg_domain = [
        ("model", "=", channel_model),
        ("date", ">=", ini_utc.strftime("%Y-%m-%d %H:%M:%S")),
        ("date", "<=", fin_utc.strftime("%Y-%m-%d %H:%M:%S")),
    ]
    msg_ids = search(models, db, uid, pwd, "mail.message", msg_domain, order="date asc")
    if not msg_ids:
        print("No hay mensajes en el rango.")
        with pd.ExcelWriter(args.archivo, engine="xlsxwriter") as w:
            used = set(); wb = w.book
            for shname in ["Chats","Mensajes","KPI_Msgs_dia_oper","KPI_Conv_dia_oper","KPI_Cliente","PeakHora_dia","KPI_Msgs_dia_oper_w","Cliente_top","Gente_top"]:
                df = pd.DataFrame()
                sh = xl_sheet(shname, used)
                df.to_excel(w, index=False, sheet_name=sh)
                autosize_and_style(w.sheets[sh], df, wb)

        print(f"Archivo creado: {args.archivo}")
        return

    msgs = read(models, db, uid, pwd, "mail.message", msg_ids,
                ["id","date","author_id","body","model","res_id","message_type","subtype_id"])

    # Canales
    chan_ids = sorted({m.get("res_id") for m in msgs if m.get("res_id")})
    channels = read(models, db, uid, pwd, channel_model, chan_ids, channel_read_fields)

    if has_channel_type:
        whatsapp_ids = {c["id"] for c in channels if c.get("channel_type") == "whatsapp"}
        channels = [c for c in channels if c["id"] in whatsapp_ids]
        msgs = [m for m in msgs if m.get("res_id") in whatsapp_ids]

    ch_map = {c["id"]: c.get("name","") for c in channels}

    # DataFrames base
    df_chats = pd.DataFrame([{
        "channel_id": c["id"], "canal": c.get("name",""),
        "create_date": c.get("create_date"),
        "channel_type": c.get("channel_type","") if has_channel_type else ""
    } for c in channels]).sort_values("create_date", ascending=False)

    rows_ms = []
    for m in msgs:
        cid = m.get("res_id")
        if cid not in ch_map: continue
        pid = m["author_id"][0] if m.get("author_id") else None
        rows_ms.append({
            "id": m.get("id"),
            "channel_id": cid, "canal": ch_map.get(cid,""),
            "fecha": m.get("date"),
            "autor": m["author_id"][1] if m.get("author_id") else "",
            "es_operador": (pid in internal_partner_ids) if pid else False,
            "mensaje": strip_html(m.get("body","")),
            "tipo": m.get("message_type",""),
            "subtipo": m.get("subtype_id")[1] if m.get("subtype_id") else "",
        })
    df_msgs = pd.DataFrame(rows_ms).sort_values(["channel_id","fecha"])
    if df_msgs.empty:
        with pd.ExcelWriter(args.archivo, engine="xlsxwriter") as w:
            used = set(); wb = w.book
            for name, df in [
                ("Chats", df_chats), ("Mensajes", df_msgs), ("KPI_Msgs_dia_oper", pd.DataFrame()),
                ("KPI_Conv_dia_oper", pd.DataFrame()), ("KPI_Cliente", pd.DataFrame()),
                ("PeakHora_dia", pd.DataFrame()), ("KPI_Msgs_dia_oper_w", pd.DataFrame()), ("Cliente_top", pd.DataFrame()), ("Gente_top", pd.DataFrame())
            ]:
                sh = xl_sheet(name, used); df.to_excel(w, index=False, sheet_name=sh); autosize_and_style(w.sheets[sh], df, wb)
        print(f"✅ Exportación lista (sin mensajes): {args.archivo}")
        return

    # Timestamps -> AR
    df_msgs["fecha_utc"] = pd.to_datetime(df_msgs["fecha"], utc=True, errors="coerce")
    df_msgs["fecha_ar"]  = df_msgs["fecha_utc"].dt.tz_convert("America/Argentina/Buenos_Aires")
    df_msgs["fecha_local"] = df_msgs["fecha_ar"].dt.date
    df_msgs["hora_local"]  = df_msgs["fecha_ar"].dt.strftime("%H:%M:%S")
    df_msgs["hora_int"]    = df_msgs["fecha_ar"].dt.hour

    #a# inicio agregado
    # ===== Sesiones por inactividad y tiempo por cliente =====
    df_s = df_msgs.sort_values(["channel_id", "fecha_ar"]).copy()
    df_s["session_break"] = (
        df_s.groupby("channel_id")["fecha_ar"]
            .diff().dt.total_seconds().div(60)
            .gt(INACTIVITY_MINUTES)
            .fillna(True)
    )
    df_s["session_id"] = df_s.groupby("channel_id")["session_break"].cumsum().astype(int)

    sessions = (
        df_s.groupby(["canal", "channel_id", "session_id"])
            .agg(
                inicio=("fecha_ar", "min"),
                fin=("fecha_ar", "max"),
                dur_min=("fecha_ar", lambda s: (s.max()-s.min()).total_seconds()/60.0),
                msgs=("id", "count"),
                hay_usuario=("es_operador", lambda s: (~s).any()),
                hay_operador=("es_operador", lambda s: (s).any()),
            )
            .reset_index()
    )
    sessions = sessions[(sessions["hay_usuario"]) & (sessions["hay_operador"])]

    tiempo_cliente = (
        sessions.groupby("canal")
            .agg(
                sesiones=("session_id", "nunique"),
                min_totales=("dur_min", "sum"),
                min_promedio=("dur_min", "mean"),
                min_max=("dur_min", "max"),
            )
            .reset_index()
            .sort_values("min_totales", ascending=False)
    )

    tiempo_cliente["min_totales"]  = tiempo_cliente["min_totales"].round(1)
    tiempo_cliente["min_promedio"] = tiempo_cliente["min_promedio"].round(1)
    tiempo_cliente["min_max"]      = tiempo_cliente["min_max"].round(1)

    top_time = tiempo_cliente[["canal", "min_totales"]].head(20).reset_index(drop=True)
    
    #a# fin agregado


    

    # ===== Sesiones por inactividad y tiempo por cliente =====
    # Ordenamos por canal y fecha local
    df_s = df_msgs.sort_values(["channel_id", "fecha_ar"]).copy()

    # Corte de sesión: diferencia de minutos > INACTIVITY_MINUTES
    df_s["session_break"] = (
        df_s.groupby("channel_id")["fecha_ar"]
            .diff().dt.total_seconds().div(60)
            .gt(INACTIVITY_MINUTES)
            .fillna(True)   # el primer mensaje de cada canal inicia sesión
    )

    # session_id incremental por canal
    df_s["session_id"] = df_s.groupby("channel_id")["session_break"].cumsum().astype(int)

    # Resumen de sesiones (contamos solo sesiones con cliente y operador)
    sessions = (
        df_s.groupby(["canal", "channel_id", "session_id"])
            .agg(
                inicio=("fecha_ar", "min"),
                fin=("fecha_ar", "max"),
                dur_min=("fecha_ar", lambda s: (s.max()-s.min()).total_seconds()/60.0),
                msgs=("id", "count"),
                hay_usuario=("es_operador", lambda s: (~s).any()),
                hay_operador=("es_operador", lambda s: (s).any()),
            )
            .reset_index()
    )
    sessions = sessions[(sessions["hay_usuario"]) & (sessions["hay_operador"])]

    # KPI por cliente (mes)
    tiempo_cliente = (
        sessions.groupby("canal")
            .agg(
                sesiones=("session_id", "nunique"),
                min_totales=("dur_min", "sum"),
                min_promedio=("dur_min", "mean"),
                min_max=("dur_min", "max"),
            )
            .reset_index()
            .sort_values("min_totales", ascending=False)
    )

    # Redondeos elegantes
    tiempo_cliente["min_totales"]  = tiempo_cliente["min_totales"].round(1)
    tiempo_cliente["min_promedio"] = tiempo_cliente["min_promedio"].round(1)
    tiempo_cliente["min_max"]      = tiempo_cliente["min_max"].round(1)

    # Top 20 clientes por tiempo
    top_time = tiempo_cliente[["canal", "min_totales"]].head(20).reset_index(drop=True)



    # KPI 1: mensajes por día/operador
    op_df = df_msgs[df_msgs["es_operador"] == True]
    kpi1 = (op_df.groupby(["fecha_local","autor"], as_index=False).agg(mensajes=("id","count"))
            .rename(columns={"autor":"operador"}).sort_values(["fecha_local","operador"])) if not op_df.empty \
            else pd.DataFrame(columns=["fecha_local","operador","mensajes"])

    # KPI 2: conversaciones por día/operador
    kpi2 = (op_df.groupby(["fecha_local","autor"])["channel_id"].nunique().reset_index()
            .rename(columns={"autor":"operador","channel_id":"conversaciones"})
            .sort_values(["fecha_local","operador"])) if not op_df.empty \
            else pd.DataFrame(columns=["fecha_local","operador","conversaciones"])

    # KPI Cliente
    kpi_cliente = (df_msgs.groupby("canal")
                   .agg(mensajes_usuario=("es_operador", lambda s: (~s).sum()),
                        mensajes_operador=("es_operador", lambda s: (s).sum()),
                        mensajes_totales=("id", "count"),
                        conversaciones=("channel_id", "nunique"))
                   .reset_index()
                   .sort_values(["mensajes_usuario","mensajes_totales"], ascending=[False, False]))

    # Peak hora por día (usuarios)
    usr_df = df_msgs[df_msgs["es_operador"] == False]
    peak_hora = (usr_df.groupby(["fecha_local","hora_int"], as_index=False).agg(mensajes_usuario=("id","count"))
                 .pipe(lambda d: d.loc[d.groupby("fecha_local")["mensajes_usuario"].idxmax()]
                       .sort_values("fecha_local")
                       .rename(columns={"hora_int":"hora_peak"}))) if not usr_df.empty \
                 else pd.DataFrame(columns=["fecha_local","hora_peak","mensajes_usuario"])


    # ===== Tiempo por OPERADOR (a partir de sessions) =====
    ops_per_sess = (
        df_s[df_s["es_operador"] == True]
        .groupby(["channel_id", "session_id", "autor"], as_index=False)
        .agg(msgs_oper=("id", "count"))
    )

    if not ops_per_sess.empty:
        # Unimos la duración de la sesión
        ops = ops_per_sess.merge(
            sessions[["channel_id", "session_id", "dur_min"]],
            on=["channel_id", "session_id"], how="left"
        )

        # Peso/participación del operador en la sesión
        if OPERATOR_TIME_ATTRIB == "by_msgs":
            tot_by_sess = ops.groupby(["channel_id", "session_id"])["msgs_oper"].transform("sum")
            weight = ops["msgs_oper"] / tot_by_sess.replace(0, 1)
        elif OPERATOR_TIME_ATTRIB == "full":
            weight = 1.0
        else:  # 'equal'
            n_ops = ops.groupby(["channel_id", "session_id"])["autor"].transform("nunique")
            weight = 1.0 / n_ops.replace(0, 1)

        ops["min_asignados"] = ops["dur_min"] * weight
        if not ops_per_sess.empty:
            # Unimos la duración de la sesión
            ops = ops_per_sess.merge(
                sessions[["channel_id", "session_id", "dur_min"]],
                on=["channel_id", "session_id"], how="left"
            )

            # Modo de reparto (usa la constante; si no existe, cae a 'equal')
            mode = OPERATOR_TIME_ATTRIB if 'OPERATOR_TIME_ATTRIB' in globals() else 'equal'
            if mode == "by_msgs":
                tot_by_sess = ops.groupby(["channel_id", "session_id"])["msgs_oper"].transform("sum")
                weight = ops["msgs_oper"] / tot_by_sess.replace(0, 1)
            elif mode == "full":
                weight = 1.0
            else:  # 'equal'
                n_ops = ops.groupby(["channel_id", "session_id"])["autor"].transform("nunique")
                weight = 1.0 / n_ops.replace(0, 1)

            ops["min_asignados"] = ops["dur_min"] * weight

            # ---- KPI por operador ----
            tiempo_operador = (
                ops.groupby("autor", as_index=False)
                .agg(sesiones=("session_id", "nunique"),
                        min_totales=("min_asignados", "sum"),
                        min_promedio=("min_asignados", "mean"),
                        min_max=("min_asignados", "max"))
                .rename(columns={"autor": "operador"})
                .sort_values("min_totales", ascending=False)
            )
            tiempo_operador[["min_totales","min_promedio","min_max"]] = \
                tiempo_operador[["min_totales","min_promedio","min_max"]].round(1)

            # ---- NUEVAS columnas en horas y promedios ----
            # (asegurate de tener definidas WORKING_DAYS_PER_MONTH y HOURS_PER_DAY arriba)
            tiempo_operador["total_horas"] = (tiempo_operador["min_totales"] / 60).round(2)
            tiempo_operador["promedio_horas_diaria"] = (tiempo_operador["total_horas"] / WORKING_DAYS_PER_MONTH).round(2)
            tiempo_operador["fte_equivalente"] = (
                tiempo_operador["total_horas"] / (WORKING_DAYS_PER_MONTH * HOURS_PER_DAY)
            ).round(2)
            tiempo_operador["carga_promedio_diaria"] = (
                tiempo_operador["promedio_horas_diaria"] / HOURS_PER_DAY
            ).round(2)

            # ---- TOP para la hoja y el gráfico ----
            gente_top_tiempo = (
                tiempo_operador.loc[:, [
                    "operador",
                    "min_totales",
                    "total_horas",
                    "promedio_horas_diaria",
                    "fte_equivalente",
                    "carga_promedio_diaria",
                ]]
                .head(N_TOP_GENTE)
                .reset_index(drop=True)
            )
        else:
            tiempo_operador = pd.DataFrame(columns=[
                "operador","sesiones","min_totales","min_promedio","min_max",
                "total_horas","promedio_horas_diaria","fte_equivalente","carga_promedio_diaria"
            ])
            gente_top_tiempo = tiempo_operador.loc[:, [
                "operador","min_totales","total_horas","promedio_horas_diaria","fte_equivalente","carga_promedio_diaria"
            ]].copy()


        # Top 20 por minutos
        gente_top_tiempo = tiempo_operador[["operador", "min_totales"]].head(20).reset_index(drop=True)
    else:
        tiempo_operador = pd.DataFrame(columns=["operador", "sesiones", "min_totales", "min_promedio", "min_max"])
        gente_top_tiempo = pd.DataFrame(columns=["operador", "min_totales"])

    # ---- columnas en horas y promedios (operador) ----
    tiempo_operador["total_horas"] = (tiempo_operador["min_totales"] / 60).round(2)
    tiempo_operador["promedio_horas_diaria"] = (tiempo_operador["total_horas"] / WORKING_DAYS_PER_MONTH).round(2)

    # extras útiles para staffing:
    tiempo_operador["fte_equivalente"] = (tiempo_operador["total_horas"] / (WORKING_DAYS_PER_MONTH * HOURS_PER_DAY)).round(2)
    tiempo_operador["carga_promedio_diaria"] = (tiempo_operador["promedio_horas_diaria"] / HOURS_PER_DAY).round(2)

    # ===== Staffing: resumen mensual a partir de tiempo_operador =====
    HOURS_PER_MONTH = WORKING_DAYS_PER_MONTH * HOURS_PER_DAY

    if not tiempo_operador.empty:
        total_hours_all = float(tiempo_operador["total_horas"].sum())
        operadores_activos = int((tiempo_operador["min_totales"] > 0).sum())
        capacidad_horas_mes = operadores_activos * HOURS_PER_MONTH

        fte_requeridos = round(total_hours_all / HOURS_PER_MONTH, 2) if HOURS_PER_MONTH > 0 else 0.0
        fte_actuales = float(operadores_activos)  # si todos a tiempo completo
        brecha_fte = round(fte_actuales - fte_requeridos, 2)
        brecha_personas = operadores_activos - math.ceil(fte_requeridos)

        ocupacion_promedio = round(total_hours_all / capacidad_horas_mes, 2) if capacidad_horas_mes > 0 else 0.0
        horas_promedio_diaria_por_persona = round(
            total_hours_all / max(1, operadores_activos) / WORKING_DAYS_PER_MONTH, 2
        )

        staffing_resumen = pd.DataFrame([{
            "dias_habiles": WORKING_DAYS_PER_MONTH,
            "horas_dia": HOURS_PER_DAY,
            "horas_mes_por_persona": HOURS_PER_MONTH,
            "operadores_activos": operadores_activos,
            "fte_requeridos": fte_requeridos,
            "fte_actuales": fte_actuales,
            "brecha_fte": brecha_fte,                         # +: sobra capacidad, -: falta
            "brecha_personas": brecha_personas,               # +: sobran personas, -: faltan
            "horas_totales_mes": round(total_hours_all, 2),
            "capacidad_horas_mes": capacidad_horas_mes,
            "ocupacion_promedio": ocupacion_promedio,         # 0–1 (ej. 0.75 = 75%)
            "horas_promedio_diaria_por_persona": horas_promedio_diaria_por_persona
        }])
    else:
        staffing_resumen = pd.DataFrame(columns=[
            "dias_habiles","horas_dia","horas_mes_por_persona","operadores_activos",
            "fte_requeridos","fte_actuales","brecha_fte","brecha_personas",
            "horas_totales_mes","capacidad_horas_mes","ocupacion_promedio","horas_promedio_diaria_por_persona"
        ])

    # === KPI_Cliente_Gral (sesiones + minutos + horas + mensajes) ===
    kpi_cliente_gral = tiempo_cliente.merge(
        kpi_cliente[["canal", "mensajes_usuario", "mensajes_operador", "mensajes_totales"]],
        on="canal", how="left"
    )
    kpi_cliente_gral["Horas"] = (kpi_cliente_gral["min_totales"] / 60).round(2)

    # Orden de columnas como la captura
    kpi_cliente_gral = kpi_cliente_gral[
        ["canal", "sesiones", "Horas", "min_totales", "min_promedio", "min_max",
        "mensajes_usuario", "mensajes_operador", "mensajes_totales"]
    ]

    # === KPI_Operador_Gral ===
    # mensajes de USUARIO por sesión (para asignarlos al operador según el modo elegido)
    sess_user_msgs = (
        df_s[df_s["es_operador"] == False]
        .groupby(["channel_id", "session_id"], as_index=False)
        .agg(msgs_user=("id", "count"))
    )

    # Detalle del operador por sesión + duración + msgs de usuario
    ops_detail = ops_per_sess.merge(
        sessions[["channel_id", "session_id", "dur_min"]],
        on=["channel_id", "session_id"], how="left"
    ).merge(
        sess_user_msgs, on=["channel_id", "session_id"], how="left"
    ).fillna({"msgs_user": 0})

    # Peso de asignación igual que para tiempo
    mode = OPERATOR_TIME_ATTRIB if 'OPERATOR_TIME_ATTRIB' in globals() else 'equal'
    if mode == "by_msgs":
        tot_by_sess = ops_detail.groupby(["channel_id", "session_id"])["msgs_oper"].transform("sum")
        weight = ops_detail["msgs_oper"] / tot_by_sess.replace(0, 1)
    elif mode == "full":
        weight = 1.0
    else:  # 'equal'
        n_ops = ops_detail.groupby(["channel_id", "session_id"])["autor"].transform("nunique")
        weight = 1.0 / n_ops.replace(0, 1)

    ops_detail["msgs_usuario_asig"] = ops_detail["msgs_user"] * weight

    # --- agregados SOLO de mensajes (no incluimos 'sesiones' para evitar colisión) ---
    agg_msgs_op = (
        ops_detail.groupby("autor", as_index=False)
        .agg(mensajes_operador=("msgs_oper", "sum"),
            mensajes_usuario=("msgs_usuario_asig", "sum"))
    )

    # Merge con 'tiempo_operador' (que ya tiene 'sesiones', minutos, etc.)
    kpi_operador_gral = tiempo_operador.merge(
        agg_msgs_op, left_on="operador", right_on="autor", how="left"
    ).drop(columns=["autor"])

    # Rellenos y tipos
    kpi_operador_gral["mensajes_operador"] = kpi_operador_gral["mensajes_operador"].fillna(0).astype(int)
    kpi_operador_gral["mensajes_usuario"]  = kpi_operador_gral["mensajes_usuario"].fillna(0).round(0).astype(int)
    kpi_operador_gral["mensajes_totales"]  = kpi_operador_gral["mensajes_operador"] + kpi_operador_gral["mensajes_usuario"]

    # Horas y orden de columnas
    kpi_operador_gral["Horas"] = (kpi_operador_gral["min_totales"] / 60).round(2)
    kpi_operador_gral = kpi_operador_gral[
        ["operador", "sesiones", "Horas", "min_totales", "min_promedio", "min_max",
        "mensajes_usuario", "mensajes_operador", "mensajes_totales"]
    ].sort_values("min_totales", ascending=False).reset_index(drop=True)



    # ============ Guardar + Gráficos ============
    with pd.ExcelWriter(args.archivo, engine="xlsxwriter") as w:
        used = set(); wb = w.book
        def write_sheet(df, name):
            sh = xl_sheet(name, used); df.to_excel(w, index=False, sheet_name=sh); autosize_and_style(w.sheets[sh], df, wb); return sh

        # 1) PRIMERA HOJA: KPI_Cliente_Gral
        sh_cli_gral = write_sheet(kpi_cliente_gral, "KPI_Cliente_Gral")
        sh_op_gral = write_sheet(kpi_operador_gral, "KPI_Operador_Gral")
        sh_chats = write_sheet(df_chats, "Chats")
        sh_msgs  = write_sheet(df_msgs.drop(columns=["fecha_utc","fecha_ar"], errors="ignore"), "Mensajes")
        sh_kpi1  = write_sheet(kpi1, "KPI_Msgs_dia_oper")
        sh_kpi2  = write_sheet(kpi2, "KPI_Conv_dia_oper")
        sh_peak  = write_sheet(peak_hora, "PeakHora_dia")

        # 1) Gráficos en PeakHora_dia
        ws_peak = w.sheets[sh_peak]; n = len(peak_hora)
        if n > 0:
            chart_hora = wb.add_chart({'type': 'line'})
            chart_hora.add_series({'name':'Hora pico (0–23)','categories':[sh_peak,1,0,n,0],'values':[sh_peak,1,1,n,1],'marker':{'type':'circle'}})
            chart_hora.set_title({'name':'Hora pico por día'}); chart_hora.set_x_axis({'name':'Fecha'})
            chart_hora.set_y_axis({'name':'Hora','min':0,'max':23,'major_unit':1})
            ws_peak.insert_chart('E2', chart_hora, {'x_scale':1.6,'y_scale':1.2})

            chart_msgs = wb.add_chart({'type': 'column'})
            chart_msgs.add_series({'name':'Mensajes (usuarios)','categories':[sh_peak,1,0,n,0],'values':[sh_peak,1,2,n,2]})
            chart_msgs.set_title({'name':'Mensajes de usuarios por día'}); chart_msgs.set_x_axis({'name':'Fecha'}); chart_msgs.set_y_axis({'name':'# mensajes'})
            ws_peak.insert_chart('E20', chart_msgs, {'x_scale':1.6,'y_scale':1.2})

        # 2) Barras agrupadas por operador/día (Top)
        if not kpi1.empty:
            kpi1_w = (kpi1.pivot_table(index='fecha_local', columns='operador', values='mensajes', aggfunc='sum').fillna(0).sort_index())
            tot = kpi1_w.sum(axis=0).sort_values(ascending=False)
            top_ops = tot.index.tolist()[:N_TOP_OPERADORES]
            kpi1_w_top = kpi1_w[top_ops].copy()
            otros_cols = [c for c in kpi1_w.columns if c not in top_ops]
            if otros_cols: kpi1_w_top['Otros'] = kpi1_w[otros_cols].sum(axis=1)
            kpi1_w_out = kpi1_w_top.reset_index()
            sh_kpi1w = write_sheet(kpi1_w_out, "KPI_Msgs_dia_oper_w")
            ws_w = w.sheets[sh_kpi1w]; rows = len(kpi1_w_out); cols = kpi1_w_out.shape[1]
            chart_ops = wb.add_chart({'type':'column'})
            for col in range(1, cols):
                chart_ops.add_series({'name':[sh_kpi1w,0,col],'categories':[sh_kpi1w,1,0,rows,0],'values':[sh_kpi1w,1,col,rows,col]})
            chart_ops.set_title({'name':'Mensajes por operador y día (Top)'}); chart_ops.set_x_axis({'name':'Fecha'}); chart_ops.set_y_axis({'name':'# mensajes'})
            chart_ops.set_legend({'position':'bottom'})
            ws_w.insert_chart('E2', chart_ops, {'x_scale':1.8,'y_scale':1.4})

        # 3) Torta Cliente_top (Top 20 + Otros)
        if not kpi_cliente.empty:
            kc = kpi_cliente[['canal', PIE_METRIC]].sort_values(PIE_METRIC, ascending=False)
            top = kc.head(N_TOP_CLIENTES).copy()
            if len(kc) > N_TOP_CLIENTES:
                otros_val = kc[PIE_METRIC].iloc[N_TOP_CLIENTES:].sum()
                top = pd.concat([top, pd.DataFrame([{'canal':'Otros', PIE_METRIC: otros_val}])], ignore_index=True)
            sh_top = write_sheet(top, "Cliente_top")
            ws_top = w.sheets[sh_top]; r = len(top)
            pie = wb.add_chart({'type': 'pie'})
            pie.add_series({'name': f"Top clientes por {PIE_METRIC}", 'categories': [sh_top,1,0,r,0], 'values': [sh_top,1,1,r,1], 'data_labels': {'percentage': True, 'value': True}})
            pie.set_title({'name': '¿A quién atendí más?'}); ws_top.insert_chart('D2', pie, {'x_scale':1.5, 'y_scale':1.5})

        # 4) Gente_top (operadores por mensajes totales)
        if not op_df.empty:
            gente_top = (op_df.groupby('autor', as_index=False)
                         .agg(mensajes_totales=('id','count'))
                         .sort_values('mensajes_totales', ascending=False)
                         .head(N_TOP_GENTE)
                         .rename(columns={'autor':'operador'}))
        else:
            gente_top = pd.DataFrame(columns=['operador','mensajes_totales'])

        sh_gente = write_sheet(gente_top, "Gente_top")
        ws_g = w.sheets[sh_gente]; r = len(gente_top)
        if r > 0:
            bar = wb.add_chart({'type':'column'})
            bar.add_series({'name':'Mensajes totales por operador', 'categories':[sh_gente,1,0,r,0], 'values':[sh_gente,1,1,r,1]})
            bar.set_title({'name':'¿Quién atendió más?'})
            bar.set_x_axis({'name':'Operador'}); bar.set_y_axis({'name':'# mensajes'})
            bar.set_legend({'none': True})
            ws_g.insert_chart('D2', bar, {'x_scale':1.6, 'y_scale':1.5})

        # === Hojas nuevas con tiempo por cliente ===
        sh_kpi_tiempo = write_sheet(tiempo_cliente, "KPI_Tiempo_cliente")
        sh_top_tiempo = write_sheet(top_time,       "Cliente_top_tiempo")

        # === Gráficos de tiempo por cliente ===
        ws_tt = w.sheets[sh_top_tiempo]
        r = len(top_time)
        if r > 0:
            pie_t = wb.add_chart({'type': 'pie'})
            pie_t.add_series({
                'name':       'Top clientes por tiempo (min)',
                'categories': [sh_top_tiempo, 1, 0, r, 0],
                'values':     [sh_top_tiempo, 1, 1, r, 1],
                'data_labels': {'percentage': True, 'value': True},
            })
            pie_t.set_title({'name': 'Clientes con más tiempo de atención'})
            ws_tt.insert_chart('D2', pie_t, {'x_scale': 1.4, 'y_scale': 1.4})

            bar_t = wb.add_chart({'type': 'column'})
            bar_t.add_series({
                'name':       'Minutos totales',
                'categories': [sh_top_tiempo, 1, 0, r, 0],
                'values':     [sh_top_tiempo, 1, 1, r, 1],
            })
            bar_t.set_title({'name': 'Top 20 por minutos'})
            bar_t.set_x_axis({'name': 'Cliente'})
            bar_t.set_y_axis({'name': 'Minutos'})
            ws_tt.insert_chart('D20', bar_t, {'x_scale': 1.6, 'y_scale': 1.2})


        # === Tiempo por OPERADOR ===
        sh_tiempo_op = write_sheet(tiempo_operador, "KPI_Tiempo_operador")
        sh_top_op_t  = write_sheet(gente_top_tiempo, "Gente_top_tiempo")

        # Gráfico: ¿Quién atendió más (minutos)?
        ws_gt = w.sheets[sh_top_op_t]
        r = len(gente_top_tiempo)
        if r > 0:
            bar_op_t = wb.add_chart({'type': 'column'})
            bar_op_t.add_series({
                'name':       'Minutos totales por operador',
                'categories': [sh_top_op_t, 1, 0, r, 0],
                'values':     [sh_top_op_t, 1, 1, r, 1],
            })
            bar_op_t.add_series({
                'name':       'Horas totales por operador',
                'categories': [sh_top_op_t, 1, 0, r, 0],  # operador
                'values':     [sh_top_op_t, 1, 2, r, 2],  # total_horas
            })

            bar_op_t.set_title({'name': '¿Quién atendió más (minutos)?'})
            bar_op_t.set_x_axis({'name': 'Operador'})
            bar_op_t.set_y_axis({'name': 'Minutos'})
            bar_op_t.set_legend({'none': True})
            ws_gt.insert_chart('D2', bar_op_t, {'x_scale': 1.6, 'y_scale': 1.4})

        # === Hoja de staffing ===
        sh_staff = write_sheet(staffing_resumen, "Staffing_resumen")

        # === Mini tabla auxiliar para gráfico FTE (en la hoja de staffing) ===
        ws_staff = w.sheets[sh_staff]
        # La tabla se escribirá a la derecha de la data (columna H en adelante para no pisar nada)
        base_row = 1
        base_col = 8  # Columna I (0-index)
        ws_staff.write(base_row,     base_col,     "Métrica")
        ws_staff.write(base_row,     base_col + 1, "Valor")
        ws_staff.write(base_row + 1, base_col,     "FTE requeridos")
        ws_staff.write(base_row + 2, base_col,     "FTE actuales")

        # Tomamos los valores desde el DataFrame (fila 0)
        if not staffing_resumen.empty:
            fte_req = float(staffing_resumen["fte_requeridos"].iloc[0])
            fte_act = float(staffing_resumen["fte_actuales"].iloc[0])
        else:
            fte_req = 0.0; fte_act = 0.0

        ws_staff.write(base_row + 1, base_col + 1, fte_req)
        ws_staff.write(base_row + 2, base_col + 1, fte_act)

        # === Gráfico de columnas FTE requeridos vs actuales ===
        chart_fte = wb.add_chart({'type': 'column'})
        chart_fte.add_series({
            'name':       'FTE',
            'categories': [sh_staff, base_row + 1, base_col, base_row + 2, base_col],     # Métricas
            'values':     [sh_staff, base_row + 1, base_col + 1, base_row + 2, base_col + 1],  # Valores
        })
        chart_fte.set_title({'name': 'FTE requeridos vs actuales'})
        chart_fte.set_x_axis({'name': ''})
        chart_fte.set_y_axis({'name': 'FTE'})
        chart_fte.set_legend({'none': True})
        ws_staff.insert_chart('I8', chart_fte, {'x_scale': 1.4, 'y_scale': 1.2})

            
    print(f"✅ Exportación lista con gráficos: {args.archivo}")
    print("   Hojas: Chats, Mensajes, KPI_Msgs_dia_oper, KPI_Conv_dia_oper, KPI_Cliente, PeakHora_dia, KPI_Msgs_dia_oper_w, Cliente_top, Gente_top")

if __name__ == "__main__":
    main()
