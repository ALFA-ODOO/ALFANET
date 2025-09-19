# -*- coding: utf-8 -*-
"""Genera reportes de KPIs de Helpdesk en Excel a partir de datos de Odoo.

Conecta vía XML-RPC, obtiene tickets, partes de horas y calcula métricas por
cliente y operador en un rango de fechas configurable. El resultado se exporta
a un archivo de Excel con hojas y estilos listos para análisis.
"""

import os, sys, argparse, datetime as dt, xmlrpc.client

import pandas as pd
from dotenv import load_dotenv

# ================= Config =================
WORKING_DAYS_PER_MONTH = 20
HOURS_PER_DAY = 8

# ================ Utils ===================
def parse_args():
    ap = argparse.ArgumentParser("KPIs Helpdesk (Cliente/Operador) → Excel")
    ap.add_argument("--desde", help="YYYY-MM-DD (por defecto, hoy)")
    ap.add_argument("--hasta", help="YYYY-MM-DD (por defecto, hoy)")
    ap.add_argument("--archivo", default="helpdesk_kpis.xlsx")
    ap.add_argument("--usar_cierre", action="store_true",
                    help="Filtra por fecha de cierre en vez de creación (si el campo existe).")
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
    return db, uid, pwd, models

def model_exists(models, db, uid, pwd, name):
    return bool(models.execute_kw(db, uid, pwd, "ir.model", "search", [[("model","=",name)]], {"limit":1}))

def fields_get(models, db, uid, pwd, model):
    return models.execute_kw(db, uid, pwd, model, "fields_get", [[], {"attributes": ["type"]}])

def search(models, db, uid, pwd, model, domain, order="id asc", limit=None):
    kw = {"order": order}
    if limit: kw["limit"] = limit
    return models.execute_kw(db, uid, pwd, model, "search", [domain], kw)

def read(models, db, uid, pwd, model, ids, fields):
    out = []
    for i in range(0, len(ids), 200):
        out.extend(models.execute_kw(db, uid, pwd, model, "read", [ids[i:i+200]], {"fields": fields}))
    return out

def xl_autostyle(ws, df, wb):
    hdr = wb.add_format({"bold": True})
    ws.set_row(0, None, hdr); ws.freeze_panes(1,0)
    if not df.empty:
        ws.autofilter(0,0, df.shape[0], df.shape[1]-1)
    for idx, col in enumerate(df.columns):
        width = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).head(800)])
        ws.set_column(idx, idx, min(max(10, width+2), 80))

# ================ Principal =================
def main():
    args = parse_args()
    ini_utc, fin_utc = rango(args)
    db, uid, pwd, models = odoo_connect()

    # --------- Helpdesk: tickets ----------
    if not model_exists(models, db, uid, pwd, "helpdesk.ticket"):
        print("Tu base no tiene helpdesk.ticket.", file=sys.stderr); sys.exit(1)

    t_fields = fields_get(models, db, uid, pwd, "helpdesk.ticket")
    close_field = "close_date" if "close_date" in t_fields else ("closed_date" if "closed_date" in t_fields else None)

    if args.usar_cierre and close_field:
        dom = [(close_field, ">=", ini_utc.strftime("%Y-%m-%d %H:%M:%S")),
               (close_field, "<=", fin_utc.strftime("%Y-%m-%d %H:%M:%S"))]
    else:
        dom = [("create_date", ">=", ini_utc.strftime("%Y-%m-%d %H:%M:%S")),
               ("create_date", "<=", fin_utc.strftime("%Y-%m-%d %H:%M:%S"))]

    t_ids = search(models, db, uid, pwd, "helpdesk.ticket", dom, order="create_date asc")
    if not t_ids:
        print("No hay tickets en el rango.")
        with pd.ExcelWriter(args.archivo, engine="xlsxwriter") as w:
            wb = w.book
            for name in ["KPI_Cliente_Gral","KPI_Operador_Gral"]:
                df = pd.DataFrame()
                df.to_excel(w, index=False, sheet_name=name)
                xl_autostyle(w.sheets[name], df, wb)
        print(f"Archivo creado: {args.archivo}")
        return

   # Antes de leer tickets, ya tenés: t_fields = fields_get(..., "helpdesk.ticket")
    read_ticket_fields = ["id","name","partner_id","user_id","create_date"]
    if close_field:
        read_ticket_fields.append(close_field)
    if "task_id" in t_fields:          # <-- NUEVO
        read_ticket_fields.append("task_id")

    t_read = read(models, db, uid, pwd, "helpdesk.ticket", t_ids, read_ticket_fields)

    df_t = pd.DataFrame(t_read)
    df_t["cliente"]  = df_t["partner_id"].apply(lambda v: v[1] if isinstance(v, list) else ("Sin cliente" if not v else str(v)))
    df_t["operador"] = df_t["user_id"].apply(lambda v: v[1] if isinstance(v, list) else "")
    df_t["create_dt"] = pd.to_datetime(df_t["create_date"], utc=True, errors="coerce")
    df_t["close_dt"]  = pd.to_datetime(df_t[close_field], utc=True, errors="coerce") if close_field else pd.NaT

    # Mapeo ticket -> task (si existe)
    if "task_id" in df_t.columns:
        df_t["task_id"] = df_t["task_id"].apply(lambda v: v[0] if isinstance(v, list) else (v if pd.notna(v) else None))
    else:
        df_t["task_id"] = None


    # --------- Timesheets (si existen) ----------
    has_timesheet_model = model_exists(models, db, uid, pwd, "account.analytic.line")
    df_ts = pd.DataFrame()

    if has_timesheet_model:
        ts_fields = fields_get(models, db, uid, pwd, "account.analytic.line")
        # Detectar el campo de vínculo disponible
        if "helpdesk_ticket_id" in ts_fields:
            link_field = "helpdesk_ticket_id"
            ids_list = t_ids
            dom_ts = [(link_field, "in", ids_list),
                    ("date", ">=", ini_utc.date().isoformat()),
                    ("date", "<=", fin_utc.date().isoformat())]
            read_ts_fields = ["id", link_field, "unit_amount", "user_id", "date"]
        elif "ticket_id" in ts_fields:
            link_field = "ticket_id"
            ids_list = t_ids
            dom_ts = [(link_field, "in", ids_list),
                    ("date", ">=", ini_utc.date().isoformat()),
                    ("date", "<=", fin_utc.date().isoformat())]
            read_ts_fields = ["id", link_field, "unit_amount", "user_id", "date"]
        elif "task_id" in ts_fields and "task_id" in df_t.columns:
            link_field = "task_id"
            ids_list = df_t["task_id"].dropna().unique().tolist()
            if ids_list:
                dom_ts = [(link_field, "in", ids_list),
                        ("date", ">=", ini_utc.date().isoformat()),
                        ("date", "<=", fin_utc.date().isoformat())]
                read_ts_fields = ["id", link_field, "unit_amount", "user_id", "date"]
            else:
                link_field = None
        else:
            link_field = None

        if link_field:
            ts_ids = search(models, db, uid, pwd, "account.analytic.line", dom_ts, order="date asc")
            if ts_ids:
                ts = read(models, db, uid, pwd, "account.analytic.line", ts_ids, read_ts_fields)
                df_ts = pd.DataFrame(ts)
                # Normalizar id del vínculo a entero
                df_ts[link_field] = df_ts[link_field].apply(lambda v: v[0] if isinstance(v, list) else v)
                df_ts["unit_amount"] = pd.to_numeric(df_ts["unit_amount"], errors="coerce").fillna(0.0)
                df_ts["operador"] = df_ts["user_id"].apply(lambda v: v[1] if isinstance(v, list) else "")

    # --------- Tiempo por ticket (horas) ----------
    horas_por_ticket = pd.Series(0.0, index=df_t["id"])

    # Si tenemos partes (timesheets) y detectamos el campo de enlace:
    if not df_ts.empty and 'link_field' in locals() and link_field:
        # Asegurar que el campo esté en df_ts y ya normalizado a enteros/IDs
        if link_field not in df_ts.columns:
            # Debug opcional:
            # print("df_ts cols:", df_ts.columns.tolist())
            link_field = None  # para forzar fallback
        else:
            h_ts_by_key = df_ts.groupby(link_field)["unit_amount"].sum()

            if link_field in ("helpdesk_ticket_id", "ticket_id"):
                # Mapeo directo: ticket -> horas
                # Alinear por índice (id de ticket)
                horas_por_ticket = horas_por_ticket.add(
                    h_ts_by_key.reindex(df_t["id"]).fillna(0.0), fill_value=0.0
                )
            elif link_field == "task_id":
                # Mapeo indirecto: ticket -> task -> horas
                task_to_hours = h_ts_by_key.to_dict()
                horas_ticket_via_task = df_t["task_id"].map(lambda t: task_to_hours.get(t, 0.0))
                horas_por_ticket = horas_por_ticket.add(horas_ticket_via_task, fill_value=0.0)

    # Fallback por duración (si el ticket quedó en 0 horas)
    dur_horas = ((df_t["close_dt"] - df_t["create_dt"]).dt.total_seconds()/3600.0).fillna(0.0).clip(lower=0)
    horas_por_ticket = horas_por_ticket.where(horas_por_ticket > 0, dur_horas)

    df_t["horas"] = horas_por_ticket.round(2)


    # --------- KPI Cliente ---------
    kpi_cliente = (df_t.groupby("cliente", as_index=False)
                      .agg(tickets=("id","count"),
                           horas=("horas","sum"))
                   .sort_values(["horas","tickets"], ascending=[False, False]))
    kpi_cliente["horas"] = kpi_cliente["horas"].round(2)

    # --------- KPI Operador ---------
    if not df_ts.empty and 'link_field' in locals() and link_field:
        df_ts2 = df_ts.copy()

        # 1) Normalizamos columna ticket_id en df_ts2
        if link_field in ("helpdesk_ticket_id", "ticket_id"):
            df_ts2["ticket_id"] = df_ts2[link_field]
        elif link_field == "task_id":
            # map task -> ticket con df_t (df_t tiene columnas: id (ticket), task_id)
            map_task_to_ticket = (
                df_t.dropna(subset=["task_id"])
                .drop_duplicates("task_id")[["task_id","id"]]
                .rename(columns={"id":"ticket_id"})
            )
            df_ts2 = df_ts2.merge(map_task_to_ticket, on="task_id", how="left")
        else:
            # si no pudimos detectar/normalizar, evitamos romper y contamos por link_field
            df_ts2["ticket_id"] = pd.NA

        # 2) Agregado por operador
        # - horas: suma de unit_amount
        # - tickets: cantidad de tickets únicos (si no hay ticket_id, cae a 0)
        if "ticket_id" in df_ts2.columns:
            tickets_agg = ("ticket_id", lambda s: pd.Series(s).nunique())
        else:
            tickets_agg = (link_field, lambda s: pd.Series(s).nunique())

        kpi_operador = (
            df_ts2.groupby("operador", as_index=False)
                .agg(horas=("unit_amount","sum"),
                    tickets=tickets_agg)
        )
        kpi_operador["tickets"] = kpi_operador["tickets"].fillna(0).astype(int)
        kpi_operador["horas"] = kpi_operador["horas"].round(2)
        kpi_operador = kpi_operador.sort_values(["horas","tickets"], ascending=[False, False])

    else:
        # Fallback: sin partes o sin link_field -> usamos asignación del ticket
        kpi_operador = (
            df_t.groupby("operador", as_index=False)
                .agg(horas=("horas","sum"),
                    tickets=("id","count"))
                .sort_values(["horas","tickets"], ascending=[False, False])
        )
        kpi_operador["horas"] = kpi_operador["horas"].round(2)


    # --------- Guardar Excel (solo 2 hojas) ---------
    with pd.ExcelWriter(args.archivo, engine="xlsxwriter") as w:
        wb = w.book
        kpi_cliente.to_excel(w, index=False, sheet_name="KPI_Cliente_Gral")
        xl_autostyle(w.sheets["KPI_Cliente_Gral"], kpi_cliente, wb)

        kpi_operador.to_excel(w, index=False, sheet_name="KPI_Operador_Gral")
        xl_autostyle(w.sheets["KPI_Operador_Gral"], kpi_operador, wb)

    print(f"✅ Listo: {args.archivo}")
    print(f"   Clientes: {len(kpi_cliente)} | Operadores: {len(kpi_operador)}")
    if not df_ts.empty:
        print("   Fuente de horas: Timesheets de Helpdesk.")
    else:
        print("   Fuente de horas: Duración estimada (creación → cierre). Activá Timesheets para más precisión.")
        
if __name__ == "__main__":
    main()
