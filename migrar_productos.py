# -*- coding: utf-8 -*-
import pyodbc
import xmlrpc.client
import base64
import os
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

inicio_proceso = datetime.datetime.now()
print(f"Inicio de la carga de artículos (con categorías): {inicio_proceso.strftime('%Y-%m-%d %H:%M:%S')}")

MAP_UNIDADES = {"UN": 1, "KG": 15, "GR": 111, "LT": 12, "M": 8, "CM": 7, "MM": 6, "PA": 117, "CA": 116, "BL": 107, "CJ": 116, "PZ": 1}
carpeta_imagenes = r"C:\\Alfa Gestion\\Imagenes\\ImagenesWeb"

# --- Conexión Odoo ---
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
# La instancia de ServerProxy no es thread-safe, por lo que se crea una
# única para las operaciones iniciales y se generan nuevas por hilo cuando
# se procesan los productos.
_models_initial = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")


thread_local = threading.local()


def get_models():
    """Devuelve un proxy de modelos por hilo para evitar errores de concurrencia."""
    if not hasattr(thread_local, "models"):
        thread_local.models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return thread_local.models

# Obtener categorías existentes una vez
categorias_odoo = _models_initial.execute_kw(
    db, uid, password,
    'product.category', 'search_read',
    [[]],
    {'fields': ['id', 'name'], 'limit': 0}
)
map_categorias = {c['name'].strip().lower(): c['id'] for c in categorias_odoo}

# Obtener productos existentes (nivel variante) para acelerar la búsqueda
# Usamos product.product para incluir referencias y códigos de barras
# definidos a nivel de variante y mapearlos a la plantilla correspondiente.
productos_existentes = _models_initial.execute_kw(
    db,
    uid,
    password,
    "product.product",
    "search_read",
    [[]],
    {
        "fields": ["id", "product_tmpl_id", "default_code", "barcode"],
        "limit": 0,
        "context": {"active_test": False},
    },
)
map_productos = {
    p["default_code"].strip(): p["product_tmpl_id"][0]
    for p in productos_existentes
    if p.get("default_code")
}
map_barcodes = {
    p["barcode"].strip(): p["product_tmpl_id"][0]
    for p in productos_existentes
    if p.get("barcode")
}

# --- Conexión SQL Server ---
sql_conn = pyodbc.connect(
    f"DRIVER={sql_server['driver']};"
    f"SERVER={sql_server['server']};"
    f"DATABASE={sql_server['database']};"
    f"UID={sql_server['user']};"
    f"PWD={sql_server['password']}"
)
cursor = sql_conn.cursor()

cursor.execute("SELECT TOP 1 MONEDA2, MONEDA3, MONEDA4, MONEDA5 FROM TA_COTIZACION ORDER BY ID DESC")
tasas = cursor.fetchone()
tasas_conversion = {"2": float(tasas[0] or 1), "3": float(tasas[1] or 1), "4": float(tasas[2] or 1), "5": float(tasas[3] or 1)}

# --- Consulta de productos ---
cursor.execute("""
    SELECT
        a.IDARTICULO,
        a.DESCRIPCION,
        a.IDUNIDAD,
        LTRIM(RTRIM(a.CODIGOBARRA)) AS CODIGOBARRA,
        a.TasaIva,
        a.Moneda,
        a.PRECIO1,
        a.COSTO,
        a.SUSPENDIDO,
        a.SUSPENDIDOC,
        a.SUSPENDIDOV,
        a.RutaImagen,
        a.DescRubro,
        a.DescMarca
    FROM vt_ma_articulos a
    ORDER BY IdArticulo
""")
# a.SUSPENDIDOC = 1 es Suspendido para las compras si esta en 0 esta habilitado para compras
# a.SUSPENDIDOV = 1 es Suspendido para las Ventas si esta en 0 esta habilitado para Ventas
# a.SUSPENDIDO = 1 es Suspendido para las compras y ventas, esta de baja

productos_raw = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
print(f"Total productos a procesar: {len(productos_raw)}")

productos_actualizados = 0
productos_creados = 0
errores_productos = []

# Batch creation accumulators
BATCH_SIZE = 20
batch_vals = []
batch_info = []  # store (default_code, name, barcode) for logging and mapping

lock = threading.Lock()


def procesar_producto(producto):
    global productos_actualizados
    default_code = producto.get("IDARTICULO", "").strip()
    if not default_code:
        return

    name = producto.get("DESCRIPCION")
    unidad_id = MAP_UNIDADES.get(producto.get("IDUNIDAD"), MAP_UNIDADES.get("UN"))
    precio = float(producto.get("PRECIO1") or 0)
    costo = float(producto.get("COSTO") or 0)
    activo = producto.get("SUSPENDIDO") != "1"
    venta_habilitada = producto.get("SUSPENDIDOV") != "1"
    compra_habilitada = producto.get("SUSPENDIDOC") != "1"
    barcode = (producto.get("CODIGOBARRA") or "").strip()

    with lock:
        existing_id = map_productos.get(default_code)
        barcode_conflict = (
            barcode
            and barcode in map_barcodes
            and (
                map_barcodes[barcode] is None or map_barcodes[barcode] != existing_id
            )
        )
        if barcode_conflict:
            errores_productos.append({
                "IDARTICULO": default_code,
                "Descripcion": name,
                "Error": f"Código de barras {barcode} ya asignado",
            })
            print(
                f" Código de barras {barcode} ya está asignado a otro producto. Se omite para {default_code}"
            )
            barcode = ""
        elif not existing_id and barcode:
            # Reservar el código de barras para evitar duplicados en lotes concurrentes
            map_barcodes[barcode] = None

    categoria_desc = (producto.get("DescRubro") or "").strip()
    ruta_imagen = os.path.join(carpeta_imagenes, f"{default_code}.jpg")

    categoria_id = None
    if categoria_desc:
        key = categoria_desc.lower()
        with lock:
            categoria_id = map_categorias.get(key)
        if not categoria_id:
            try:
                new_id = get_models().execute_kw(
                    db, uid, password, 'product.category', 'create', [{'name': categoria_desc}]
                )
                with lock:
                    map_categorias[key] = new_id
                    categoria_id = new_id
            except Exception as e:
                with lock:
                    errores_productos.append({
                        "IDARTICULO": default_code,
                        "Descripcion": name,
                        "Error": f"Error creando categoría: {e}",
                    })
                return

    producto_vals = {
        "name": name,
        "default_code": default_code,
        "uom_id": unidad_id,
        "standard_price": round(costo, 2),
        "list_price": round(precio, 2),
        "active": activo,
        "sale_ok": venta_habilitada,
        "purchase_ok": compra_habilitada,
    }

    if categoria_id:
        producto_vals["categ_id"] = categoria_id
    if barcode:
        producto_vals["barcode"] = barcode

    if os.path.exists(ruta_imagen):
        with open(ruta_imagen, "rb") as img_file:
            producto_vals["image_1920"] = base64.b64encode(img_file.read()).decode("utf-8")

    try:
        if existing_id:
            get_models().execute_kw(
                db, uid, password, "product.template", "write", [[existing_id], producto_vals]
            )
            with lock:
                productos_actualizados += 1
                if barcode:
                    map_barcodes[barcode] = existing_id
            print(f" Actualizado (ID: {existing_id}) - {name}")
        else:
            create_vals = None
            create_info = None
            with lock:
                batch_vals.append(producto_vals)
                batch_info.append((default_code, name, barcode))
                if len(batch_vals) >= BATCH_SIZE:
                    create_vals = list(batch_vals)
                    create_info = list(batch_info)
                    batch_vals.clear()
                    batch_info.clear()
            if create_vals:
                _crear_batch(create_vals, create_info)
    except Exception as e:
        with lock:
            errores_productos.append({"IDARTICULO": default_code, "Descripcion": name, "Error": str(e)})
        print(f" Error procesando {default_code}: {e}")


def _crear_batch(valores, info):
    global productos_creados
    try:
        ids_creados = get_models().execute_kw(db, uid, password, "product.template", "create", [valores])
        with lock:
            for (codigo, nombre, bc), pid in zip(info, ids_creados):
                map_productos[codigo] = pid
                if bc:
                    map_barcodes[bc] = pid
                productos_creados += 1
        for (codigo, nombre, _), pid in zip(info, ids_creados):
            print(f" Creado (ID: {pid}) - {nombre}")
    except Exception as e:
        with lock:
            for codigo, nombre, bc in info:
                errores_productos.append({"IDARTICULO": codigo, "Descripcion": nombre, "Error": str(e)})
                if bc:
                    map_barcodes.pop(bc, None)
        print(f" Error procesando lote: {e}")


with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(procesar_producto, productos_raw)

# Enviar cualquier producto pendiente de creación
with lock:
    remaining_vals = list(batch_vals)
    remaining_info = list(batch_info)
    batch_vals.clear()
    batch_info.clear()

if remaining_vals:
    _crear_batch(remaining_vals, remaining_info)

cursor.close()
sql_conn.close()

print(f"Resumen de la carga:")
print(f"  - Productos actualizados: {productos_actualizados}")
print(f"  - Productos creados: {productos_creados}")

if errores_productos:
    pd.DataFrame(errores_productos).to_csv("errores_actualizacion_productos.csv", index=False)
    print("Archivo de errores guardado como errores_actualizacion_productos.csv")

fin_proceso = datetime.datetime.now()
duracion = fin_proceso - inicio_proceso
print(f"Actualización de artículos finalizada. Tiempo total: {duracion}")
