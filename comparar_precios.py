# -*- coding: utf-8 -*-
"""Comparar productos y precios entre Odoo y Alfa Gestión.

Genera un archivo Excel con la información de precios proveniente de Alfa y
los precios existentes en Odoo para cada producto y lista de precios.
"""

import pyodbc
import xmlrpc.client
import pandas as pd

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# --- Conexión a Odoo ---
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# --- Conexión a SQL Server ---
sql_conn = pyodbc.connect(
    f"DRIVER={sql_server['driver']};"
    f"SERVER={sql_server['server']};"
    f"DATABASE={sql_server['database']};"
    f"UID={sql_server['user']};"
    f"PWD={sql_server['password']}"
)
cursor = sql_conn.cursor()


def limpiar(valor):
    """Quitar espacios y decimales innecesarios."""
    if valor is None:
        return ""
    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto

# --- Datos desde Alfa Gestión ---
cursor.execute(
    "SELECT IdArticulo, DescripcionArticulo, Precio4, IdLista, Nombre "
    "FROM VT_MA_PRECIOS_ARTICULOS WHERE TipoLista = 'V'"
)
cols = [col[0] for col in cursor.description]
rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

precios_sql = [
    {
        'IdArticulo': limpiar(r['IdArticulo']),
        'DescripcionArticulo': limpiar(r['DescripcionArticulo']),
        'Precio4': float(r['Precio4'] or 0),
        'IdLista': limpiar(r['IdLista']),
        'Nombre': limpiar(r['Nombre']),
    }
    for r in rows
]

# --- Productos existentes en Odoo ---
codigos = list({p['IdArticulo'] for p in precios_sql})
product_map = {}
chunk_size = 80
for i in range(0, len(codigos), chunk_size):
    chunk = codigos[i:i + chunk_size]
    productos = models.execute_kw(
        db,
        uid,
        password,
        'product.product',
        'search_read',
        [[['default_code', 'in', chunk]]],
        {'fields': ['id', 'default_code']}
    )
    for prod in productos:
        product_map[prod['default_code'].strip()] = prod['id']

# --- Listas de precios y precios en Odoo ---
nombres_listas = {p['Nombre'] for p in precios_sql}
odoo_pricelist_map = {}
for nombre in nombres_listas:
    lista = models.execute_kw(
        db, uid, password,
        'product.pricelist', 'search_read',
        [[['name', '=', nombre]]],
        {'fields': ['id'], 'limit': 1}
    )
    if lista:
        odoo_pricelist_map[nombre] = lista[0]['id']

odoo_price_map = {}
for nombre, lista_id in odoo_pricelist_map.items():
    items = models.execute_kw(
        db, uid, password,
        'product.pricelist.item', 'search_read',
        [[['pricelist_id', '=', lista_id], ['applied_on', '=', '0_product_variant']]],
        {'fields': ['product_id', 'fixed_price']}
    )
    product_ids = [item['product_id'][0] for item in items if item.get('product_id')]
    if product_ids:
        prods = models.execute_kw(
            db, uid, password,
            'product.product', 'read',
            [product_ids],
            {'fields': ['default_code']}
        )
        code_map = {p['id']: p['default_code'].strip() for p in prods}
    else:
        code_map = {}
    for item in items:
        pid = item.get('product_id')
        if not pid:
            continue
        code = code_map.get(pid[0])
        if code:
            odoo_price_map[(code, nombre)] = float(item.get('fixed_price') or 0)

# --- Comparación y DataFrame final ---
rows_comparacion = []
for p in precios_sql:
    key = (p['IdArticulo'], p['Nombre'])
    rows_comparacion.append({
        'IdArticulo': p['IdArticulo'],
        'DescripcionArticulo': p['DescripcionArticulo'],
        'Precio4': p['Precio4'],
        'IdLista': p['IdLista'],
        'Nombre': p['Nombre'],
        'Existe_Odoo': p['IdArticulo'] in product_map,
        'Precio_Odoo': odoo_price_map.get(key),
    })

df = pd.DataFrame(rows_comparacion)
df.sort_values(['Nombre', 'IdArticulo'], inplace=True)
df.to_excel('comparacion_precios.xlsx', index=False)

cursor.close()
sql_conn.close()
print("Excel 'comparacion_precios.xlsx' generado.")
