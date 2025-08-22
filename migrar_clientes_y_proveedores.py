# -*- coding: utf-8 -*-

import pyodbc
import xmlrpc.client
import pandas as pd
import os


from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# Conexión a Odoo
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# Conexión a SQL Server
sql_conn = pyodbc.connect(
    f"DRIVER={sql_server['driver']};"
    f"SERVER={sql_server['server']};"
    f"DATABASE={sql_server['database']};"
    f"UID={sql_server['user']};"
    f"PWD={sql_server['password']}"
)
cursor = sql_conn.cursor()

# Mapeos

dict_paises = {"1": 10, "2": 31, "3": 234, "4": 185, "5": 46}
dict_provincias = {
    "0": 553, "1": 554, "2": 560, "3": 555, "4": 558, "5": 559, "6": 556,
    "7": 557, "8": 561, "9": 562, "10": 563, "11": 564, "12": 565, "13": 566,
    "14": 567, "15": 1912, "16": 568, "17": 570, "18": 571, "19": 574,
    "20": 572, "21": 573, "22": 576, "23": 575, "24": None
}
dict_tipos_doc = {"1": 4, "2": 6, "3": 5, "4": None}
dict_afip_responsabilidad = {
    "1": 1, "2": None, "3": 5, "4": 4, "5": 6, "7": None, "8": None
}

# Consulta desde SQL Server con JOIN correcto
consulta_sql = '''
    SELECT
        MC.CODIGO,
        MC.DESCRIPCION AS RAZON_SOCIAL,
        MC.TipoVista,
        MC.Dada_De_Baja,
        MA.MAIL,
        MA.TELEFONO,
        MA.CALLE,
        MA.LOCALIDAD,
        MA.PROVINCIA,
        MA.PAIS,
        MA.DOCUMENTO_TIPO,
        MA.NUMERO_DOCUMENTO,
        MA.IVA,
        MA.IdLista
FROM MA_CUENTAS MC
LEFT JOIN MA_CUENTASADIC MA ON MC.CODIGO = MA.CODIGO
WHERE MC.TipoVista IN ('CL', 'PR')
'''
cursor.execute(consulta_sql)
rows = cursor.fetchall()

errores = []

for row in rows:
    (
        codigo, razon_social, tipo_vista, dada_de_baja, mail, telefono, calle,
        localidad, provincia, pais, doc_tipo, doc_num, iva, id_lista
    ) = [str(x).strip() if x else '' for x in row]

    activo = False if dada_de_baja.lower() in ["1", "si", "sí", "true"] else True
    tipo_doc_odoo = dict_tipos_doc.get(doc_tipo)
    afip_id = dict_afip_responsabilidad.get(iva)
    vat = doc_num if doc_tipo == "1" else ""
    link_customer =  f""  # f"https://alfanet.com.ar/ac/autologin_odoo/{codigo}/94/customer"--ESTO LO USA EL TREBOL, LINK PARA DESCARGAR LISTA CREO

    vals_partner = {
        'name': razon_social,
        'phone': telefono,
        'email': mail,
        'street': calle,
        'city': localidad,
        'country_id': dict_paises.get(pais),
        'state_id': dict_provincias.get(provincia),
        'l10n_latam_identification_type_id': tipo_doc_odoo,
        'vat': vat,
        'is_company': True,
        'x_customer_link': link_customer,
        'customer_rank': 1 if tipo_vista == 'CL' else 0,
        'supplier_rank': 1 if tipo_vista == 'PR' else 0,
        'active': activo,
        'ref': codigo,
    }

    vals_partner = {k: v for k, v in vals_partner.items() if v}

    try:
        existentes = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['ref', '=', codigo]]])
        partner_id = None
        if existentes:
            partner_id = existentes[0]
            models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], vals_partner])
            print(f"[ACTUALIZADO] {codigo} - {razon_social} (ID: {partner_id})")
        else:
            partner_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [vals_partner])
            print(f"[CREADO] {codigo} - {razon_social} (ID: {partner_id})")

        # Actualizar la lista de precios si se encontró un partner_id y un id_lista
        if partner_id and id_lista:
            # Buscar la lista de precios en Odoo por el campo personalizado 'x_idlista'
            pricelist_ids = models.execute_kw(db, uid, password, 'product.pricelist', 'search', [[['x_idlista', '=', str(id_lista)]]])
            if pricelist_ids:
                pricelist_id = pricelist_ids[0]
                try:
                    models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], {'property_product_pricelist': pricelist_id}])
                    print(f"  └─ Lista de precios con x_idlista {id_lista} (ID: {pricelist_id}) actualizada para el cliente.")
                except Exception as e_write_pricelist:
                    print(f"  ⚠️ Error al escribir la lista de precios para el cliente {codigo}: {e_write_pricelist}")
            else:
                print(f"  ⚠️ No se encontró la lista de precios con x_idlista {id_lista} en Odoo.")

    except Exception as e_partner:
        errores.append({"codigo": codigo, "error": str(e_partner)})

cursor.close()
sql_conn.close()

# Guardar errores
if errores:

    # Crear la carpeta si no existe
    ruta_directorio = "C:\\MIGRACION_ODOO"
    os.makedirs(ruta_directorio, exist_ok=True)

    # Definir la ruta del archivo de salida
    archivo_errores = os.path.join(ruta_directorio, "clientes_errores.csv")

    # Guardar el CSV
    pd.DataFrame(errores).to_csv(archivo_errores, index=False)


print("\n✅ Actualización de clientes finalizada.")