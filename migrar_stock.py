import xmlrpc.client
import pyodbc
import logging
import time
from datetime import datetime # Necesario para la fecha actual

# --- 1. Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 2. Cargar Configuraciones Externas ---
try:
    from odoo_config import url, db, username, password
    from sqlserver_config import sql_server
    logger.info("Configuraciones de Odoo y SQL Server cargadas exitosamente.")
except ImportError as e:
    logger.error(f"Error al cargar archivos de configuración: {e}. Asegúrate de que 'odoo_config.py' y 'sqlserver_config.py' existen y están en el mismo directorio o en el PYTHONPATH.")
    exit(1) # Salir si no se pueden cargar las configuraciones

# --- 3. Conexión a Odoo (usando xmlrpc.client) ---
def connect_odoo():
    """Establece y devuelve la conexión con Odoo."""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        if not uid:
            raise Exception("Autenticación fallida en Odoo.")
        logger.info(f"Conexión a Odoo exitosa. UID: {uid}")
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        return uid, models
    except Exception as e:
        logger.error(f"Error al conectar o autenticar en Odoo: {e}")
        return None, None

# --- 4. Conexión a SQL Server ---
def connect_sql_server():
    """Establece y devuelve la conexión con SQL Server."""
    conn_str = (
        f"DRIVER={sql_server['driver']};"
        f"SERVER={sql_server['server']};"
        f"DATABASE={sql_server['database']};"
        f"UID={sql_server['user']};"
        f"PWD={sql_server['password']};"
    )
    try:
        logger.info(f"Intentando conectar a SQL Server en {sql_server['server']}...")
        cnxn = pyodbc.connect(conn_str)
        cursor = cnxn.cursor()
        logger.info("Conexión a SQL Server exitosa.")
        return cnxn, cursor
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.error(f"Error al conectar a SQL Server: {sqlstate} - {ex.args[1]}")
        return None, None

# --- 5. Obtener Datos de SQL Server para el día actual ---
def get_sql_daily_data(cursor):

    # La consulta SQL usa GETDATE() para obtener la fecha actual en SQL Server.
    sql_query = """
     SELECT ltrim(rtrim(a.idarticulo)) as idarticulo , a.iddeposito, b.STOCK , PUNTOPEDIDO
    FROM v_mv_stock a inner join STK_MA_ARTICULOS b on a.IDArticulo = b.IDARTICULO and a.IDDEPOSITO = b.DEPOSITO
    WHERE fecha >= CAST(GETDATE() AS DATE) AND fecha < DATEADD(DAY, 1, CAST(GETDATE() AS DATE)) group by a.idarticulo, iddeposito,STOCK,  PUNTOPEDIDO
    """

    try:
        logger.info("Obteniendo datos de SQL Server para el día actual...")
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        logger.info(f"Se obtuvieron {len(rows)} registros de SQL Server (movimientos de hoy).")
        return rows
    except Exception as e:
        logger.error(f"Error al obtener datos de SQL Server: {e}")
        return []

# --- 6. Función Principal para Actualizar Stock en Odoo (Diario) ---
def update_odoo_daily_stock():
    uid, models = connect_odoo()
    if not uid:
        return

    cnxn, cursor = connect_sql_server()
    if not cnxn:
        return

    sql_data = get_sql_daily_data(cursor)
    if not sql_data:
        logger.info("No se obtuvieron registros de SQL Server para el día actual. No se realizarán actualizaciones de stock.")
        cnxn.close()
        return

    updated_count = 0
    failed_count = 0

    # 1. Obtener el ID de la ubicación de stock por defecto (WH/Existencias)
    stock_location_ids = models.execute_kw(
        db, uid, password,
        'stock.location', 'search',
        [[['complete_name', '=', 'WH/Existencias']]]
    )
    if not stock_location_ids:
        logger.error("Ubicación 'WH/Existencias' no encontrada. No se puede actualizar el stock físico para ningún producto. Asegúrate que la ubicación existe y el usuario API tiene permisos.")
        cnxn.close()
        return
    stock_location_id = stock_location_ids[0]

    # Cargar todos los productos activos para mapear default_code -> id
    productos_odoo = models.execute_kw(
        db,
        uid,
        password,
        "product.product",
        "search_read",
        [[["active", "=", True]]],
        {"fields": ["id", "default_code"], "limit": 0},
    )
    product_map = {
        p["default_code"]: p["id"]
        for p in productos_odoo
        if p.get("default_code")
    }

    logger.info(f"Procesando {len(sql_data)} productos con movimientos del día en SQL Server.")

    # --- Iterar sobre los productos con cambios del día desde SQL Server ---
    for row in sql_data:
        id_articulo_sql = row.idarticulo
        stock_sql = int(row.STOCK)
        punto_pedido_sql = int(row.PUNTOPEDIDO)

        logger.info(f"Procesando IDARTICULO: {id_articulo_sql}, STOCK: {stock_sql}, PUNTOPEDIDO: {punto_pedido_sql} (desde datos diarios SQL).")

        try:
            # Buscar el product.product en el mapa cargado previamente
            product_product_id = product_map.get(id_articulo_sql)

            if not product_product_id:
                logger.warning(
                    f"Producto con default_code '{id_articulo_sql}' (de SQL) no encontrado o archivado en Odoo. Saltando."
                )
                failed_count += 1
                continue

            # Obtener el product.template_id asociado al product.product
            product_product_data = models.execute_kw(
                db, uid, password,
                'product.product', 'read',
                [[product_product_id]],
                {'fields': ['product_tmpl_id']}
            )
            if not product_product_data:
                logger.warning(f"No se pudo leer datos para product.product ID: {product_product_id}. Saltando.")
                failed_count += 1
                continue

            product_template_id = product_product_data[0]['product_tmpl_id'][0]

            # --- Actualizar product.template ---
            # Siempre marcamos is_storable a True aquí, para asegurar que el producto es rastreable
            # y gestionar las opciones de venta.
            models.execute_kw(
                db, uid, password,
                'product.template', 'write',
                [[product_template_id], {
                    'is_storable': True, # Asegura que el producto es rastreable (tipo Bienes)
                    'allow_out_of_stock_order': True, # Permite vender aunque no haya stock si no está agotado
                    # 'available_threshold': punto_pedido_sql # Umbral para mostrar la cantidad en web
                }]
            )

            # --- Actualizar la cantidad a la mano (stock) usando stock.quant ---
            existing_quant_ids = models.execute_kw(
                db, uid, password,
                'stock.quant', 'search',
                [[
                    ['product_id', '=', product_product_id],
                    ['location_id', '=', stock_location_id]
                ]]
            )

            if existing_quant_ids:
                quant_id = existing_quant_ids[0]
                models.execute_kw(
                    db, uid, password,
                    'stock.quant', 'write',
                    [[quant_id], {'quantity': stock_sql}]
                )
                logger.info(f"-> Stock físico actualizado para '{id_articulo_sql}' a {stock_sql}.")
            else:
                models.execute_kw(
                    db, uid, password,
                    'stock.quant', 'create',
                    [{
                        'product_id': product_product_id,
                        'location_id': stock_location_id,
                        'quantity': stock_sql
                    }]
                )
                logger.info(f"-> Nuevo quant creado para '{id_articulo_sql}' con stock {stock_sql}.")

            updated_count += 1

        except Exception as e:
            logger.error(f"Error al procesar el producto '{id_articulo_sql}': {e}")
            failed_count += 1

    logger.info(f"Proceso de actualización diaria completado.")
    logger.info(f"Productos actualizados con datos de SQL (cambios del día): {updated_count}")
    logger.info(f"Productos con errores/no encontrados en Odoo: {failed_count}")

    # Cerrar conexiones
    cnxn.close()
    logger.info("Conexión a SQL Server cerrada.")

# --- Ejecución del script ---
if __name__ == "__main__":
    update_odoo_daily_stock()