import pyodbc
import os

from sqlserver_config import sql_server


def exportar_correos(archivo_salida: str) -> None:
    """Genera un archivo de texto con los contactos para una lista de correos.

    Cada línea del archivo tendrá el formato::

        "Nombre o Razón Social" <correo@example.com>

    La información se obtiene de la tabla BT del sistema Alfa-Gestión.
    Solo se incluyen registros con un correo válido.
    """

    consulta = (
        "SELECT nombre, email "
        "FROM BT "
        "WHERE email IS NOT NULL AND email <> ''"
    )

    conexion = pyodbc.connect(
        f"DRIVER={sql_server['driver']};"
        f"SERVER={sql_server['server']};"
        f"DATABASE={sql_server['database']};"
        f"UID={sql_server['user']};"
        f"PWD={sql_server['password']}"
    )
    cursor = conexion.cursor()
    cursor.execute(consulta)

    with open(archivo_salida, 'w', encoding='utf-8') as archivo:
        for nombre, email in cursor.fetchall():
            nombre = nombre.strip() if nombre else ''
            email = email.strip() if email else ''
            if nombre and email:
                archivo.write(f'"{nombre}" <{email}>\n')

    cursor.close()
    conexion.close()


if __name__ == '__main__':
    archivo_salida = os.path.join(os.getcwd(), 'contactos.txt')
    exportar_correos(archivo_salida)
    print(f'Archivo generado: {archivo_salida}')
