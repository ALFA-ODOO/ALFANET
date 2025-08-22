import os
from dotenv import load_dotenv
load_dotenv()

# Configuration for connecting to Odoo
#url = os.getenv("ODOO_URL", "https://danlovic.odoo.com")
#db = os.getenv("ODOO_DB", "danlovic")
#username = os.getenv("ODOO_USERNAME")
#password = os.getenv("ODOO_PASSWORD")

url = os.getenv("ODOO_URL", "")
db = os.getenv("ODOO_DB", "")
username = os.getenv("ODOO_USERNAME")
password = os.getenv("ODOO_PASSWORD")

if username is None or password is None:
    raise EnvironmentError("ODOO_USERNAME and ODOO_PASSWORD must be set in the environment")
