
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host="https://adb-4002612472177324.4.azuredatabricks.net/", client_id="05c3a10a-d434-4fce-adc8-cc4b9fa2cd2a", client_secret="doseb81a8f85a840d0065d99fab48751bbd3")
catalogs = w.catalogs.list()
catalog=next(catalogs).name
print(catalog)

schemas = w.schemas.list(catalog_name=catalog)
schema=next(schemas).name
print(schema)