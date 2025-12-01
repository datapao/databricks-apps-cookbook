import json
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks import sql

cfg = Config()

w = WorkspaceClient(config=cfg)

warehouses = w.warehouses.list()

warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}

@st.cache_resource
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

st.header(body="Workflows", divider=True)
st.subheader("Get job run history")

def read_table(table_name, conn, job_id):
    with conn.cursor() as cursor:
        query = f"""SELECT
  workspace_id,
  job_id,
  run_id,
  run_name,
  run_type,
  result_state,
  MIN(period_start_time) AS run_start_time,
  MAX(period_end_time) AS run_end_time
FROM system.lakeflow.job_run_timeline
WHERE job_id = {job_id}
GROUP BY
  workspace_id,
  job_id,
  run_id,
  run_name,
  run_type,
  result_state
ORDER BY run_start_time DESC
        """
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

http_path_input = st.selectbox(
    "Select a SQL warehouse:",
    [""] + list(warehouse_paths.keys()),
    key="http_path_input"
)

job_id = st.text_input(
    label="Specify job id:",
    placeholder="1060426922965246",
    help="You can find the job ID under job details after opening a job in the UI.",
)

if http_path_input and job_id and st.button(label="Get history"):
    conn = get_connection(warehouse_paths[http_path_input])        
    table_full_name = "system.lakeflow.job_run_timeline"
    df = read_table(table_full_name, conn, job_id=job_id)
    st.dataframe(df)