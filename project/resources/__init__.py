
from dagster import resource, InitResourceContext
from sqlalchemy import create_engine
from dagster_dbt import DbtCliResource
from pathlib import Path
from ..assets.constants import DBT_DIRECTORY
import os




@resource(
    config_schema={
        "user": str,
        "password": str,
        "database": str,
        "host": str,
        "port": int
    }
)
def postgres_db_resource(context):
    user = context.resource_config["user"]
    password = context.resource_config["password"]
    database = context.resource_config["database"]
    host = context.resource_config["host"]
    port = context.resource_config["port"]
    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    
    engine = create_engine(conn_str)
    try:
        yield engine
    finally:
        engine.dispose()

dbt_resource = DbtCliResource(
    project_dir=DBT_DIRECTORY,
    target=os.getenv("DAGSTER_ENVIRONMENT", "dev")
)