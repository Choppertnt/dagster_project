from dagster import  Definitions,load_assets_from_modules
from .assets import requests,dbt
from .resources import postgres_db_resource,dbt_resource
import os
from .jobs import update_update_job
from .schedules import update_sql_sche


requests_assets = load_assets_from_modules([requests])
dbt_analytics_assets = load_assets_from_modules(modules=[dbt])
all_jobs = [update_update_job]
all_schedual = [update_sql_sche]


defs = Definitions(
    assets=requests_assets + dbt_analytics_assets ,
    resources={
        "postgres_db": postgres_db_resource.configured({
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD'),
            "database": os.getenv('POSTGRES_DATABASE'),
            "host": os.getenv('POSTGRES_HOST'),
            "port": int(os.getenv('POSTGRES_PORT')),
        }),
        "dbt": dbt_resource,
        },
    jobs = all_jobs,
    schedules = all_schedual,
)
