from dagster import AssetSelection, define_asset_job
from ..assets.dbt import dbt_analytics
from dagster_dbt import build_dbt_asset_selection
trips_by_week = AssetSelection.keys("sales_role")
dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "upload_").downstream()

update_update_job = define_asset_job(
    name="update_job",
    selection=AssetSelection.all()-trips_by_week
)

