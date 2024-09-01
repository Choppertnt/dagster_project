from dagster import AssetExecutionContext,AssetKey
from dagster_dbt import dbt_assets, DbtCliResource , DagsterDbtTranslator
from pathlib import Path
from ..resources import dbt_resource
from .constants import DBT_DIRECTORY
import os


dbt_project_dir = Path(DBT_DIRECTORY)

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        if 'data1' in dbt_resource_props["name"]:
            name = 'employee'
        else:
            name = 'career'
        if resource_type == "source":
            return AssetKey(f"upload_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(["--quiet", "parse"]).wait().target_path.joinpath("manifest.json"))
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_build_invocation = dbt.cli(["build"], context=context)

    yield from dbt_build_invocation.stream()

    run_results_json = dbt_build_invocation.get_artifact("run_results.json")
    for result in run_results_json["results"]:
        context.log.debug(result["compiled_code"])