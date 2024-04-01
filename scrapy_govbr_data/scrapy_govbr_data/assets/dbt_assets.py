import os
from pathlib import Path

from dagster import AssetExecutionContext, file_relative_path
from dagster_dbt import DbtCliResource, dbt_assets


dbt_project_dir = file_relative_path(__file__, "../../../govbr_data")
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

dbt_manifest_path = (
    dbt.cli(
        ["--quiet", "parse"],
        target_path=Path("target"),
    )
    .wait()
    .target_path.joinpath("manifest.json")
)


@dbt_assets(manifest=dbt_manifest_path)
def govbr_data_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()