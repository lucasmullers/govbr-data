import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
# from dagster_dbt import build_schedule_from_dbt_selection

from .assets.dbt_assets import govbr_data_dbt_assets
from .assets import extract_asset 

from .assets.dbt_assets import dbt_project_dir

extract_data_assets = load_assets_from_modules(
    [extract_asset]
    # all of these assets live in the duckdb database, under the schema raw_data
    # key_prefix=["duckdb", "raw_data"],
)


schedules = [
    # build_schedule_from_dbt_selection(
    #     [govbr_data_dbt_assets],
    #     job_name="materialize_dbt_models",
    #     cron_schedule="0 0 * * *",
    #     dbt_select="fqn:*",
    # ),
]

defs = Definitions(
    assets=[govbr_data_dbt_assets, *extract_data_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)