from blocks.postgres_dw import PostgresDataWarehouse
from blocks.soda import Soda
from prefect import flow, task
from prefect.context import FlowRunContext
from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck
from prefect_soda_core.tasks import soda_scan_execute
from prefect_dbt import DbtCoreOperation, DbtCliProfile


soda = Soda.load("default")
postgres_dw = PostgresDataWarehouse.load("default")


@task
def load_raw_data_to_store():
    postgres_dw.load_csv("./datasets/countries.csv", schema="raw")
    postgres_dw.load_csv("./datasets/invoices.csv", schema="raw")
    return None


@task(task_run_name="quality_check_{name}")
def quality_check(name: str, soda_check: SodaCLCheck, data_source: str = "raw"):
    flow_run_ctx = FlowRunContext.get()
    flow_run_name = flow_run_ctx.flow_run.name
    scan_results_file_path = f"{flow_run_name}.json"

    env = {**postgres_dw.as_env_properties(), **soda.as_env_properties()}
    soda_configuration = SodaConfiguration(
        configuration_yaml_path="./soda/configuration.yml"
    )

    return soda_scan_execute.fn(
        data_source_name=data_source,
        configuration=soda_configuration,
        checks=soda_check,
        scan_results_file=scan_results_file_path,
        variables={},
        verbose=False,
        return_scan_result_file_content=False,
        shell_env=env,
    )


@task
def build_models():
    env = {**postgres_dw.as_env_properties()}
    dbt_init = DbtCoreOperation(
        commands=["dbt debug", "dbt run"],
        profiles_dir="./dbt",
        project_dir="./dbt/online_retail",
        overwrite_profiles=False,
        env=env,
    )
    return dbt_init.run()


@flow(name="Retail data", log_prints=True)
def process_retail_data():
    load = load_raw_data_to_store()

    raw_data_check = quality_check(
        name="source_data",
        wait_for=[load],
        data_source="raw",
        soda_check=SodaCLCheck(sodacl_yaml_path="./soda/checks/sources/*.yml"),
    )

    create_models = build_models(wait_for=[raw_data_check])

    staged_data_check = quality_check(
        name="staged_data",
        wait_for=[create_models],
        data_source="retail",
        soda_check=SodaCLCheck(sodacl_yaml_path="./soda/checks/staged/*.yml"),
    )

    transform_data_check = quality_check(
        name="transform_data",
        wait_for=[create_models],
        data_source="retail",
        soda_check=SodaCLCheck(sodacl_yaml_path="./soda/checks/transform/*.yml"),
    )


if __name__ == "__main__":
    process_retail_data.serve(name="retail-data-deployment")
