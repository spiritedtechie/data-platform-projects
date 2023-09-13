from blocks.postgres import Postgres
from blocks.soda import Soda
from prefect import flow, task
from prefect.context import FlowRunContext
from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck
from prefect_soda_core.tasks import soda_scan_execute
from prefect_dbt import DbtCoreOperation, DbtCliProfile


soda = Soda.load("default")
postgres = Postgres.load("default")


@task
def load_raw_data_to_store():
    postgres.load_csv("./datasets/countries.csv", schema="raw")
    return postgres.load_csv("./datasets/invoices.csv", schema="raw")


@task
def quality_check_raw_data():
    flow_run_ctx = FlowRunContext.get()
    flow_run_name = flow_run_ctx.flow_run.name
    scan_results_file_path = f"{flow_run_name}.json"

    env = {**postgres.as_env_properties(), **soda.as_env_properties()}
    soda_configuration = SodaConfiguration(
        configuration_yaml_path="./soda/configuration.yml"
    )
    soda_check = SodaCLCheck(sodacl_yaml_path="./soda/checks/sources/raw_invoices.yml")

    return soda_scan_execute.fn(
        data_source_name="raw",
        configuration=soda_configuration,
        checks=soda_check,
        scan_results_file=scan_results_file_path,
        variables={},
        verbose=False,
        return_scan_result_file_content=False,
        shell_env=env,
    )


@task
def create_models():
    env = {**postgres.as_env_properties()}
    dbt_init = DbtCoreOperation(
        commands=["dbt debug", "dbt run"],
        profiles_dir="./dbt",
        project_dir="./dbt/online_retail",
        overwrite_profiles=False,
        env=env,
    )
    dbt_init.run()


@flow(name="Retail data", log_prints=True)
def process_retail_data():
    load = load_raw_data_to_store()

    check = quality_check_raw_data(wait_for=[load])

    create_models(wait_for=[check])


if __name__ == "__main__":
    process_retail_data.serve(name="retail-data-deployment")
