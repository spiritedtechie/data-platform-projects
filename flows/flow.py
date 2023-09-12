from blocks.postgres import Postgres
from blocks.soda import Soda
from prefect import flow, task
from prefect.context import FlowRunContext
from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck
from prefect_soda_core.tasks import soda_scan_execute

soda = Soda.load("default")
postgres = Postgres.load("default")


@task
def load_csv_to_store(file_path):
    return postgres.load_csv(file_path)


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
        data_source_name="postgres_dw",
        configuration=soda_configuration,
        checks=soda_check,
        scan_results_file=scan_results_file_path,
        variables={},
        verbose=False,
        return_scan_result_file_content=False,
        shell_env=env,
    )


@flow(name="Retail data", log_prints=True)
def process_retail_data():
    load = load_csv_to_store("./datasets/Online_Retail.csv")

    quality_check_raw_data(wait_for=[load])


if __name__ == "__main__":
    process_retail_data.serve(name="retail-data-deployment")
