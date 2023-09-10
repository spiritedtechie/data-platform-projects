from prefect import flow
from prefect import flow, task
from blocks.postgres import Postgres


@flow(name="Retail data", log_prints=True)
def process_retail_data():
    postgres = Postgres.load("default")
    postgres.load_from_csv("./datasets/Online_Retail.csv")


if __name__ == "__main__":
    process_retail_data.serve(name="retail-data-deployment")
