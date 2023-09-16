import os

from blocks.postgres_dw import PostgresDataWarehouse
from blocks.soda import Soda
from dotenv import load_dotenv

load_dotenv()

postgres_dw = PostgresDataWarehouse(
    host=os.environ.get("DW_HOST"),
    port=os.environ.get("DW_PORT"),
    db=os.environ.get("DW_DB"),
    user=os.environ.get("DW_USER"),
    password=os.environ.get("DW_PASSWORD"),
)

uuid = postgres_dw.save("default", overwrite=True)
slug = postgres_dw.dict().get("block_type_slug")
print(f"Created block {slug}/default with ID: {uuid}")


soda = Soda(
    key=os.environ.get("SODA_KEY"),
    secret=os.environ.get("SODA_KEY_SECRET"),
)

uuid = soda.save("default", overwrite=True)
slug = soda.dict().get("block_type_slug")
print(f"Created block {slug}/default with ID: {uuid}")
