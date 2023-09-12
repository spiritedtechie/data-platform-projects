import os

from blocks.postgres import Postgres
from blocks.soda import Soda
from dotenv import load_dotenv

load_dotenv()

postgres = Postgres(
    host=os.environ.get("POSTGRES_HOST"),
    port=os.environ.get("POSTGRES_PORT"),
    db=os.environ.get("POSTGRES_DB"),
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASSWORD"),
)

uuid = postgres.save("default", overwrite=True)
slug = postgres.dict().get("block_type_slug")
print(f"Created block {slug}/default with ID: {uuid}")


soda = Soda(
    key=os.environ.get("SODA_KEY"),
    secret=os.environ.get("SODA_KEY_SECRET"),
)

uuid = soda.save("default", overwrite=True)
slug = soda.dict().get("block_type_slug")
print(f"Created block {slug}/default with ID: {uuid}")
