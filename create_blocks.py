from dotenv import load_dotenv
import os

from blocks import postgres

load_dotenv()

postgres = postgres.Postgres(
    host=os.environ.get("POSTGRES_HOST"),
    port=os.environ.get("POSTGRES_PORT"),
    db=os.environ.get("POSTGRES_DB"),
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASSWORD"),
)

uuid = postgres.save("default", overwrite=True)
slug = postgres.dict().get("block_type_slug")
print(f"Created block {slug}/default with ID: {uuid}")
