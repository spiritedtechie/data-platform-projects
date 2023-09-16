from prefect.blocks.core import Block
from pydantic import SecretStr

from lib.postgres_loader import PostgresDBProperties, load_file_to_database


class PostgresDataWarehouse(Block):
    host: str
    port: str
    db: str
    user: SecretStr
    password: SecretStr

    def load_csv(self, file_path, schema):
        return load_file_to_database(
            file_path=file_path,
            schema=schema,
            db=PostgresDBProperties(
                self.host,
                self.port,
                self.db,
                self.user.get_secret_value(),
                self.password.get_secret_value(),
            ),
        )

    def as_env_properties(self):
        return {
            "DW_HOST": self.host,
            "DW_PORT": self.port,
            "DW_DB": self.db,
            "DW_USER": self.user.get_secret_value(),
            "DW_PASSWORD": self.password.get_secret_value(),
            "DBT_ENV_SECRET_DW_PASSWORD": self.password.get_secret_value(),
        }
