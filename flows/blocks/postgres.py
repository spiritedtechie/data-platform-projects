from prefect.blocks.core import Block
from pydantic import SecretStr

from lib.postgres_loader import PostgresDBProperties, load_file_to_database


class Postgres(Block):
    host: str
    port: str
    db: str
    user: SecretStr
    password: SecretStr

    def load_csv(self, file_path):
        return load_file_to_database(
            file_path,
            PostgresDBProperties(
                self.host,
                self.port,
                self.db,
                self.user.get_secret_value(),
                self.password.get_secret_value(),
            ),
        )

    def as_env_properties(self):
        return {
            "POSTGRES_HOST": self.host,
            "POSTGRES_PORT": self.port,
            "POSTGRES_DB": self.db,
            "POSTGRES_USER": self.user.get_secret_value(),
            "POSTGRES_PASSWORD": self.password.get_secret_value(),
            "DBT_ENV_SECRET_POSTGRES_PASSWORD": self.password.get_secret_value()
        }
