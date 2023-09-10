from prefect.blocks.core import Block
from pydantic import SecretStr
from postgres_loader import load_file_to_database
from postgres_loader import DatabaseProperties


class Postgres(Block):
    host: str
    port: str
    db: str
    user: SecretStr
    password: SecretStr

    def load_from_csv(self, file_path):
        return load_file_to_database(
            file_path,
            DatabaseProperties(self.host, self.port, self.db, self.user.get_secret_value(), self.password.get_secret_value()),
        )