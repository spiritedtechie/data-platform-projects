from prefect.blocks.core import Block
from pydantic import SecretStr


class Soda(Block):
    key: SecretStr
    secret: SecretStr

    def as_env_properties(self):
        return {
            "SODA_KEY": self.key.get_secret_value(),
            "SODA_KEY_SECRET": self.secret.get_secret_value(),
        }
