from pydantic_settings import BaseSettings


class SnowflakeAuth(BaseSettings):
    username: str
    account: str
    password: str


class GCS(BaseSettings):
    gcs_bucket: str
    gcs_base_path: str
