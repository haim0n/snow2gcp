from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeAuth(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='SNOWFLAKE_')

    user: str
    account: str
    password: str


class Settings(BaseSettings):
    snowflake_auth: SnowflakeAuth
