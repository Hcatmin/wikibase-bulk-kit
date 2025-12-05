from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Project-wide settings sourced from .env and environment variables."""

    wikibase_url: str
    mediawiki_api_url: str | None
    sparql_endpoint_url: str | None = None

    wikibase_username: str
    wikibase_password: str

    mysql_host: str | None = None
    mysql_port: int | None = None
    mysql_database: str | None = None
    mysql_user: str | None = None
    mysql_password: str | None = None

    class Config:
        env_file = ".env", ".env.local"
        env_file_encoding = "utf-8"

settings = Settings()
