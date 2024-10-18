import logging
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    uri: str
    SCHEMA: str
    max_tries: int = 5
    LOGGING_LEVEL: int = logging.INFO
