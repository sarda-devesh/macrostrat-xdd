import logging

from macrostrat_db_insertion.settings import Settings

settings = Settings()

logging.basicConfig(
    level=settings.LOGGING_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


