
from sqlalchemy import create_engine, MetaData, Engine
from sqlalchemy.orm import sessionmaker, declarative_base

engine: Engine | None = None
Base: declarative_base = None


def get_engine():
    return engine


def get_base():
    return Base


def connect_engine(uri: str, schema: str):
    global engine
    global Base

    engine = create_engine(uri)

    Base = declarative_base()
    Base.metadata.reflect(get_engine())


def dispose_engine():
    global engine
    engine.dispose()


def get_session_maker():
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
