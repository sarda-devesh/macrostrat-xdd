
from sqlalchemy import create_engine, MetaData, Engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session

engine: Engine | None = None
base: declarative_base = None
session: Session | None = None


def get_engine() -> Engine:
    return engine


def get_base() -> declarative_base:
    return base


def connect_engine(uri: str, schema: str):
    global engine
    global session
    global base

    engine = create_engine(uri)
    session = session

    base = declarative_base()
    base.metadata.reflect(get_engine())
    base.metadata.reflect(get_engine(), schema=schema, views=True)


def dispose_engine():
    global engine
    engine.dispose()


def get_session_maker() -> sessionmaker:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_session() -> Session:
    with get_session_maker()() as s:
        yield s
