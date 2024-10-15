from contextlib import contextmanager
from os import environ
from typing import Iterable

from sqlalchemy import MetaData, create_engine
from sqlalchemy.future import Connection
from sqlalchemy.orm import Session


def drop_tables(conn):
    # https://github.com/sqlalchemy/sqlalchemy/wiki/DropEverything
    metadata = MetaData()
    metadata.reflect(conn)
    metadata.drop_all(conn)


@contextmanager
def connection_in_transaction(url: str = None) -> Iterable[Connection]:
    engine = create_engine(url or environ['DB_URL'], future=True)
    conn = engine.connect()
    transaction = conn.begin()
    try:
        yield conn
    finally:
        transaction.rollback()


@contextmanager
def nested_transaction_on_connection(conn):
    transaction = conn.begin_nested()
    with transaction:
        yield
        if transaction.is_active:
            transaction.rollback()


@contextmanager
def create_tables_and_session(db, base):
    with nested_transaction_on_connection(db):
        base.metadata.create_all(bind=db, checkfirst=False)
        # https://github.com/sqlalchemy/sqlalchemy/discussions/12006#discussioncomment-10979244
        yield Session(db, join_transaction_mode="control_fully")
