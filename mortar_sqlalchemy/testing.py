from contextlib import contextmanager
from os import environ

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session


def drop_tables(conn):
    # https://github.com/sqlalchemy/sqlalchemy/wiki/DropEverything
    metadata = MetaData()
    metadata.reflect(conn)
    metadata.drop_all(conn)


@contextmanager
def connection_in_transaction(url: str = None):
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
    try:
        yield
    finally:
        # https://docs.sqlalchemy.org/en/14/faq/sessions.html#but-why-does-flush-insist-on-issuing-a-rollback
        if transaction.is_active:
            transaction.rollback()


@contextmanager
def create_tables_and_session(db, base):
    with nested_transaction_on_connection(db):
        base.metadata.create_all(bind=db, checkfirst=False)
        yield Session(db)
