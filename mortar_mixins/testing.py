from contextlib import contextmanager
from os import environ

from sqlalchemy import MetaData, ForeignKeyConstraint, Table, inspect, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql.ddl import DropConstraint, DropTable


def drop_tables(conn):

    inspector = inspect(conn)

    # gather all data first before dropping anything.
    # some DBs lock after things have been dropped in
    # a transaction.
    metadata = MetaData()

    tbs = []
    for table_name in inspector.get_table_names():
        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            fks.append(
                ForeignKeyConstraint((), (), name=fk['name'])
            )
        t = Table(table_name, metadata, *fks)
        tbs.append(t)
        for fkc in fks:
            conn.execute(DropConstraint(fkc, cascade=True))

    for table in tbs:
        conn.execute(DropTable(table))


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
