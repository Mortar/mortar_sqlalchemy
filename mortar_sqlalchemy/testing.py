from contextlib import contextmanager
from os import environ
from typing import Iterable

from sqlalchemy import MetaData, create_engine
from sqlalchemy.future import Connection
from sqlalchemy.orm import Session, DeclarativeBase


def drop_tables(connection: Connection) -> None:
    """
    :meth:`Drop all <sqlalchemy.schema.MetaData.drop_all>` tables that can be
    :meth:`reflected <sqlalchemy.schema.MetaData.reflect>`
    from the supplied connection.
    """
    # https://github.com/sqlalchemy/sqlalchemy/wiki/DropEverything
    metadata = MetaData()
    metadata.reflect(connection)
    metadata.drop_all(connection)


@contextmanager
def connection_in_transaction(url: str = None) -> Iterable[Connection]:
    """
    Create an :any:`engine <sqlalchemy.engine.Engine>` using the supplied
    url or, in not specified, the contents of the ``DB_URL`` environment variable.

    The context return is a :class:`~sqlalchemy.engine.Connection` that is in an
    active :any:`transaction <sqlalchemy.engine.RootTransaction>` that will be
    :any:`rolled back <sqlalchemy.engine.RootTransaction.rollback>` when exiting
    the context.
    """
    engine = create_engine(url or environ['DB_URL'], future=True)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
    finally:
        transaction.rollback()


@contextmanager
def create_tables_and_session(connection: Connection, base: DeclarativeBase) -> Iterable[Session]:
    """
    Create all the tables found in the :class:`~sqlalchemy.schema.MetaData` of the supplied
    :class:`~sqlalchemy.orm.DeclarativeBase`.

    The context returned is a :class:`~sqlalchemy.orm.Session` joined to the transaction open on
    the supplied connection using the `create_savepoint` mode as described in the pattern
    :ref:`session_external_transaction`.
    """
    base.metadata.create_all(bind=connection, checkfirst=False)
    yield Session(connection, join_transaction_mode="create_savepoint")
