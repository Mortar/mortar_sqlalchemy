import pytest
from sqlalchemy import create_engine, Column, Integer, ForeignKey, inspect
from sqlalchemy.orm import relationship, Session
from testfixtures import compare
from testservices.provider import Provider
from testservices.services.databases import DatabaseFromEnvironment, PostgresContainer

from mortar_sqlalchemy.testing import drop_tables


@pytest.fixture()
def db():
    # per-test DB
    provider = Provider(
        DatabaseFromEnvironment(),
        PostgresContainer(),
    )
    with provider as database:
        # Use psycopg3:
        database.driver = 'psycopg'
        engine = create_engine(database.url, future=True)
        conn = engine.connect()
        transaction = conn.begin()
        try:
            yield conn
        finally:
            transaction.rollback()


def test_drop_tables(db, base):

    class Model1(base):
        __tablename__ = 'model1'
        id = Column(Integer, primary_key=True)
        model2_id = Column(Integer, ForeignKey('model2.id'))
        model2 = relationship("Model2")

    class Model2(base):
        __tablename__ = 'model2'
        id = Column('id', Integer, primary_key=True)

    base.metadata.create_all(bind=db, checkfirst=False)

    m1 = Model1()
    m2 = Model2()
    m1.model2 = m2

    session = Session(db)
    session.add(m1)
    session.add(m2)

    compare(session.query(Model1).count(), expected=1)
    compare(session.query(Model2).count(), expected=1)

    drop_tables(db)

    inspector = inspect(db)
    compare(inspector.get_table_names(), expected=[])
