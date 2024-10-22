import pytest
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, joinedload
from testfixtures import compare, ShouldAssert

from mortar_sqlalchemy import Common
from mortar_sqlalchemy.testing import create_tables_and_session


@pytest.fixture
def model(base):

    class Model(Common, base):
        id = Column(Integer, primary_key=True)
        value = Column(Integer)

    return Model


@pytest.fixture
def another_model(base):

    class AnotherModel(Common, base):
        id = Column(Integer, primary_key=True)
        attr = Column(Integer)
        other_id = Column(Integer, ForeignKey('model.id'))
        other = relationship("Model", backref='another')

    return AnotherModel


@pytest.fixture()
def session(connection, base, model, another_model):
    with create_tables_and_session(connection, base) as session:
        yield session


class TestCommon:

    def test_table_name(self, model):
        compare(model.__table__.name, 'model')

    def test_eq_wrong_type(self, model):
        assert not (model() == object())

    def test_eq_wrong_model_type(self, base, model):
        class OtherModel(Common, base):
            id = Column(Integer, primary_key=True)
        assert not (model(id=1) == OtherModel(id=1))

    def test_eq_different(self, model):
        assert not (model(id=1) == model(id=2))

    def test_eq_different_keys(self, model):
        assert not (model() == model(id=2))

    def test_eq_same(self, model):
        assert (model(id=1) == model(id=1))

    def test_ne_wrong_type(self, model):
        assert (model() != object())

    def test_ne_wrong_model_type(self, base, model):
        class OtherModel(Common, base):
            id = Column(Integer, primary_key=True)
        assert (model(id=1) != OtherModel(id=1))

    def test_ne_different(self, model):
        assert (model(id=1) != model(id=2))

    def test_ne_same(self, model):
        assert (model(id=1) != model(id=2))

    def test_repr(self, model):
        compare('Model(id=1, value=3)', repr(model(id=1, value=3)))

    def test_str(self, model):
        compare('Model(id=3, value=1)', str(model(id=3, value=1)))

    def test_repr_relationships_excluded(self, model, another_model, session):
        model = another_model(id=1, other=model(id=1, value=42))
        session.add(model)
        session.flush()
        compare('AnotherModel(id=1, other_id=1)',
                actual=str(model))


class TestCompare:

    def check_raises(self, x, y, message, **kw):
        with ShouldAssert(message):
            compare(x, y, **kw)

    def test_identical(self, model):
        compare(
            [model(id=1), model(id=2)],
            [model(id=1), model(id=2)]
            )

    def test_different(self, model):
        self.check_raises(
            model(id=1), model(id=2),
            "Model not as expected:\n"
            '\n'
            'same:\n'
            "['value']\n"
            "\n"
            "values differ:\n"
            "'id': 1 != 2"
        )

    def test_different_types(self, model, another_model):
        self.check_raises(
            model(id=1), another_model(id=1),
            "Model(id=1) != AnotherModel(id=1)"
        )

    def test_db_versus_non_db_equal(self, model, another_model, session):
        session.add(model(id=1))
        session.add(another_model(id=2, other_id=1))
        session.flush()

        db = session\
            .query(another_model)\
            .options(joinedload(another_model.other, innerjoin=True))\
            .one()

        raw = another_model(id=2, other_id=1)

        compare(db, raw)

    def test_db_versus_non_db_not_equal(self, model, another_model, session):
        session.add(model(id=1))
        session.add(another_model(id=2, other_id=1))
        session.flush()

        db = session\
            .query(another_model)\
            .options(joinedload(another_model.other, innerjoin=True))\
            .one()

        raw = another_model(id=2, other=model(id=2), attr=6)

        self.check_raises(
            db, raw,
            "AnotherModel not as expected:\n"
            "\n"
            "same:\n"
            "['id']\n"
            "\n"
            "values differ:\n"
            "'attr': None != 6\n"
            "'other_id': 1 != None\n"
            '\n'
            "While comparing ['attr']: None != 6\n"
            '\n'
            "While comparing ['other_id']: 1 != None"
        )

    def test_backref_equal(self, model, another_model, session):
        db = another_model(id=2, other=model(id=1, value=2))
        session.add(db)
        session.flush()
        db = session.query(another_model).one()
        raw = another_model(id=2, other=model(id=1, value=2))
        compare(raw, db, ignore_fields=['other_id'])

    def test_ignore_fields(self, model):
        compare(model(id=1), model(id=2), ignore_fields=['id'])

    def test_hashable(self, model):
        o = model(id=1)
        mapping = {}
        assert o not in mapping
        mapping[o] = 1
        assert mapping[o] == 1
