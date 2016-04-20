from unittest import TestCase
from sqlalchemy.orm import relationship, joinedload
from mortar_mixins.common import Common
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from testfixtures import compare
from mortar_rdb import get_session
from mortar_rdb.testing import register_session


class SetupModels(object):

    def setUp(self):
        register_session(transactional=False)

        self.Base = declarative_base()

        class Model(Common, self.Base):
            id =  Column(Integer, primary_key=True)
            value = Column(Integer)

        class AnotherModel(Common, self.Base):
            id = Column(Integer, primary_key=True)
            attr = Column(Integer)
            other_id = Column(Integer, ForeignKey('model.id'))
            other = relationship("Model", backref='another')

        self.Model = Model
        self.AnotherModel = AnotherModel

        self.session = get_session()
        self.addCleanup(self.session.rollback)
        self.Base.metadata.create_all(self.session.bind)


class CommonTests(SetupModels, TestCase):

    def test_table_name(self):
        compare(self.Model.__table__.name, 'model')

    def test_eq_wrong_type(self):
        self.assertFalse(self.Model() == object())

    def test_eq_wrong_model_type(self):
        class OtherModel(Common, self.Base):
            id = Column(Integer, primary_key=True)
        self.assertFalse(self.Model(id=1) == OtherModel(id=1))

    def test_eq_different(self):
        self.assertFalse(self.Model(id=1) == self.Model(id=2))

    def test_eq_different_keys(self):
        self.assertFalse(self.Model() == self.Model(id=2))

    def test_eq_same(self):
        self.assertTrue(self.Model(id=1) == self.Model(id=1))

    def test_ne_wrong_type(self):
        self.assertTrue(self.Model() != object())

    def test_ne_wrong_model_type(self):
        class OtherModel(Common, self.Base):
            id = Column(Integer, primary_key=True)
        self.assertTrue(self.Model(id=1) != OtherModel(id=1))

    def test_ne_different(self):
        self.assertTrue(self.Model(id=1) != self.Model(id=2))

    def test_ne_same(self):
        self.assertTrue(self.Model(id=1) != self.Model(id=2))

    def test_repr(self):
        compare('Model(id=1, value=3)', repr(self.Model(id=1, value=3)))

    def test_str(self):
        compare('Model(id=3, value=1)', str(self.Model(id=3, value=1)))

    def test_repr_relationships_excluded(self):
        model = self.AnotherModel(id=1, other=self.Model(id=1, value=42))
        self.session.add(model)
        self.session.flush()
        compare('AnotherModel(id=1, other_id=1)',
                actual=str(model))


class CompareTests(SetupModels, TestCase):

    def check_raises(self, x, y, message, **kw):
        try:
            compare(x, y, **kw)
        except Exception as e:
            if not isinstance(e, AssertionError):
                raise # pragma: no cover
            actual = e.args[0]
            if actual != message: # pragma: no cover
                self.fail(compare(actual, expected=message,
                                  show_whitespace=True,
                                  raises=False))
        else: # pragma: no cover
            self.fail('No exception raised!')

    def test_identical(self):
        compare(
            [self.Model(id=1), self.Model(id=2)],
            [self.Model(id=1), self.Model(id=2)]
            )

    def test_different(self):
        self.check_raises(
            self.Model(id=1), self.Model(id=2),
            "Model not as expected:\n"
            '\n'
            'same:\n'
            "['value']\n"
            "\n"
            "values differ:\n"
            "'id': 1 != 2"
        )

    def test_different_types(self):
        self.check_raises(
            self.Model(id=1), self.AnotherModel(id=1),
            "Model(id=1) != AnotherModel(id=1)"
        )

    def test_db_versus_non_db_equal(self):
        self.session.add(self.Model(id=1))
        self.session.add(self.AnotherModel(id=2, other_id=1))
        self.session.commit()

        db = self.session\
            .query(self.AnotherModel)\
            .options(joinedload(self.AnotherModel.other, innerjoin=True))\
            .one()

        raw = self.AnotherModel(id=2, other_id=1)

        compare(db, raw)

    def test_db_versus_non_db_not_equal(self):
        self.session.add(self.Model(id=1))
        self.session.add(self.AnotherModel(id=2, other_id=1))
        self.session.commit()

        db = self.session\
            .query(self.AnotherModel)\
            .options(joinedload(self.AnotherModel.other, innerjoin=True))\
            .one()

        raw = self.AnotherModel(id=2, other=self.Model(id=2), attr=6)

        self.check_raises(
            db, raw,
            "AnotherModel not as expected:\n"
            "\n"
            "same:\n"
            "['id']\n"
            "\n"
            "values differ:\n"
            "'attr': None != 6\n"
            "'other_id': 1 != None"
        )

    def test_backref_equal(self):
        db = self.AnotherModel(id=2, other=self.Model(id=1, value=2))
        self.session.add(db)
        self.session.commit()
        db = self.session.query(self.AnotherModel).one()
        raw = self.AnotherModel(id=2, other=self.Model(id=1, value=2))
        compare(raw, db, ignore_fields=['other_id'])

    def test_ignore_fields(self):
        compare(self.Model(id=1), self.Model(id=2), ignore_fields=['id'])
