from unittest import TestCase
from mortar_mixins.common import Common
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from testfixtures import compare


class MoreTests(TestCase):

    def setUp(self):
        self.Base = declarative_base()
        class Model(Common, self.Base):
            id = Column(Integer, primary_key=True)
        self.Model = Model

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

    def test_compare(self):
        compare(
            [self.Model(id=1), self.Model(id=2)],
            [self.Model(id=1), self.Model(id=2)]
            )

    def test_repr(self):
        compare('Model(id=1)', repr(self.Model(id=1)))

    def test_str(self):
        compare('Model(id=1)', str(self.Model(id=1)))
