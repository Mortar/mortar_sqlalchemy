import re
from typing import Type

from sqlalchemy import inspect
from sqlalchemy.orm import declared_attr

name_re = re.compile('([a-z]|^)([A-Z])')


def name_subber(match):
    if match.group(1):
        start = match.group(1)+'_'
    else:
        start = ''
    return start+match.group(2).lower()


def de_hump(cls):
    "SomeThing -> some_thing"
    return name_re.sub(name_subber,cls.__name__)


def comparable_attributes(obj):
    state = inspect(obj)
    relationships = state.mapper.relationships
    for name, attr in state.attrs.items():
        if name in relationships:
            continue
        yield name, attr.value


class Common:
    """
    This mixin provides a common set of functionality that is slightly opinionated.
    """
    
    @declared_attr
    def __tablename__(cls: Type) -> str:
        return de_hump(cls)
    
    def __eq__(self, other) -> bool:
        """
        Instances of this class are considered equal if values of all the
        mapped attributes are equal. Relationships are excluded from this,
        but the attributes that are used to form them are included.
        """
        if type(self) is not type(other):
            return False
        other_attrs = inspect(other).attrs
        for name, value in comparable_attributes(self):
            if value != other_attrs[name].value:
                return False
        return True

    def __hash__(self) -> int:
        """
        The Python identity of instances of this class is used as its hash.
        """
        return id(self)

    def __ne__(self, other) -> bool:
        return not (self == other)

    def __repr__(self) -> str:
        """
        The :func:`repr` of instances of this class shows the values of all
        mapped attributes.
        """
        content = []
        for name, value in sorted(comparable_attributes(self)):
            if value is not None:
                content.append('%s=%r' % (name, value))
        return '%s(%s)' % (self.__class__.__name__, ', '.join(content))

    __str__ = __repr__


def compare_common(x, y, context):

    ignore_fields = context.get_option('ignore_fields', ())

    if type(x) is not type(y):
        return compare_simple(x, y, context)

    args = []

    for obj in x, y:
        attrs = {}
        for name, value in comparable_attributes(obj):
            if name in ignore_fields:
                continue
            attrs[name] = value
        args.append(attrs)

    if ignore_fields and args[0]==args[1]:
        return

    args.append(context)
    args.append(x)

    return _compare_mapping(*args)


try:
    from testfixtures.comparison import (
        register, _compare_mapping, compare_simple
    )
except ImportError:  # pragma: no cover
    pass
else:
    register(Common, compare_common)
