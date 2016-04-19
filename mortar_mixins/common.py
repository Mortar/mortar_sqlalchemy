import re
from sqlalchemy import inspect
from sqlalchemy.orm.state import AttributeState
from sqlalchemy.util import classproperty

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
        rel = relationships.get(name)
        if rel is not None:
            if any(r.backref == name for r in rel._reverse_property):
                continue
        yield name, attr.value, rel is not None


class Common(object):
    
    @classproperty
    def __tablename__(cls):
        return de_hump(cls)
    
    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        other_attrs = inspect(other).attrs
        for name, value, _ in comparable_attributes(self):
            if value != other_attrs[name].value:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        content = []
        for name, value, is_relationship in sorted(comparable_attributes(self)):
            if value is None or is_relationship:
                continue
            content.append('%s=%r' % (name, value))
        return '%s(%s)' % (self.__class__.__name__, ', '.join(content))

    __str__ = __repr__


def compare_common(x, y, context):

    check_relationships = context.get_option('check_relationships', False)
    ignore_fields = context.get_option('ignore_fields', ())

    if type(x) is not type(y):
        return compare_simple(x, y, context)

    args = []

    for obj in x, y:
        attrs = {}
        for name, value, is_relationship in comparable_attributes(obj):
            if name in ignore_fields:
                continue
            elif is_relationship and not check_relationships:
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
except ImportError: # pragma: no cover
    pass
else:
    register(Common, compare_common)
