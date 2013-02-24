#!/usr/bin/python3
#
# CherryMusic - a standalone music server
# Copyright (c) 2012 Tom Wallroth & Tilman Boerner
#
# Project page:
#   http://fomori.org/cherrymusic/
# Sources on github:
#   http://github.com/devsnd/cherrymusic/
#
# CherryMusic is based on
#   jPlayer (GPL/MIT license) http://www.jplayer.org/
#   CherryPy (BSD license) http://www.cherrypy.org/
#
# licensed under GNU GPL version 3 (or later)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>
#
'''Define data and infrastructure requirements for databases.'''

from collections import namedtuple

try:
    from collections.abc import Callable, MutableSequence
except ImportError:
    from collections import Callable, MutableSequence


_identity = lambda self, x: x


class Property(namedtuple('_InlineNamedtuple', 'name type notnull default unique')):
    '''The primitive property of a datatype contained in a database.

    name : str
        The handle of this property. Unique in the datatype.
    type : type
        The type of value this property can have.
        One of ``(int, float, str, bytes)``.
    notnull : bool
        Disallow values of ``None``. (optional)
    default : (-> value of type)
        The default value of this property. (optional)
    unique : bool
        .. deprecated:: forever
            Included for historical reasons. Use :class:`IndexDefinition` to define a
            unique index instead.
    '''
    def __new__(cls, name, type, notnull=False, default=None, unique=False):
        return super().__new__(cls, name, type, notnull, default, unique)


class Id(namedtuple('_InlineNamedtuple', 'name auto')):
    '''An integer id key as a property of a datatype in a database; only one 
    allowed per datatype.

    name : str
        The handle this id property goes by. Unique in the datatype.
    auto : bool
        Autogenerate values when ``True``.
    '''
    def __new__(cls, name, auto=False):
        return super().__new__(cls, name, auto)


def _Definition(name, properties):
    name += 'Definition'
    proprule = '`{}`'.format(name)
    dic = {}
    productions = []
    for key, val in properties.items():
        assert isinstance(val, Callable), 'properties values must be callables'
        dic[key] = None     # class vars == instance var default values
        productions.append("{}: {}".format(
            key,
            val.__productionrule if hasattr(val, '__productionrule') else val.__name__))
    docstr = """
.. productionlist::
    {name!s}: {{ {keys} }}
    {defs}
""".format(
        name=name,
        keys=', '.join('`%s`' % (p,) for p in properties),
        defs='\n    '.join(productions)
    )
    dic['__productionrule'] = proprule
    dic['__doc__'] = docstr
    dic['_factories'] = properties
    return type(name, (_GenericDefinition,), dic)


def MapOf(factory):
    name = 'MapOf({})'.format(factory.__name__)
    val = factory
    docname = 'MapOf ( {} )'.format(val.__productionrule if hasattr(val, '__productionrule') else val.__name__)
    assert isinstance(factory, Callable)
    return type(name, (_GenericDefinition,), {
        # '__doc__': docstring,
        '__productionrule': docname,
        '_factories': _FixedType(factory)
    })


def ListOf(factory):
    name = 'ListOf({})'.format(factory.__name__)
    val = factory
    docname = 'ListOf ( {} )'.format(val.__productionrule if hasattr(val, '__productionrule') else val.__name__)
    assert isinstance(factory, Callable)
    return type(name, (_ListDefinition,), {
        '__productionrule': docname,
        '_factory': factory
    })


class _GenericDefinition(object):

    _factories = {}

    def __init__(self, desc={}):
        for name, value in desc.items():
            self[name] = value

    def __repr__(self):
        return '{}: {}'.format(self.__class__.__name__, self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __getitem__(self, name):
        return getattr(self, str(name))

    def __setitem__(self, name, value):
        setattr(self, str(name), value)

    def __setattr__(self, name, value):
        try:
            factory = self._factories[name]
        except KeyError:
            raise AttributeError('{} has no attribute {!r}'.format(
                       self.__class__.__name__,
                       name))
        else:
            value = factory(value)
            super().__setattr__(name, value)


class _ListDefinition(MutableSequence):
    _factory = _identity

    def __init__(self, desc=()):
        self._list = []
        for item in desc:
            self.append(item)

    def __len__(self):
        return len(self._list)

    def __contains__(self, item):
        return item in self._list

    def __iter__(self):
        return iter(self._list)

    def __getitem__(self, idx):
        return self._list[idx]

    def __setitem__(self, idx, value):
        self._list[idx] = self._factory(value)

    def __delitem__(self, idx):
        del self._list[idx]

    def insert(self, idx, value):
        self._list.insert(idx, self._factory(value))


class _FixedType(object):

    def __init__(self, factory):
        assert isinstance(factory, Callable)
        self.factory = factory

    def __getitem__(self, name):
        return self.factory

    def __contains__(self, name):
        return True


TransitionDefinition = _Definition('Transition', {
    'prompt': bool,
    'reason': str,
    'sql': str,
})

IndexDefinition = _Definition('Index', {
    'on_type': str,
    'keys': ListOf(str),
    'unique': bool,
    'name': str,
})


def PropertyDefinition(owner, prop):
    assert isinstance(prop, (Property, Id))
    return prop
PropertyDefinition.__productionrule = '`Property` | `Id`'

DatabaseVersionDefinition = _Definition('DatabaseVersion', {
    'transition': TransitionDefinition,
    'types': MapOf(ListOf(PropertyDefinition)),
    'indexes': ListOf(IndexDefinition),
})

DatabaseDefinition = _Definition('Database', {
    'versions': MapOf(DatabaseVersionDefinition),
})

MultiDatabaseDef = MapOf(DatabaseDefinition)
