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
"""CherryMusic database definition, versioning and control.

To support schema changes that are not backward-compatible, databases can be
versioned. A *version* is defined as a  collection of
*types*, *indexes* and a *transition* from the previous version.

| Database --> Versions
| Version --> Types, Indexes, Transition

**Types** have a name and a number of *properties*, which are handles for
typed, primitive values. Properties can be: ``(int, float, str, bytes)``.
They can be ``notnull``, which excludes ``None``; a ``default`` value is
also accepted. There's a special property called ``Id``, which is a
``notnull`` ``int`` that can be freely named. Pass ``auto=True`` and
unique values will be auto-generated. Only one ``Id`` can be used per type.

**Indexes** are for quick lookups of *type* instances using certain properties
as keys. It's possible to choose their name, but by default the name will
be auto-generated. *Uniqueness* of property values can be enforced by
creating an *unique index* on them. ``Id`` properties are indexed
automatically.

**Versioning** A database definition can be changed in some ways without incrementing
its version. It's possible to:

* add types;
* add indexes;
* remove indexes;
* add non-``Id`` properties with

    * ``notnull == False`` or
    * ``notnull == True`` and a ``default != None``.

For all other changes, a new version needs to be created.

**Version numbers:** Versions are kept in a map with integer keys.
The highest version number is the assumed target: A fresh database
will be "jumpstarted" to this version without making use of any
transitions. A database with existing content will be taken through the
applicable transitions.
``0`` is the implicit version of a non-empty, unversioned
database. No transtitions will be applied to reach this version.

A **transition** consists of a *script* that updates the database schema
from the previous version and an optional *reason* for the
change. A *prompt* flag can also be set; when ``True``, executing the
transition requires user consent. In this case, the reason will be displayed.
This mechanism can be used to give a warning or offer other options.

See :mod:`.defs`.

.. _dbdef_example:

Example definition
------------------

Pass a dict like this to :meth:`DatabaseController.require`::

    from cherrymusicserver.database.defs import Id, Property
    defdict = {
        'somedb': {
            'versions': {
                0: {
                    # ...
                },
                1: {
                    'types': {
                        'sometype': [
                            Id('_id', auto=True),
                            Property('someprop', str, notnull=True, default=''),
                            # ...
                        ],
                    },
                    'indexes': [
                        {'on_type': 'sometype', 'keys': ['someprop',], 'unique': True},
                    ],
                    'transition': {
                        'prompt': True,
                        'reason': 'A stitch in time saves nine',
                        'sql' : "[SQL SCRIPT]"
                    },
                },
            },
        },
    }

"""

from cherrymusicserver import log
from cherrymusicserver.database.update import Updater, MultiUpdater
from cherrymusicserver.database.defs import MultiDatabaseDef


class DatabaseController(object):
    '''
    Supply an :class:`.connect.AbstractConnector` instance that the
    DatabaseController and client modules will use to connect to the actual
    database(s).
    '''

    def __init__(self, connector):
        self.defs = MultiDatabaseDef({})
        self.connector = connector

    def require(self, defdict):
        '''Define one or more databases that the client intends to use.

        defdict: dict
            Definition dictionary; see :ref:`example <dbdef_example>`.

        Raises:
            ValueError : If the definition contains a database name that is already in use.
        '''
        dupekeys = set(self.defs) & set(defdict)
        if dupekeys:
            raise ValueError('name(s) already in use: {}'.format(dupekeys))
        for name, value in defdict.items():
            self.defs[name] = value

    def ensure_requirements(self, dbname=None, autoconsent=False):
        '''Make sure all databases defined via :meth:`require` are up to date.

        Will connect to all these databases and try to update them, if
        necessary, possibly asking the user for consent.

        dbname : str
            When given, only make sure of the database with that name.
        autoconsent : bool
            When ``True``, don't ask for consent, ever.
        Returns : bool
            ``True`` if requirements are met.
        '''
        update = self._create_updater(dbname)
        if update.needed:
            log.i('database definition out of date')
            if not (autoconsent or update.agreed):
                return False
            update.run()
            log.i('database definition updated')
        return True

    def dbdef(self, dbname):
        '''Return the effective definition for a database.

        Raises:
            ValueError : If dbname is not a defined database name.
        '''
        try:
            dbdef = self.defs[dbname]
        except KeyError:
            raise ValueError('database %r is not defined' % (dbname,))
        maxversion = max(int(k) for k in dbdef.versions)
        return dbdef.versions[maxversion]

    def resetdb(self, dbname):
        '''Delete all content and defined data structures from a database.

        Raises:
            ValueError : If dbname is ``None`` or empty, or not a defined database name.
        '''
        if not dbname:
            raise ValueError('dbname must not be empty or None')
        updater = self._create_updater(dbname)
        updater.reset()

    def _create_updater(self, dbname):
        '''Create an :class: .update.Updater for dbname, or, if no name is
        given, an :class: .update.MultiUpdater for all defined databases.

        Raises:
        ValueError if dbname is not defined.
        '''
        if dbname:
            try:
                dbdef = self.defs[dbname]
            except KeyError:
                raise ValueError('database %r is not defined' % (dbname,))
            update = Updater(dbname, dbdef, self.connector)
        else:
            update = MultiUpdater(self.defs, self.connector)
        return update
