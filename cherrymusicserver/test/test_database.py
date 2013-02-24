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

import unittest
import tempfile
import sqlite3
import os

from copy import deepcopy

from cherrymusicserver import log
log.setTest()
# log.level(log.DEBUG)

from cherrymusicserver import database as db
from cherrymusicserver.database.defs import Property, Id
from cherrymusicserver.database.connect import AbstractConnector

DBNAME = 'testdb'
testdef = {
DBNAME: {
    'versions': {
        0: {
            'types': {
                'test': [
                    Property('a', int),
                    Property('b', int),
                ],
            },
        },
        1: {
            'transition': {
                'prompt': True,
                'reason': 'copy test table to accomodate new columns',
                'sql': '''
                    ALTER TABLE test RENAME TO _bak_test;
                    CREATE TABLE test(
                        _id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        _mod INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                        a INTEGER,
                        b INTEGER
                        );
                    INSERT INTO test(_id, a, b)
                        SELECT rowid, a, b FROM _bak_test;
                    DROP TABLE _bak_test;
                ''',
             },
            'types': {
                'test': [
                    Id('_id', auto=True),
                    Property('_mod', int, notnull=True, default="(strftime('%s', 'now'))"),
                    Property('a', int),
                    Property('b', int),
                ],
            },
            'indexes': [
                {
                    'on_type': 'test', 'keys': ['a', 'b'],
                },
            ],
        },
    },
},
}


class TmpConnector(AbstractConnector):  # NOT threadsafe
    def __init__(self):
        self.testdir = tempfile.TemporaryDirectory(suffix=self.__class__.__name__)

    def __del__(self):
        self.testdir.cleanup()

    def connection(self, dbname):
        return sqlite3.connect(self.dbname(dbname))

    def dbname(self, basename):
        return os.path.join(self.testdir.name, basename)


class MemConnector(AbstractConnector):  # NOT threadsafe
    def __init__(self):
        self.Connection = type(
            self.__class__.__name__ + '.Connection',
            (sqlite3.Connection,),
            {'close': self.__disconnect})

    def __del__(self):
        self.__disconnect(seriously=True)

    def __repr__(self):
        return '{name} [{id}]'.format(
            name=self.__class__.__name__,
            id=hex(id(self._cxn)) if hasattr(self, 'cxn') else None
        )

    def connection(self, dbname):
        return self.__connect()

    def dbname(self, _):
        return ':memory:'

    def __connect(self):
        try:
            return self._cxn
        except AttributeError:
            self._cxn = sqlite3.connect(':memory:', factory=self.Connection)
            return self._cxn

    def __disconnect(self, seriously=False):
        try:
            conn = self._cxn
            if seriously:
                del self._cxn
        except AttributeError:
            pass
        else:
            if seriously:
                super(conn.__class__, conn).close()


class TestDatabaseController(unittest.TestCase):

    def setUp(self):
        self.defs = deepcopy(testdef)
        self.connector = TmpConnector()

    def tearDown(self):
        del self.connector

    def test_fullupdate(self):
        self._apply(self.defs)
        self._validate(self.defs)

    def test_update_with_transition(self):
        v0 = deepcopy(self.defs)
        del v0[DBNAME]['versions'][1]
        self._apply(v0)
        self._validate(v0)

        self._apply(self.defs)
        self._validate(self.defs)

    def test_indexes_and_tables_added_within_same_version(self):
        self._apply(self.defs)
        changedefs = deepcopy(self.defs)
        dbdef = changedefs[DBNAME]['versions'][1]
        dbdef['types']['test'] += [Property('addproperty', str)]
        dbdef['types']['addtype'] = [Property('x', int)]
        dbdef['indexes'] += [{'on_type': 'addtype', 'keys': ['x'], 'unique': True, 'name': 'addindex'}]

        self._apply(changedefs)
        self._validate(changedefs)

    def test_indexes_removed_within_same_version(self):
        self._apply(self.defs)
        changed = deepcopy(self.defs)
        del changed[DBNAME]['versions'][1]['indexes']

        self._apply(changed)
        self._validate(changed)

    def _apply(self, defdict):
        dh = db.DatabaseController(self.connector)
        dh.require(defdict)
        dh.ensure_requirements(autoconsent=True)

    def _validate(self, defdict):
        defs = db.defs.MultiDatabaseDef(defdict)
        for dbname in defs:
            conn = self.connector.bound(dbname).connection
            version = max(int(t) for t in defs[dbname].versions)

            deftypes = defs[dbname].versions[version].types or ()
            for defname in deftypes:
                self._validate_type(defname, deftypes[defname], conn)
            self._validate_no_other_types_than(deftypes, conn)

            defindexes = defs[dbname].versions[version].indexes or ()
            for idef in defindexes:
                self._validate_index(idef, conn)
            self._validate_no_other_indexes_than(defindexes, conn)

    def _validate_type(self, defname, deftype, conn):
        if not deftype:
            return
        tabledesc = db.sql.TableDescriptor(defname, (db.sql.Column.from_property(p) for p in deftype))
        tabledesc.verify(conn)

    def _validate_no_other_types_than(self, oktypes, conn):
        oknames = ('_meta_version',)
        oknames += tuple(n for n in oktypes)
        other = self._fetch_names(conn, type='table', without=oknames)
        self.assertEqual(0, len(other),
            'there must be no undefined types: %r' % (other,))

    def _validate_index(self, idxdef, conn):
        if not idxdef:
            return
        idx = db.sql.IndexDescriptor.from_def(idxdef)
        self.assertTrue(idx.exists(conn), 'index must exist: %r' % (idx,))

    def _validate_no_other_indexes_than(self, okindexes, conn):
        oknames = tuple(db.sql.IndexDescriptor.from_def(e).name for e in okindexes)
        other = self._fetch_names(conn, 'index', oknames)
        self.assertEqual(0, len(other),
            'there must be no undefined indexes: %r' % (other,))

    def _fetch_names(self, conn, type, without=()):
        sql = 'SELECT name FROM sqlite_master WHERE type=?'
        params = (type,)
        if without:
            sql += ' AND name not in (' + ', '.join('?' for _ in without) + ')'
            params += tuple(without)
        rows = conn.cursor().execute(sql, params).fetchall()
        return () if not rows else tuple(r[0] for r in rows if not r[0].startswith('sqlite'))


if __name__ == '__main__':
    unittest.main()
