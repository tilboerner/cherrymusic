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
'''SQL Database handling.'''

import sqlite3
import os.path

from collections import OrderedDict, namedtuple
from collections.abc import Callable

from cherrymusicserver import log
from cherrymusicserver.database import defs
from cherrymusicserver.database.connect import AbstractConnector


class SQLiteConnector(AbstractConnector):
    '''Connector for SQLite3 databases.

    datadir: str
        Base directories of database files.
    suffix: str (optional)
        Suffix to append to database filenames.
    connargs: dict (optional)
        Dictionary with keyword args to pass on to sqlite3.Connection.
    '''
    def __init__(self, datadir='', suffix='', connargs={}):
        self.datadir = datadir
        self.suffix = suffix
        self.connargs = connargs

    def connection(self, dbname):
        return sqlite3.connect(self.dbname(dbname), **self.connargs)

    def dbname(self, basename):
        if self.suffix:
            basename = os.path.extsep.join((basename, self.suffix))
        return os.path.join(self.datadir, basename)


class TableDescriptor(object):
    def __init__(self, tablename, columns):
        self.tablename = tablename
        self.columns = OrderedDict()
        for column in columns:
            if not isinstance(column, (Column,)):
                raise TypeError("column must be of type TableColumn")
            self.columns[column.name] = column

    def _table_exists(self, sqlconn):
        return sqlconn.execute("""SELECT 1 FROM sqlite_master
            WHERE type='table' AND name=? """, (self.tablename,)).fetchall()

    def _get_tablelayout(self, sqlconn):
        cursor = sqlconn.execute("PRAGMA table_info('%s')" % self.tablename)
        cols = (Column.from_rowinfo(row) for row in cursor.fetchall())
        return OrderedDict((c.name, c) for c in cols)

    def _check_existing_columns(self, existing_layout):
        for column in existing_layout.values():
            if column.name not in self.columns:
                log.e('Column exists, but is not defined: %s.%s',
                      self.tablename, column.name)
                log.d('%s', column)
            elif column.normal != self.columns[column.name].normal:
                log.e('''%s.%s: existing column differs from definition:
                      existing: %s
                      expected: %s''',
                      self.tablename, column.name,
                      column.normal, self.columns[column.name].normal)
            else:
                log.d('Column %s.%s exists and needs no change',
                      self.tablename, column.name)

    def _add_missing_columns(self, existing, sqlconn):
        table_changed = False
        add = filter(lambda c: c.name not in existing, self.columns.values())
        for column in add:
            log.d('Adding column %s.%s', self.tablename, column.name)
            log.d('%s', column)
            log.d('SQL: %r', column.sql)
            sql = "ALTER TABLE %s ADD COLUMN %s" % (self.tablename, column.sql)
            sqlconn.execute(sql)
            table_changed = True
        return table_changed

    def _create_table(self, sqlconn):
        log.d('Creating table %s' % self.tablename)
        cols = ', '.join(map(lambda x: x.sql, self.columns.values()))
        stmt = """CREATE TABLE "%s" (%s)""" % (self.tablename, cols)
        log.d('SQL %r', stmt)
        sqlconn.execute(stmt)

    def verify(self, sqlconn):
        assert self._table_exists(sqlconn), 'missing table %r' % (self.tablename,)
        defined = self.columns
        for column in self._get_tablelayout(sqlconn).values():
            assert column.name in defined, 'Column %s.%s exists, but is not defined' % (
                self.tablename,
                column.name)
            expected = defined[column.name].normal
            assert column.normal == expected, 'Existing column %s differs from definition:\n%s !=\n%s' % (
                '.'.join((self.tablename, column.name)),
                column.normal,
                expected)

    def createOrAlterTable(self, sqlconn):
        updatedTable = False
        if self._table_exists(sqlconn):
            existing_layout = self._get_tablelayout(sqlconn)
            self._check_existing_columns(existing_layout)
            updatedTable = self._add_missing_columns(existing_layout, sqlconn)
        else:
            self._create_table(sqlconn)
            updatedTable = True
        return updatedTable

    def drop_if_exists(self, sqlconn):
        log.d('dropping table %r (if exists)', self.tablename)
        cur = sqlconn.cursor()
        cur.execute('DROP TABLE IF EXISTS "%s";' % (self.tablename,))


class Column(namedtuple('Column', 'name type notnull default pkey unique')):

    _NormTuple = namedtuple('NormColumn', 'name type notnull default pkey')

    @classmethod
    def from_rowinfo(cls, rowdata):
        name, ctype, notnull, default, pkey = tuple(rowdata)[1:]
        typefactory = cls.__get_datatype_factory(ctype)
        assert typefactory, 'unknown type %r of column %r' % (ctype, name)
        if None != default:
            try:
                default = typefactory(default)
            except (ValueError, TypeError):
                pass
        return Column(name, ctype, notnull, default, pkey)

    @classmethod
    def from_property(cls, prop):
        if isinstance(prop, defs.Id):
            pkey = 'AUTOINCREMENT' if prop.auto else True
            return Column(prop.name, int, notnull=True, default=None,
                          pkey=pkey, unique=False)
        return Column(prop.name, prop.type, prop.notnull, prop.default,
                      unique=prop.unique)

    def __new__(cls, name, type, notnull=0, default=None, pkey='', unique=''):
        name = cls.__normalize_name(name)
        type = cls.__normalize_datatype(type)
        return super().__new__(cls, name, type, notnull, default, pkey, unique)

    @property
    def normal(self):
        default = self.default
        if isinstance(default, str):
            default = default.strip().lstrip('(').rstrip(')')
        return self._NormTuple(
                              self.name,
                              self.type,
                              1 if self.notnull else 0,
                              default,
                              1 if self.pkey else 0
                              )

    @property
    def sql(self):
        elements = ['"%s"' % self.name]
        if self.type:
            elements.append(self.type)
        if self.pkey:
            flags = isinstance(self.pkey, str) and (' ' + self.pkey)
            elements.append('PRIMARY KEY' + (flags or ''))
        if self.notnull:
            elements.append('NOT NULL')
        if self.unique:
            elements.append('UNIQUE')
        if self.default != None:
            elements.append('DEFAULT %s' % self.default)
        return ' '.join(elements)

    @classmethod
    def __normalize_name(self, name):
        return name.lower()

    __datatypes = {
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'TEXT': 'TEXT',
        'BLOB': 'BLOB',
        'REAL': 'REAL',
        int: 'INTEGER',
        str: 'TEXT',
        bytes: 'BLOB',
        float: 'REAL',
    }

    @classmethod
    def __get_datatype_factory(cls, datatype):
        matches = list(k for k, v in cls.__datatypes.items()
                        if v == cls.__datatypes[datatype.upper()]
                        and isinstance(k, Callable))
        return matches and matches[0]

    @classmethod
    def __normalize_datatype(self, datatype):
        if isinstance(datatype, str):
            datatype = datatype.upper()
        try:
            return self.__datatypes[datatype]
        except KeyError:
            raise ValueError("column cannot have datatype: %r" % datatype)


class IndexDescriptor(object):

    class Normal(namedtuple('IndexNormal', 'name table columns unique')):
        def __new__(cls, name, table, cols, unique=False):
            return super().__new__(cls, name, table, cols, bool(unique))

        @property
        def _as_descriptor(self):
            return IndexDescriptor.from_normal(self)

    @classmethod
    def from_normal(cls, idxtuple):
        i = IndexDescriptor(*(idxtuple[1:]), name=idxtuple.name)
        assert i.name == idxtuple.name
        return i

    @classmethod
    def from_def(cls, idxdef):
        return IndexDescriptor(idxdef.on_type, tuple(idxdef.keys), bool(idxdef.unique), idxdef.name)

    def __init__(self, table, columns, unique=False, name=None):
        self.table = table
        self.columns = columns
        self.unique = unique
        self.name = name or self._create_name()

    def _create_name(self):
        typename = 'uidx' if self.unique else 'idx'
        components = (typename, self.table,) + self.columns
        return '_'.join(components)

    def __repr__(self):
        norm = self.normal
        return '{}({})'.format(
            self.__class__.__name__,
            'table={}, columns={}, unique={}'.format(
                norm.table, norm.columns, norm.unique
            )
        )

    @property
    def normal(self):
        return self.Normal(self.name, self.table, self.columns, self.unique)

    def exists(self, conn):
        have = self.fetch_normal_existing(conn, table=self.table)
        return self.normal in have

    def verify(self, conn):
        assert self.exists(conn), 'missing index %r' % (self.name,)

    def create(self, conn):
        idx = self.normal
        sql = 'CREATE {unq} INDEX "{name}" ON "{table}"({cols});'.format(
            unq=('UNIQUE' if idx.unique else ''),
            name=idx.name,
            table=idx.table,
            cols=', '.join('"%s"' % (s,) for s in idx.columns)
        )
        log.d('creating index %r', idx.name)
        log.d('SQL %r', sql)
        conn.execute(sql)

    def drop_if_exists(self, conn):
        log.d('dropping index %r (if exists)', self.name)
        conn.execute('DROP INDEX IF EXISTS {};'.format(self.name))

    @classmethod
    def fetch_normal_existing(cls, conn, table=None):
        masterinfo = namedtuple('masterinfo', 'type name table rootpage sql')
        idxinfo = namedtuple('indexinfo', 'seqno cid colname')
        listitem = namedtuple('indexlist_item', 'seqno name unique')

        cursor = conn.cursor()
        sql = 'SELECT * FROM sqlite_master WHERE type=?'
        args = ('index',)
        if table:
            sql += ' AND tbl_name=?'
            args += (table,)
        rows = cursor.execute(sql, args).fetchall()
        tables = dict((m.name, m.table) for m in (masterinfo(*r) for r in rows))

        cols = {}
        for idxname in tables:
            rows = cursor.execute(
                'PRAGMA index_info({});'.format(idxname)
            ).fetchall()
            infos = (idxinfo(*r) for r in rows)
            cols[idxname] = tuple(i.colname for i in infos)

        uniq = {}
        tablenames = set(tables.values())
        for tablename in tablenames:
            rows = cursor.execute(
                'PRAGMA INDEX_LIST({});'.format(tablename)
            ).fetchall()
            for item in (listitem(*r) for r in rows):
                uniq[item.name] = item.unique

        indexes = ((n, t, cols[n], uniq[n]) for n, t in tables.items())
        return tuple(cls.Normal(*i) for i in indexes)
