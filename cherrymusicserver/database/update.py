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
'''Manage database versioning.'''

from functools import reduce

from cherrymusicserver import log
from cherrymusicserver.database.sql import TableDescriptor, IndexDescriptor, Column

_metatable = TableDescriptor('_meta_version', (
    Column('version', int),
    Column('_created', int, notnull=True, default="(strftime('%s', 'now'))")
))


def _get_user_consent(items):
    msg = '''The following database changes need your consent to continue: {}

Enter 'y' to proceed, 'n' to abort [y|N]: '''
    msg = msg.format(''.join(('\n\n- ' + s) for s in items))
    answer = None
    while not answer in ('y', 'n', ''):
        answer = input(msg).lower()
    return 'y' == answer


class Updater(object):
    '''Handle the versioning needs of a single database.

    name : str
        The name of the database to manage.
    dbdef : :class:`cherrymusicserver.database.defs.DatabaseDefinition`
        The corresponding definition.
    connector : :class:`cherrymusicserver.database.connect.AbstractConnector`
        To connect to the database.

    Raises:
        some `Exception` if database state is not consistent with the apparent version.
    '''
    def __init__(self, name, dbdef, connector):
        assert name and dbdef and connector
        self.name = name
        self.desc = dbdef
        self.db = connector.bound(self.name)
        self._init_meta()
        version = self._version
        try:
            if None != version:
                self._autoupdate_within_version(version)
                self._verify_version(version)
        except AssertionError:
            raise AssertionError('Database state inconsistent: {} ({!r})'.format(
                self.name, self.db.dbname))

    def __repr__(self):
        return 'updater({!r}, {} -> {})'.format(
                                        self.name,
                                        self._version,
                                        self._target,
                                        )

    @property
    def needed(self):
        """``True`` if the database version is less then the maximum defined."""
        version = self._version
        log.d('%s update check: version=[%s] target=[%s]', self.name, version, self._target)
        return version is None or version < self._target

    @property
    def agreed(self):
        """``True`` if the user agreed to all prompts triggered by an update."""
        prompts = list(t.reason for t in self._transitions if t.prompt)
        return not prompts or _get_user_consent(prompts)

    def run(self):
        """Update to the highest possible version and :meth:`verify`."""
        version, target = self._version, self._target
        log.i('{}: updating database schema'.format(self.name))
        log.d('from version {} to {}'.format(
            version, target))
        if None == version:
            self._jump_to_version(target)
            self._verify_version(target)
        else:
            for vnum, vdef in self._missing_versions:
                self._update_to_version(vnum)
                self._verify_version(vnum)

    def verify(self):
        """Verify that the database is at the hightest defined version.

        Raises:
            some `Exception` if out of date.
        """
        target = self._target
        self._verify_version(target)

    def reset(self):
        """Delete all content from the database along with supporting structures."""
        version = self._version
        log.i('%s: resetting database', self.name)
        log.d('version: %s', version)
        if None is version:
            tables = []     # drop tables defined in all versions
            [tables.extend(t) for t in (self._tables(v) for v in self.desc.versions)]
        else:
            tables = self._tables(version)
        with self.db.transaction('IMMEDIATE') as txn:
            for t in tables:
                t.drop_if_exists(txn)       # sqlite will drop associated indexes, too
            _metatable.drop_if_exists(txn)
            _metatable.createOrAlterTable(txn)
            self._setversion(None, txn)

    @property
    def _version(self):
        maxv = self.db.execute(
                'SELECT MAX(version) FROM _meta_version'
            ).fetchone()
        return maxv and maxv[0]

    def _setversion(self, value, conn=None):
        conn = conn or self.db.connection
        log.d('{}: set version to {}'.format(self.name, value))
        # conn.execute('UPDATE _meta SET version=?', (value,))
        conn.execute('INSERT INTO _meta_version(version) VALUES (?)',
            (value,))

    @property
    def _target(self):
        return max(int(v) for v in self.desc.versions)

    @property
    def _missing_versions(self):
        _min, _max = (self._version or -1) + 1, self._target
        keys = (int(k) for k in self.desc.versions if _min <= int(k) <= _max)
        return ((k, self.desc.versions[k]) for k in sorted(keys))

    @property
    def _transitions(self):
        if self._version is None:
            return ()
        return (v[1].transition for v in self._missing_versions
            if v[1].transition is not None)

    def _init_meta(self):
        content = self.db.execute('SELECT type, name FROM sqlite_master;').fetchall()
        content = [(t, n) for t, n in content if n != _metatable.tablename and not n.startswith('sqlite')]
        with self.db.transaction('IMMEDIATE') as conn:
            _metatable.createOrAlterTable(conn)
            if content:
                if not self._version:
                    log.d('%s: unversioned content found: %r', self.name, content)
                    self._setversion(0, conn)

    def _tables(self, vnum):
        version = vnum
        if version is None:
            return ()
        return self._types_to_tables(self.desc.versions[version].types)

    def _indexes(self, vnum):
        version = vnum
        if version is None:
            return ()
        return self._indexdefs_to_indexes(self.desc.versions[version].indexes)

    def _autoupdate_within_version(self, vnum):
        with self.db.transaction('IMMEDIATE') as txn:
            self._autotables(vnum, txn)
            self._autoindexes(vnum, txn)

    def _jump_to_version(self, vnum):
        log.d('jumpstarting database %r to version %d', self.name, vnum)
        with self.db.transaction('IMMEDIATE') as txn:
            self._autotables(vnum, txn)
            self._autoindexes(vnum, txn)
            self._setversion(vnum, txn)

    def _update_to_version(self, vnum):
        log.d('updating database %r to version %d', self.name, vnum)
        with self.db.transaction('IMMEDIATE') as txn:
            self._make_transition(self.desc.versions[vnum].transition, txn)
            self._autotables(vnum, txn)
            self._autoindexes(vnum, txn)
            self._setversion(vnum, txn)

    def _verify_version(self, vnum):
        vdef = self.desc.versions[vnum]
        assert self._version == vnum, '%r != %r' % (self._version, vnum)
        conn = self.db.connection
        for table in self._types_to_tables(vdef.types):
            table.verify(conn)
        for idx in self._indexdefs_to_indexes(vdef.indexes):
            idx.verify(conn)
        log.d('%s: verified version %s', self.name, vnum)

    def _make_transition(self, transition, conn):
        if transition and transition.sql:
            conn.executescript(transition.sql)

    def _autotables(self, vnum, conn):
        for table in self._tables(vnum):
            table.createOrAlterTable(conn)

    def _autoindexes(self, vnum, conn):
        defined = self._indexes(vnum)
        for idx in defined:
            if not idx.exists(conn):
                idx.create(conn)
        defined_normals = tuple(i.normal for i in defined)
        for inorm in IndexDescriptor.fetch_normal_existing(conn):
            if inorm not in defined_normals:
                inorm._as_descriptor.drop_if_exists(conn)

    def _types_to_tables(self, types):
        if not types:
            return ()
        tables = []
        for name in types:
            props = types[name]
            tabledesc = TableDescriptor(
                name,
                (Column.from_property(p) for p in props)
            )
            tables.append(tabledesc)
        return tables

    def _indexdefs_to_indexes(self, indexdefs):
        if not indexdefs:
            return ()
        return [IndexDescriptor.from_def(idx) for idx in indexdefs]


class MultiUpdater(object):
    '''Manage the state of multiple databases at once.

    defs : :class:`cherrymusicserver.database.defs.MultiDatabaseDef`
        Definitions of all databases to manage.
    connector : :class:`cherrymusicserver.database.connect.AbstractConnector`
        For connecting to the databases.
    '''
    def __init__(self, defs, connector):
        assert defs and connector
        self.updaters = tuple(Updater(k, defs[k], connector) for k in defs)

    @property
    def needed(self):
        """``True`` if any database needs updating.

        See :meth:`Updater.needed`
        """
        return reduce(lambda accu, u: accu or u.needed, self.updaters, False)

    @property
    def agreed(self):
        """``True`` if the user agrees to all prompts triggered by a general update."""
        prompts = []
        for u in self.updaters:
            prompts += (t.reason for t in u._transitions if t.prompt)
        return not prompts or _get_user_consent(prompts)

    def run(self):
        """Update all databases with out of date versions.

        See :meth:`Updater.run`
        """
        for u in self.updaters:
            if u.needed:
                u.run()
