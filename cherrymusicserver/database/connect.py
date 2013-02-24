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
'''
Connect to databases.
'''


# from abc import ABCMeta, abstractmethod   # don't, for py2 compatibility
from contextlib import contextmanager


# class AbstractConnector(metaclass=ABCMeta):
class AbstractConnector(object):
    '''Provide database connections by name.

    Override :meth:`connection` and :meth:`dbname` to subclass.
    '''
    def __repr__(self):
        return '{} [{}]'.format(self.__class__.__name__, hex(id(self)))

    # @abstractmethod
    def connection(self, dbname):
        '''Return a connection object to talk to a database.'''
        raise NotImplementedError('abstract method')

    # @abstractmethod
    def dbname(self, basename):
        '''Return the internal handle used for the database with ``basename``.'''
        raise NotImplementedError('abstract method')

    def bound(self, dbname):
        '''Return a :class:`BoundConnector` bound to the database with ``dbname``.'''
        return BoundConnector(dbname, self)


class BoundConnector(object):
    '''Provide connections to a specific database name.'''
    def __init__(self, dbname, baseconnector):
        self.name = dbname
        self.baseconnector = baseconnector

    def __repr__(self):
        return 'Connector({}, {})'.format(self.name, repr(self.baseconnector))

    @property
    def dbname(self):
        '''Return the internal handle used for the bound database.'''
        return self.baseconnector.dbname(self.name)

    @property
    def connection(self):
        '''Return a connection object to talk to the bound database.'''
        return self.baseconnector.connection(self.name)

    @contextmanager
    def transaction(self, mode=None):
        '''*Context Manager*; return a connection to the bound database,
        wrapped in an active transaction and commit active transactions on exit.'''
        con = self.connection
        con.isolation_level = mode
        with con:
            yield con

    def execute(self, query, params=()):
        '''Connect to the bound database and execute a query; then return the
        cursor object used.'''
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor
