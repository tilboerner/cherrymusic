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

import cherrymusicserver as cherry
from cherrymusicserver import log
from cherrymusicserver.cherrymodel import MusicEntry
try:
    from urllib.parse import unquote
except ImportError:
    from backport.urllib.parse import unquote
from cherrymusicserver.database.defs import Id, Property

DBNAME = 'playlist'
cherry.db.require({
    DBNAME: {
        'versions': {
            0: {
                'types': {
                    'playlists': [
                        Property('title', str),
                        Property('userid', int),
                        Property('public', int),
                    ],
                    'tracks': [
                        Property('playlistid', int),
                        Property('track', int),
                        Property('url', str),
                        Property('title', str),
                    ],
                },
            },
            1: {
                'transition': {
                    'sql': '''
                        CREATE TABLE playlists_copy (
                            _id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            title TEXT,
                            userid INTEGER,
                            public INTEGER
                        );
                        INSERT INTO playlists_copy
                            SELECT rowid, title, userid, public FROM playlists;
                        DROP TABLE playlists;
                        ALTER TABLE playlists_copy RENAME TO playlists;

                        CREATE TABLE tracks_copy (
                            _id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            playlistid INTEGER,
                            track INTEGER,
                            url TEXT,
                            title TEXT
                        );
                        INSERT INTO tracks_copy(playlistid, track, url, title)
                            SELECT playlistid, track, url, title FROM tracks;
                        DROP TABLE tracks;
                        ALTER TABLE tracks_copy RENAME TO tracks;
                    ''',
                },
                'types': {
                    'playlists': [
                        Id('_id', auto=True),
                        Property('title', str),
                        Property('userid', int),
                        Property('public', int),
                    ],
                    'tracks': [
                        Id('_id', auto=True),
                        Property('playlistid', int),
                        Property('track', int),
                        Property('url', str),
                        Property('title', str),
                    ],
                },
                'indexes': [
                    {'on_type': 'tracks', 'keys': ['playlistid'], },
                ]
            },
        },
    },
}
)

class PlaylistDB:
    def __init__(self, connector=None):
        if connector is None:
            connector = cherry.db.connector
        self.conn = connector.bound(DBNAME)

    def deletePlaylist(self, plid, userid):
        cursor = self.conn.cursor()
        cursor.execute("""DELETE FROM playlists WHERE rowid = ? and userid = ?""",(plid,userid))
        cursor.execute("""DELETE FROM tracks WHERE playlistid = ?""",(plid,))
        self.conn.commit()

    def savePlaylist(self, userid, public, playlist, playlisttitle):
        if not len(playlist):
            return 'I will not create an empty playlist. sorry.'
        duplicatetitles = self.conn.execute("""SELECT * FROM playlists
            WHERE userid = ? AND title = ?""",(userid,playlisttitle)).fetchall()
        if not duplicatetitles:
            cursor = self.conn.cursor()
            cursor.execute("""INSERT INTO playlists
                (title, userid, public) VALUES (?,?,?)""",
                (playlisttitle, userid, 1 if public else 0))
            playlistid = cursor.lastrowid;
            #put tracknumber to each track
            numberedplaylist = []
            for entry in zip(range(len(playlist)), playlist):
                track = entry[0]
                song = entry[1]
                numberedplaylist.append((playlistid, track, song['mp3'], song['title']))
            cursor.executemany("""INSERT INTO tracks (playlistid, track, url, title)
                VALUES (?,?,?,?)""", numberedplaylist)
            self.conn.commit()
            return "success"
        else:
            return "This playlist name already exists! Nothing saved."

    def loadPlaylist(self, playlistid, userid):
        cursor = self.conn.cursor()
        cursor.execute("""SELECT rowid FROM playlists WHERE
            rowid = ? AND (public = 1 OR userid = ?) LIMIT 0,1""",
            (playlistid, userid));
        result = cursor.fetchone()
        if result:
            cursor.execute("""SELECT title, url FROM tracks WHERE
                playlistid = ? ORDER BY track ASC""", (playlistid,))
            alltracks = cursor.fetchall()
            apiplaylist = []
            for track in alltracks:
                #TODO ugly hack: playlistdb saves the "serve" dir as well...
                apiplaylist.append(MusicEntry(path=unquote(track[1])[7:], repr=unquote(track[0])))
            return apiplaylist

    def getName(self, plid, userid ):
        cur = self.conn.cursor()
        cur.execute("""SELECT rowid as id,title FROM playlists WHERE
            (public = 1 OR userid = ?) and rowid=?""", (userid,plid));
        result = cur.fetchall()
        if result:
            print(result)
            return result[0][1]
        return 'playlist'
        
    def setPublic(self, userid, plid, value):
        ispublic = 1 if value else 0
        cur = self.conn.cursor()
        cur.execute("""UPDATE playlists SET public = ? WHERE rowid = ? AND userid = ?""", (ispublic, plid, userid))

    def showPlaylists(self, userid):
        cur = self.conn.cursor()
        #change rowid to id to match api
        cur.execute("""SELECT rowid as id,title, userid, public FROM playlists WHERE
            public = 1 OR userid = ?""", (userid,));
        res = cur.fetchall()
        return list(map(lambda x: {'plid':x[0], 'title':x[1], 'userid':x[2],'public':bool(x[3]), 'owner':bool(userid==x[2])}, res))

    def createPLS(self,userid,plid, addrstr):
        pl = self.loadPlaylist(userid, plid)
        if pl:
            plsstr = '''[playlist]
    NumberOfEntries={}
    '''.format(len(pl))
            for i,track in enumerate(pl):
                trinfo = {  'idx':i+1,
                            'url':addrstr+'/serve/'+track.path,
                            'name':track.repr,
                            'length':-1,
                        }
                plsstr += '''
    File{idx}={url}
    Title{idx}={name}
    Length{idx}={length}
    '''.format(**trinfo)
            return plsstr

    def createM3U(self,userid,plid,addrstr):
        pl = self.loadPlaylist(userid, plid)
        if pl:
            trackpaths = map(lambda x: addrstr+'/serve/'+x.path,pl)
            return '\n'.join(trackpaths)


