#  This file is part of Headphones.
#
#  Headphones is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Headphones is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Headphones.  If not, see <http://www.gnu.org/licenses/>.

#####################################
## Stolen from Sick-Beard's db.py  ##
#####################################

from __future__ import with_statement

import os
import sqlite3
import threading
import time
import inspect

import headphones

from headphones import logger

def dbFilename(filename="headphones.db"):

    return os.path.join(headphones.DATA_DIR, filename)
    
def getCacheSize():
    #this will protect against typecasting problems produced by empty string and None settings
    if not headphones.CACHE_SIZEMB:
        #sqlite will work with this (very slowly)
        return 0
    return int(headphones.CACHE_SIZEMB)

class DBConnection:

    def __init__(self, filename="headphones.db"):
    
        self.filename = filename
        self.connection = sqlite3.connect(dbFilename(filename), timeout=20)
        #don't wait for the disk to finish writing
        self.connection.execute("PRAGMA synchronous = OFF")
        #journal disabled since we never do rollbacks
        self.connection.execute("PRAGMA journal_mode = %s" % headphones.JOURNAL_MODE)        
        #64mb of cache memory,probably need to make it user configurable
        self.connection.execute("PRAGMA cache_size=-%s" % (getCacheSize()*1024))
        self.connection.row_factory = sqlite3.Row
        
    def action(self, query, args=None):

        if query == None:
            return
            
        sqlResult = None
        attempt = 0
        
        if inspect.getframeinfo(inspect.currentframe().f_back)[2] not in ('select', 'upsert', 'render_body', 'getArtistjson'):
            loggedargs = ''
            if args != None:
                loggedargs = args
            logger.info("Undetermined query type was called from: %s" % inspect.getframeinfo(inspect.currentframe().f_back)[2].decode(headphones.SYS_ENCODING, 'replace'))
            logger.info("Database query was: %s with args: %s" % (query.decode(headphones.SYS_ENCODING, 'replace'), loggedargs))
        
        while attempt < 5:
            try:
                if args == None:
                    #logger.debug(self.filename+": "+query)
                    sqlResult = self.connection.execute(query)
                else:
                    #logger.debug(self.filename+": "+query+" with args "+str(args))
                    sqlResult = self.connection.execute(query, args)
                self.connection.commit()
                break
            except sqlite3.OperationalError, e:
                if "unable to open database file" in e.message or "database is locked" in e.message:
                    logger.warn('Database Error: %s', e)
                    attempt += 1
                    time.sleep(1)
                else:
                    logger.error('Database error: %s', e)
                    raise
            except sqlite3.DatabaseError, e:
                logger.error('Fatal Error executing %s :: %s', query, e)
                raise
        
        return sqlResult
    
    def select(self, query, args=None):
        
        if inspect.getframeinfo(inspect.currentframe().f_back)[2] not in ('render_body', 'getArtistjson'):
            logger.info("Select query was called from: %s" % inspect.getframeinfo(inspect.currentframe().f_back)[2].decode(headphones.SYS_ENCODING, 'replace'))
        sqlResults = self.action(query, args).fetchall()
        if inspect.getframeinfo(inspect.currentframe().f_back)[2] not in ('render_body', 'getArtistjson'):
            loggedargs = ''
            if args != None:
                loggedargs = args
            logger.info("Database select was: %s with args: %s" % (query.decode(headphones.SYS_ENCODING, 'replace'), loggedargs))
        
        if sqlResults == None:
            return []
            
        return sqlResults
                    
    def upsert(self, tableName, valueDict, keyDict):
    
        logger.info("upsert query was called from: %s" % inspect.getframeinfo(inspect.currentframe().f_back)[2].decode(headphones.SYS_ENCODING, 'replace'))
        changesBefore = self.connection.total_changes
        
        genParams = lambda myDict : [x + " = ?" for x in myDict.keys()]
        
        query = "UPDATE "+tableName+" SET " + ", ".join(genParams(valueDict)) + " WHERE " + " AND ".join(genParams(keyDict))
        
        self.action(query, valueDict.values() + keyDict.values())
        
        if self.connection.total_changes == changesBefore:
            query = "INSERT INTO "+tableName+" (" + ", ".join(valueDict.keys() + keyDict.keys()) + ")" + \
                        " VALUES (" + ", ".join(["?"] * len(valueDict.keys() + keyDict.keys())) + ")"
            logger.info("Database insert was: %s with values: %s" % (query.decode(headphones.SYS_ENCODING, 'replace'), valueDict.values().decode(headphones.SYS_ENCODING, 'replace'))             
            self.action(query, valueDict.values() + keyDict.values())
