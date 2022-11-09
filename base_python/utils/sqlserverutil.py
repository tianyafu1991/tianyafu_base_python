#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2019-05-11
# @Author  : tianyafu


import os
import sys
import pymssql
from datetime import datetime

mylib_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)) + '/utils'
sys.path.append(mylib_path)

from logutil import Logging

logger = Logging().get_logger()


def connect(logger, host, user, password, database, port):
    """
        sqlserver connection.

        Parameters
        ----------
        logger : Logger
        host: str
        user : str
        password : str
        database : str
        port : str

        Returns
        -------
        conn : Connection
        """
    try:
        return pymssql.connect(host=host, user=user, password=password, database=database, port=port, charset='utf8')
    except Exception as e:
        logger.error(repr(e))
        return conn_retry(logger, None, 10, host, user, password, database, port)


def conn_retry(logger, conn, retry_count, *args):
    """
        sqlserver reconnect

        Parameters
        ----------
        logger : Logger
        conn : Connection
        retry_count : int
        args : tuple --> host, user, password, database, port

        Returns
        -------
        conn : Connection
        """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('sqlserver connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return pymssql.connect(host=list(args)[0], user=list(args)[1], password=list(args)[2],
                                       database=list(args)[3], port=list(args)[4], charset='utf8')
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def save(logger, sql, data, host, user, password, database, port):
    """
    Save cleaned data to mysql.

    Parameters
    ----------
    logger : Logger
    sql : str
    data : [tuple] like [(1, 2), (a, b)]
    host : str
    user : str
    passwd : str
    database : str

    Returns
    -------
    True or False : boolean
    """
    conn = connect(logger, host, user, password, database, port)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Saving data to sqlserver...')
        begintime = datetime.now()
        cur.executemany(sql, data)
        endtime = datetime.now()
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        conn.commit()
        return True
    except Exception as e:
        logger.error(repr(e))
        conn.rollback()
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def select(logger, host, user, password, database, port, sql):
    conn = connect(logger, host, user, password, database, port)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from sqlserver...')
        begintime = datetime.now()
        cur.execute(sql)
        rows = cur.fetchall()
        endtime = datetime.now()
        logger.info('   --- result dataset size is %i' % len(rows))
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        return rows
    except Exception as e:
        logger.error(e)
        logger.error('Cannot connect database after retry 10 times, Please check database address is correct,'
                     '\n also check net is normal[ping *.*.*.*]')
        logger.error('[Failed] Analysis & Statistics flux stopped!')
        conn.rollback()
        return None
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
