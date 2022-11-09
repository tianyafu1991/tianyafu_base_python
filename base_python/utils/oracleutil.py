#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2021-03-20
# @Author  : tianyafu

import cx_Oracle
from datetime import datetime



def connect_with_port(logger, host, user, passwd, database, port, service):
    """
    oracle connection.
    cx_Oracle.connect(user=None,
     password=None,
     dsn=None,
      mode=cx_Oracle.DEFAULT_AUTH,
      handle=0,
      pool=None,
      threaded=False,
      events=False,
      cclass=None,
      purity=cx_Oracle.ATTR_PURITY_DEFAULT,
      newpassword=None, encoding=None,
      nencoding=None,
      edition=None,
      appcontext=[],
      tag=None,
      matchanytag=None,
      shardingkey=[],
      supershardingkey=[])
    Parameters
    ----------
    logger : Logger
    host: str
    user : str
    passwd : str
    database : str

    Returns
    -------
    conn : Connection
    """
    try:
        url = user + "/" + passwd + "@" + host + ":" + port + "/" + service
        return cx_Oracle.connect(url)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_port(logger, None, 10, host, user, passwd, database, port, service)


def conn_retry_with_port(logger, conn, retry_count, *args):
    """
    oracle reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database,port,service

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('oracle connection is closed, Now retry connect...')
        retry = 0
        url = list(args)[1] + "/" + list(args)[2] + "@" + list(args)[0] + ":" + list(args)[4] + "/" + list(args)[5]
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return cx_Oracle.connect(url)
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def close(conn):
    if conn is not None:
        conn.close()


def select_with_conn(logger, sql, conn):
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from oracle...')
        begintime = datetime.now()
        cur.execute(sql)
        conn.commit()
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
        raise Exception
    finally:
        if cur is not None:
            cur.close()
