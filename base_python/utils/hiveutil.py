#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2018-08-07
# @Author  : qinpengya


from datetime import datetime
from pyhive import hive


def connect(logger, host, user, passwd, database, port):
    """
    hive connection.

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
        return hive.Connection(database=database, host=host, username=user, password=passwd, port=port)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry(logger, None, 10, host, user, passwd, database, port)


def connect_with_nopasswd(logger, host, user, database, port):
    """
    hive connection.

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
        return hive.Connection(host=host, port=port, username=user, database=database)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_nopasswd(logger, None, 10, host, user, database, port)


def conn_retry_with_nopasswd(logger, conn, retry_count, *args):
    """
    hive reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('hive connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return hive.Connection(host=list(args)[0], username=list(args)[1], database=list(args)[2],
                                       port=list(args)[3])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def conn_retry(logger, conn, retry_count, *args):
    """
    hive reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('hive connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return hive.Connection(host=list(args)[0], username=list(args)[1], password=list(args)[2],
                                       database=list(args)[3], port=list(args)[4])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def connect_kerberos(logger, host, user, passwd, database, port, auth, kerberos_service_name, configuration):
    """
    hive connection.

    Parameters
    ----------
    logger : Logger
    host: str
    user : str
    passwd : str
    database : str
    port : hive server port
    auth :
    kerberos_service_name :
    configuration :
    Returns
    -------
    conn : Connection
    """
    try:
        return hive.Connection(database=database, host=host, username=user, port=port, auth=auth,
                               kerberos_service_name=kerberos_service_name, configuration=configuration)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_kerberos(logger, None, 10, host, user, passwd, database, port, auth, kerberos_service_name,
                                   configuration)


def conn_retry_kerberos(logger, conn, retry_count, *args):
    """
    hive reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('hive connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return hive.Connection(host=list(args)[0], username=list(args)[1],
                                       database=list(args)[3], port=list(args)[4], auth=list(args)[5],
                                       kerberos_service_name=list(args)[6], configuration=list(args)[7])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def select_with_conn(logger, conn, sql):
    """
    查询Hive数据，没有kerberos权限,不主动关闭连接，需要用户自己维护 add by tianyafu@20201103
    :param logger:
    :param conn: Hive conn
    :param sql:
    :return:
    """
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from hive...')
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


def select_with_no_kerberos(logger, host, user, passwd, database, port, sql):
    """
    查询Hive数据，没有kerberos权限,只查询一次，不复用连接 add by tianyafu@20201103
    :param logger:
    :param host:
    :param user:
    :param passwd:
    :param database:
    :param port:
    :param sql:
    :return:
    """
    conn = connect(logger, host, user, passwd, database, port)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from hive...')
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
        if conn is not None:
            conn.close()


def select(logger, host, user, passwd, database, port, auth, kerberos_service_name, configuration, sql):
    conn = connect_kerberos(logger, host, user, passwd, database, port, auth, kerberos_service_name, configuration)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from hive...')
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
        if conn is not None:
            conn.close()


def connect_with_auth(logger, host, user, passwd, database, port, auth):
    """
    hive connection.

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
        return hive.Connection(database=database, host=host, username=user, password=passwd, port=port, auth=auth)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_auth(logger, None, 10, host, user, passwd, database, port, auth)


def conn_retry_with_auth(logger, conn, retry_count, *args):
    """
    hive reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('hive connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return hive.Connection(host=list(args)[0], username=list(args)[1], password=list(args)[2],
                                       database=list(args)[3], port=list(args)[4], auth=list(args)[5])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def connect_nosasl(logger, host, database, port, auth):
    """
    hive connection.

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
        return hive.Connection(database=database, host=host, port=port, auth=auth)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_nosasl(logger, None, 10, host, database, port, auth)


def conn_retry_nosasl(logger, conn, retry_count, *args):
    """
    hive reconnect

    Parameters
    ----------
    logger : Logger
    conn : Connection
    retry_count : int
    args : tuple --> host, user, passwd, database

    Returns
    -------
    conn : Connection
    """
    try:
        conn.ping(True)
        return conn
    except Exception as e:
        logger.error(e)
        logger.error('hive connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return hive.Connection(host=list(args)[0],
                                       database=list(args)[1], port=list(args)[2], auth=list(args)[3])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None
