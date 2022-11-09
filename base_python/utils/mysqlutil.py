#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2018-08-07
# @Author  : qinpengya


from datetime import datetime
import pymysql


def connect(logger, host, user, passwd, database):
    """
    Mysql connection.

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
        return pymysql.connect(host, user, passwd, database, charset='utf8')
    except Exception as e:
        logger.error(repr(e))
        return conn_retry(logger, None, 10, host, user, passwd, database)


def conn_retry(logger, conn, retry_count, *args):
    """
    Mysql reconnect

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
        logger.error('Mysql connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return pymysql.connect(list(args)[0], list(args)[1], list(args)[2], list(args)[3], charset='utf8')
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def save(logger, sql, data, host, user, passwd, database):
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
    conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Saving data to mysql...')
        begintime = datetime.now()
        rows = cur.executemany(sql, data)
        endtime = datetime.now()
        logger.info('   --- affect rows: %i' % rows)
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


def save_by_sql(logger, sql, host, user, passwd, database):
    """
    Save cleaned data to mysql.

    Parameters
    ----------
    logger : Logger
    sql : str
    host : str
    user : str
    passwd : str
    database : str

    Returns
    -------
    True or False : boolean
    """
    conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Saving data to mysql...')
        begintime = datetime.now()
        rows = cur.execute(sql)
        endtime = datetime.now()
        logger.info('   --- affect rows: %i' % rows)
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


def save(logger, sql, data, conn):
    """
        Save cleaned data to mysql.

        Parameters
        ----------
        logger : Logger
        sql : str
        data : [tuple] like [(1, 2), (a, b)]
        conn : database_connection

        Returns
        -------
        True or False : boolean
        """
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Saving data to mysql...')
        begintime = datetime.now()
        rows = cur.executemany(sql, data)
        endtime = datetime.now()
        logger.info('   --- affect rows: %i' % rows)
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
        # if conn is not None:
        #     conn.close()


def select(logger, host, user, passwd, database, sql):
    conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from mysql...')
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
        return None
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()



def insert(logger, conn, sql):
    """
    插入数据，不关连接 add by tianyafu @ 20200924
    :param logger:
    :param conn:
    :param sql:
    :return:
    """
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Saving data to mysql...')
        begintime = datetime.now()
        cur.execute(sql)
        conn.commit()
        endtime = datetime.now()
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        return True
    except Exception as e:
        logger.error(repr(e))
        conn.rollback()
        return False


def truncate(logger, sql, conn):
    """
    truncate table add by tianyafu @ 20200924

    Parameters
    ----------
    logger : Logger
    sql : str
    conn : pgdb connection
    Returns
    -------
    True or False : boolean
    """
    logger.info('truncate table: ' + str(sql))
    cur = None
    try:
        cur = conn.cursor()
        begintime = datetime.now()
        rows = cur.execute(sql)
        endtime = datetime.now()
        # logger.info('   --- affect rows: %i' % rows.rowcount)
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        conn.commit()
        return True
    except Exception as e:
        logger.error(repr(e))
        conn.rollback()
        raise Exception

def select_with_conn(logger, conn, sql):
    """
    传入conn执行sql，这个不关闭连接，别的地方可以复用连接 add by tianyafu @ 20200923
    :param logger:
    :param conn:
    :param sql:
    :return:
    """
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from mysql...')
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
        return None


def close(conn):
    """
    关闭连接 add by tianyafu @ 20200923
    :param conn: database connection
    :return:
    """
    if conn is not None:
        conn.close()



def select_with_port(logger, host, user, passwd, database, port, sql):
    """
    数据库连接指定了端口 add doc by tianyafu @ 20200925
    :param logger:
    :param host:
    :param user:
    :param passwd:
    :param database:
    :param port:
    :param sql:
    :return:
    """
    conn = connect_with_port(logger, host, user, passwd, database, port)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from mysql...')
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
        return None
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def connect_with_port(logger, host, user, passwd, database, port):
    """
    Mysql connection. add by tianyafu @ 20200925

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
        return pymysql.connect(host, user, passwd, database, port=port, charset='utf8')
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_port(logger, None, 10, host, user, passwd, database, port)


def conn_retry_with_port(logger, conn, retry_count, *args):
    """
    Mysql reconnect

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
        logger.error('Mysql connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return pymysql.connect(list(args)[0], list(args)[1], list(args)[2], list(args)[3], port=list(args)[4],
                                       charset='utf8')
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None
