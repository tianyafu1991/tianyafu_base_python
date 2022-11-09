#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2018-08-07
# @Author  : qinpengya


from datetime import datetime
import pymysql


def connect(logger, host, user, passwd,database,port):
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
        return pymysql.connect(host, user, passwd, database,port, charset='utf8')
    except Exception as e:
        logger.error(repr(e))
        return conn_retry(logger, None, 10, host, user, passwd, port,database)


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


def save(logger, sql, data, host, user, passwd, database,port):
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
    conn = connect(logger, host, user, passwd, database,port)
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


def select(logger, host, user, passwd, database,port, sql):
    conn = connect(logger, host, user, passwd, database,port)
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


def get_meta(logger, conn, table):
    sql = 'SELECT b.PARAM_KEY, b.PARAM_VALUE FROM TBLS AS a JOIN TABLE_PARAMS AS b ' \
          'WHERE a.TBL_ID = b.TBL_ID AND TBL_NAME=%s' % table
    # conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from mysql...')
        begintime = datetime.now()
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        size, record = 0, 0
        for row in rows:
            if row[0] == 'rawDataSize':
                size = row[1]
                if row[1] == '-1':
                    size = 0
            if row[0] == 'numRows':
                record = row[1]
                if row[1] == '-1':
                    record = 0
        endtime = datetime.now()
        logger.info('   --- result dataset size is %i' % len(rows))
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        return record, size
    except Exception as e:
        logger.error(e)
        logger.error('Cannot connect database after retry 10 times, Please check database address is correct,'
                     '\n also check net is normal[ping *.*.*.*]')
        logger.error('[Failed] Analysis & Statistics flux stopped!')
        # conn.rollback()
        raise Exception
    # finally:
    #     if cur is not None:
    #         cur.close()
    #     if conn is not None:
    #         conn.close()


def query_data(logger, conn, table_id, data_time):
    # sql = "select total_records, total_size from data_grow_info where table_id=%s and create_time='%s'" % (table_id, data_time)
    sql = "select total_records, total_size from data_grow_info where table_id=%s and create_time='%s'" % (table_id, data_time)
    # conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from mysql...')
        begintime = datetime.now()
        cur.execute(sql)
        conn.commit()
        rows = cur.fetchall()
        if rows is None or len(rows) < 1:
            return 0, 0
        size, record = 0, 0
        size = 0 if rows[0][1] is None or rows[0][1] == '' else rows[0][1]
        record = 0 if rows[0][0] is None or rows[0][0] == '' else rows[0][0]
        endtime = datetime.now()
        logger.info('   --- result dataset size is %i' % len(rows))
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        return record, size
    except Exception as e:
        logger.error(e)
        logger.error('Cannot connect database after retry 10 times, Please check database address is correct,'
                     '\n also check net is normal[ping *.*.*.*]')
        logger.error('[Failed] Analysis & Statistics flux stopped!')
        # conn.rollback()
        raise Exception
    # finally:
    #     if cur is not None:
    #         cur.close()
    #     if conn is not None:
    #         conn.close()


def close(conn):
    if conn is not None:
        conn.close()


