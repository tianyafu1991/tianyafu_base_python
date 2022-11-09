import psycopg2
from datetime import datetime
from io import StringIO

"""
gssencmode参见:https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
"""


def connect(logger, host, user, passwd, database):
    """
    greenplum connection.

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
        return psycopg2.connect(database=database, host=host, user=user, password=passwd)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry(logger, None, 10, host, user, passwd, database)


def conn_retry(logger, conn, retry_count, *args):
    """
    greenplum reconnect

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
        logger.error('greenplum connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return psycopg2.connect(list(args)[0], list(args)[1], list(args)[2], list(args)[3])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def save(logger, sql, data, host, user, passwd, database):
    """
    Save cleaned data to greenplum.

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
    logger.info('Saving data to greenplum...')
    if len(data) == 0:
        logger.info('Data cannot be empty...')
        return False
    conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        begintime = datetime.now()
        rows = cur.executemany(sql, data)
        # rows = cur.execute(sql)
        endtime = datetime.now()
        # logger.info('   --- affect rows: %i' % rows.rowcount)
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


def select(logger, host, user, passwd, database, sql):
    conn = connect(logger, host, user, passwd, database)
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from greenplum...')
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


def insert(logger, sql, data, conn):
    """
    Save cleaned data to greenplum.

    Parameters
    ----------
    logger : Logger
    sql : str
    data : [tuple] like [(1, 2), (a, b)]
    conn : pgdb connection
    Returns
    -------
    True or False : boolean
    """
    logger.info('Saving data to greenplum...')
    if len(data) == 0:
        logger.info('Data cannot be empty...')
        return
    if conn is None:
        logger.error('insert function connection object is None.')
        raise Exception
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
    finally:
        if cur is not None:
            cur.close()


def close(conn):
    if conn is not None:
        conn.close()


def truncate(logger, sql, conn):
    """
    truncate table

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
    finally:
        if cur is not None:
            cur.close()


def connect_with_port(logger, host, user, passwd, database, port):
    """
    greenplum connection.

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
        return psycopg2.connect(database=database, host=host, user=user, password=passwd, port=port)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_port(logger, None, 10, host, user, passwd, database, port)


def conn_retry_with_port(logger, conn, retry_count, *args):
    """
    greenplum reconnect

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
        logger.error('greenplum connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return psycopg2.connect(list(args)[0], list(args)[1], list(args)[2], list(args)[3], port=list(args)[4])
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


def update_with_conn(logger, sql, conn):
    """
    execute update sql

    Parameters
    ----------
    logger : Logger
    sql : str
    conn : pgdb connection
    Returns
    -------
    True or False : boolean
    """
    logger.info('execute sql ...')
    if conn is None:
        logger.error('insert function connection object is None.')
        raise Exception
    cur = None
    try:
        cur = conn.cursor()
        begintime = datetime.now()
        cur.execute(sql)
        endtime = datetime.now()
        # logger.info('   --- affect rows: %i' % rows.rowcount)
        logger.info('   --- cost: %is' % (endtime - begintime).seconds)
        conn.commit()
        return True
    except Exception as e:
        logger.error(repr(e))
        conn.rollback()
        raise Exception
    finally:
        if cur is not None:
            cur.close()


def select_with_conn(logger, sql, conn):
    cur = None
    try:
        cur = conn.cursor()
        logger.info('Selecting data from greenplum...')
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


def connect_with_port_and_disable_gss(logger, host, user, passwd, database, port):
    """
    greenplum connection.

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
        return psycopg2.connect(database=database, host=host, user=user, password=passwd, port=port,
                                gssencmode='disable')
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_port_and_disable_gss(logger, None, 10, host, user, passwd, database, port)


def conn_retry_with_port_and_disable_gss(logger, conn, retry_count, *args):
    """
    greenplum reconnect

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
        logger.error('greenplum connection is closed, Now retry connect...')
        retry = 0
        while retry < retry_count:
            try:
                logger.debug('Retry times is %i' % (retry + 1))
                return psycopg2.connect(list(args)[0], list(args)[1], list(args)[2], list(args)[3], port=list(args)[4],
                                        gssencmode='disable')
            except Exception as e:
                logger.error(repr(e))
                retry += 1
        else:
            return None


# 将pandas的DataFrame通过copy from 的形式写入到GP中 add by tianyafu@20211016
def df_2_gp_use_copy_from(logger, df, conn, table_name, columns, sep):
    logger.info('使用copy from 将DataFrame按照字段 %s 插入到表 %s 中' % (columns, table_name))
    if conn is None:
        logger.error('insert function connection object is None.')
        raise Exception
    cur = None
    try:
        begintime = datetime.now()
        output = StringIO()
        df.to_csv(output, sep=sep, index=False, header=False)
        output1 = output.getvalue()
        cur = conn.cursor()
        # 使用copy from 的形式写入到GP中
        cur.copy_from(StringIO(output1), table_name, sep=sep, columns=columns)
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


def connect_with_port_and_set_gss(logger, host, user, passwd, database, port, gssencmode='disable'):
    """
    greenplum connection.

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
        return psycopg2.connect(database=database, host=host, user=user, password=passwd, port=port,
                                gssencmode=gssencmode)
    except Exception as e:
        logger.error(repr(e))
        return conn_retry_with_port_and_disable_gss(logger, None, 10, host, user, passwd, database, port)


def maintain_conn(logger, gp_conn, gp_host, gp_user, gp_passwd, gp_database, gp_port):
    """
    维护GP数据库连接 因脚本全量处理时 时间较长 中间可能跨多天  导致脚本还在运行  数据库连接已失效 所以要引入该方法
    :param gp_conn:
    :return:
    """
    if gp_conn is not None and gp_conn.closed == 0:
        # conn的closed属性 0 if the connection is open, nonzero if it is closed or broken.
        logger.info("数据库连接有效.............")
        pass
    else:
        # 如果GP数据库的连接已关闭 或 已失效  则重新获取连接
        gp_conn = connect_with_port(logger, gp_host, gp_user, gp_passwd, gp_database,
                                    int(gp_port))
        logger.warn("创建新的数据库连接.............")
    return gp_conn
