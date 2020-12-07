"""
_sql_helper_
~~~~~~~~~~~~~~~~~~
General database access and sql manipulation methods
"""
# pylint: disable=C0103, C0201

import sqlite3 as _sq3
from sqlite3 import Error as _error
from sqlite3 import ProgrammingError as _pe


_con_ = None  # connection object
_cur_ = None  # cursor object


def _to_ascii_(s):
    """ Convert chars to ascii counterparts"""
    for i, j in zip(list('ğüşıöçĞÜŞİÖÇ'), list('gusiocGUSIOC')):
        s = s.replace(i, j)
    return s.lower()


def _get_cmp_(val):
    """ Get comparison values as tuple """
    ops = ('>=', '<=', '>', '<')
    if val.startswith(ops):
        for o in ops:
            if val.startswith(o):
                cmp = o
                val = val[len(cmp):]
                break
    else:
        cmp = '='
    return cmp, val


def build_where(var, val):  # pylint: disable=R0912
    """
    Build where part of the query
    :param var: Name of variable
    :type var: str
    :param val: Value of variable. Can be str, list or list of list os strings.
    :type val: str | list
    :return: A where statement for sql query
    :rtype: str
    """
    ret = ''
    if isinstance(val, str):
        if ',' in val:
            ret = build_where(var, val.split(','))
        else:
            cmp, val = _get_cmp_(val)
            val = _to_ascii_(val).lower()
            if not val.isnumeric():
                val = '\'' + val + '\''
            ret = cmp.join([var, val])
    elif isinstance(val, list):
        if all([isinstance(v, str) for v in val]):  # all is str
            if all([v.startswith(('>', '<')) for v in val]):
                ret = ' AND '.join([build_where(var, v) for v in val])
                ret = '(' + ret + ')'
            else:
                ret = var + ' IN (' + \
                      ','.join(['\'' + str(i) + '\'' for i in val]) + ')'
        elif all(isinstance(v, list) for v in val):  # all is list
            val = [['>=' + str(v[0]), '<=' + str(v[1])] for v in val]
            ret = '(' + ' OR '.join([build_where(var, v) for v in val]) + ')'

        if ret == '':
            if len(val) > 1:
                ret = var + ' IN (' + ','.join([str(i) for i in val]) + ')'
            else:
                ret = var + ' = ' + str(val[0])
    else:
        ret = var + ' = ' + str(val)
    return ret


def build_select(select, where, table):
    """ create a select statement for a table
    :param select: A dictionary of key:value of boolean or string of list or
                   a comma sepereated values as string
    :type select: dict | list | str
    :param where: A dictionary of key:value of where statements
    :type select: dict
    :param table: Name of table in database
    :type table: str
    :return: str of select query
    """
    if isinstance(select, dict):
        select = ','.join([k for k, v in select.items() if v])
    if isinstance(select, list):
        select = ','.join([str(i) for i in select])
    where = {k: v for k, v in where.items()
             if len(str(v)) > 0 and str(v) != '[]'}
    where = ' AND '.join([build_where(k, v) for k, v in where.items()
                          if v != ''])
    if where == '':
        return ''
    return 'SELECT ' + select + ' FROM ' + table + ' WHERE ' + where


def _is_open_(con):
    """ Check SQLite connection is open """
    if con is None:
        return False
    try:
        con.cursor()
        return True
    except _pe:
        return False
    except _error as e:
        print(e)
        return False


def create_con(file=':memory:', in_memory=False):
    """
    Create a database connection to a SQLite database
    :param file: Path to SQLite database
    :param in_memory: If True, database is loaded into memory for fast access
    :return: SQLite3 Connection Object
    """
    global _con_, _cur_  # pylint: disable=W0603
    if _is_open_(_con_):
        return _con_
    try:
        if _con_ is None:
            if in_memory:
                source_con = _sq3.connect(file,
                                          detect_types=_sq3.PARSE_DECLTYPES)
                _con_ = _sq3.connect(':memory:')  # pylint: disable=W0621
                source_con.backup(_con_)
                source_con.close()
            else:
                _con_ = _sq3.connect(file, detect_types=_sq3.PARSE_DECLTYPES)
    except _error as e:
        print(e)
    _cur_ = _con_.cursor()
    _cur_.arraysize = 10000
    return _con_


def close_con():
    """ Close database connection """
    global _con_, _cur_  # pylint: disable=W0603
    if _is_open_(_con_):
        _cur_.close()
        _con_.close()
        _cur_ = _con_ = None  # pylint: disable=W0621,W0612


def execute(sql):
    """ Execute SQL statement """
    return _cur_.execute(sql)
