"""
airpy main
~~~~~~~~~~~~~~~~~~
General database access methods
"""
# pylint: disable=C0103, C0201


import os as _os
import sys as _sys
from time import time
import pandas as _pd
from . import _airpy_db_helper_ as _adh

_adh.connect()


def airpy(*args, **kwargs):
    """ General database access method """
    # args = ()
    # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan'}
    if _check_db_():
        return None
    return_as = 'gen'  # default
    opt_return = ['gen', 'list', 'long_list', 'df']  # possible args
    if 'return_as' in kwargs.keys():
        return_as = kwargs.pop('return_as')
    if return_as not in opt_return:
        raise TypeError("return_as must be one of " + str(opt_return))

    include_nan = True
    if 'include_nan' in kwargs.keys():
        include_nan = kwargs.pop('include_nan')

    as_list = 'list' in return_as

    if 'in_memory' in kwargs.keys():
        in_memory = kwargs.pop('in_memory')

    verbose = False
    if 'verbose' in kwargs.keys():
        verbose = kwargs.pop('verbose')

    t1 = time()
    data, colnames, query = _adh.query_data(args, kwargs,
                                            as_list=as_list,
                                            include_nan=include_nan)
    if verbose:
        print(query)
    ret = data
    if return_as == 'long_list':
        ret = list(map(list, zip(*data)))
    elif return_as == 'df':
        ret = _pd.DataFrame(data, columns=colnames)

    # _adh.close()
    t2 = time()
    elapsed = t2 - t1
    if verbose:
        print('Query completed in %f seconds.' % elapsed)
    return ret


def _generator_(cur):
    for i in cur:
        yield list(i)


def _return_(cur, return_as, columns):
    ret = _generator_(cur)
    if return_as == 'list':
        ret = list(ret)
    elif return_as == 'long_list':
        ret = list(map(list, zip(*ret)))
    elif return_as == 'df':
        ret = _pd.DataFrame(ret, columns=columns)
    return ret


def reg(name=None, return_as='df'):
    """
    Get region names
    :param name: Name of region
    :type name: str
    :param return_as: One of 'gen', list', 'long_list', ['df']
    :type return_as: str
    """
    sel = 'id,name,lat,lon'
    sql = 'SELECT {} FROM reg'.format(sel)
    if name is not None:
        sql += ' WHERE name LIKE \'{}\''.format(name.lower())
    cur = _adh.execute(sql)
    return _return_(cur, return_as, ['id', 'name', 'lat', 'lon'])


def pol(name=None, return_as='df'):
    """
    Get pollutant names
    :param name: Name of pollutant
    :type name: str
    :param return_as: One of 'gen', list', 'long_list', ['df']
    :type return_as: str
    """
    sel = 'name,long_name,short_name,unit'
    sql = 'SELECT {} FROM pol'.format(sel)
    if name is not None:
        sql += ' WHERE name LIKE \'{}\''.format(name.lower())
    cur = _adh.execute(sql)
    return _return_(cur, return_as, ['name', 'long', 'short', 'unit'])


def city(name=None, return_as='df'):
    """
    Get city names
    :param name: Name of city
    :type name: str
    :param return_as: One of 'gen', list', 'long_list', ['df']
    :type return_as: str
    """
    sel = 'id,nametr,lat,lon'
    sql = 'SELECT {} FROM city'.format(sel)
    if name is not None:
        sql += ' WHERE name LIKE \'{}\''.format(name.lower())
    cur = _adh.execute(sql)
    return _return_(cur, return_as, ['', 'name', 'lat', 'lon'])


def station(name=None, return_as='df'):
    """
    Get station names
    :param name: Name of station
    :type name: str
    :param return_as: One of 'gen', list', 'long_list', ['df']
    :type return_as: str
    """
    sel = 'id,nametr,cat,lat,lon'
    sql = 'SELECT {} FROM sta'.format(sel)
    if name is not None:
        sql += ' WHERE name LIKE \'{}\''.format(name.lower())
    cur = _adh.execute(sql)
    return _return_(cur, return_as, ['', 'name', 'cat', 'lat', 'lon'])


def _check_db_():
    cur = _exec_('SELECT COUNT(*) FROM data')
    ret = cur.fetchall()[0][0] == 0
    if ret:
        script_path = _os.path.join(_sys.prefix, 'bin/airpy-create-db')
        print("Run '{}' command to use this function".format(script_path))
    return ret


def _exec_(sql):
    con = _adh.connect()
    cur = con.cursor()
    cur.execute(sql)
    return cur


def _fetchall_(sql):
    cur = _exec_(sql)
    rows = cur.fetchall()
    print('nrows:', len(rows))


def _fetchmany_(sql):
    cur = _exec_(sql)
    nrows = 0
    while True:
        rows = cur.fetchmany()
        if not rows:
            break
        nrows += len(rows)
    print('nrows:', nrows)


def _for_loop_(sql):
    cur = _exec_(sql)
    nrows = 0
    for _ in cur:
        nrows += 1
    print('nrows:', nrows)


def _eqp_(sql):
    """ Explain Query Plan """
    cur = _exec_('EXPLAIN QUERY PLAN ' + sql)
    for i in cur:
        print(i)


def _exp_(sql):
    """ Explain SQL """
    cur = _exec_('EXPLAIN ' + sql)
    for i in cur:
        print(i)
