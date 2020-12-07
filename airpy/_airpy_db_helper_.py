"""
_sql_helper_
~~~~~~~~~~~~~~~~~~
General database access and sql manipulation methods
"""
# pylint: disable=C0103, C0201

import os as _os
# import sys as _sys
from . import _sql_helper_ as _sh

_keys_date_ = ('date', 'year', 'month', 'day', 'hour', 'week', 'doy', 'hoy')
_keys_ = ('pol', 'reg', 'city', 'sta') + _keys_date_ + ('value',)


def _get_db_path_(data_dir='data'):
    """ Get path to database folder """
    try:
        pth = __file__
        rpth = _os.path.realpath(pth)
        db_path = _os.path.dirname(rpth)
        # db_path = _os.path.split(db_path)[0]
        # if not _os.path.exists(_os.path.join(db_path, data_dir)):
        #     db_path = _sys.prefix
    except NameError:
        db_path = _os.getcwd()
    return _os.path.join(db_path, data_dir)


def _get_db_file_():
    """ Get path to database file """
    db_path = _get_db_path_()
    return _os.path.join(db_path, 'airpy.db')


def _get_ids_(where, table):
    x = _sh.execute(_sh.build_select('id', where, table)).fetchall()
    if len(x) > 0:
        x = [i[0] for i in x]
    return x


def _end_points_(date_ids):
    """ return end points of consecutive integers """
    diff_date_ids = [date_ids[i] - date_ids[i - 1] for i in
                     range(1, len(date_ids))]
    endpi = [i for i, v in enumerate(diff_date_ids) if v != 1]
    endpi = [-1] + endpi + [len(date_ids) - 1]
    endpi = [[endpi[i - 1] + 1, endpi[i]] for i in range(1, len(endpi))]
    endp = [[date_ids[i[0]], date_ids[i[1]]] for i in endpi]
    return endp


def _get_ids_for_tables(opt_queries):
    """
    Get query results from side tables
    :param opt_queries: A dict of query params
    :return: A dict of query results
    :rtype: dict
    """
    pol_ids = _get_ids_({'name': opt_queries['pol']}, 'pol')
    reg_ids = _get_ids_({'name': opt_queries['reg']}, 'reg')
    city_ids = _get_ids_({'name': opt_queries['city'], 'reg': reg_ids}, 'city')
    sta_ids = _get_ids_({'name': opt_queries['sta'], 'city': city_ids}, 'sta')
    date_ids = _get_ids_({k: opt_queries[k] for k in _keys_date_}, 'cal')
    if len(date_ids) == 0:
        # date_ids = '>0'
        date_ids = []
    else:
        if len(date_ids) > 1:
            ret = _sh.execute('SELECT MAX(date) FROM data')
            max_date = ret.fetchone()[0]
            date_ids = [i for i in date_ids if i <= max_date]
            date_ids = _end_points_(date_ids)

    # create query for dat table
    where = {'pol': pol_ids, 'sta': sta_ids, 'date': date_ids}
    return where


def _get_cal_table_(opt_queries):
    where = {k: opt_queries[k] for k in _keys_date_}
    sql = _sh.build_select('*', where, 'cal')
    if len(sql) == 0:
        sql = 'SELECT * FROM cal'
    x = _sh.execute(sql).fetchall()
    # return list(map(list, zip(*x)))
    return x


def _build_main_select_str_(sel):
    """ build select statement for the db query """
    opt_select = dict(zip(_keys_, [True] * 5 + [False] * 7 + [True]))
    if isinstance(sel, str):
        sel = sel.split(',')
    if isinstance(sel, list):
        if any([s in opt_select.keys() for s in sel]):
            opt_select = {k: False for k in opt_select.keys()}
            for s in sel:
                if s in opt_select.keys():
                    opt_select[s] = True
    sel = ','.join([list(opt_select.keys())[i] for i, x in
                    enumerate(list(opt_select.values())) if x])
    # if 'value' in sel:
    #     sel = sel.replace('value', 'cast(value AS float)')
    return sel


def _get_opt_queries_(args, kwargs):
    """ Get option queries from args """
    opt_queries = dict.fromkeys(_keys_, '')
    for a, k in zip(args, opt_queries.keys()):
        opt_queries[k] = a
    # get options from kwargs
    for k in kwargs.keys():
        if k in opt_queries.keys():
            opt_queries[k] = kwargs[k]
    return opt_queries


def build_query(args, kwargs):
    """ build query """
    # args = ()
    # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
    #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}

    select = kwargs.pop('select') if 'select' in kwargs.keys() else ''
    select = _build_main_select_str_(select)
    opt_queries = _get_opt_queries_(args, kwargs)

    where_ids = _get_ids_for_tables(opt_queries)
    select_data = _sh.build_select('*', where_ids, 'data')

    sql = """
        SELECT
            {select}
        FROM
        (SELECT
            pol.name AS pol,
            reg.name AS reg,
            city.name AS city_ascii,
            city.nametr AS city,
            sta.name AS sta_ascii,
            sta.nametr AS sta,
            cal.date AS date,
            cal.year AS year,
            cal.month AS month,
            cal.day AS day,
            cal.hour AS hour,
            cal.week AS week,
            cal.doy AS doy,
            cal.hoy AS hoy,
            cast(data.value AS float) AS value
        FROM
            ({data}) data
        INNER JOIN pol ON pol.id = data.pol
        INNER JOIN reg ON reg.id = city.reg
        INNER JOIN city ON city.id = sta.city
        INNER JOIN sta ON sta.id = data.sta
        INNER JOIN cal ON cal.id = data.date);"""
    return (sql.format(select=select, data=select_data), select.split(','),
            opt_queries)


def _replace_list_(r, cal_row, sel):
    for s in sel:
        if s in _keys_date_:
            i = sel.index(s)
            j = _keys_date_.index(s) + 1
            r[i] = cal_row[j]
    if 'value' in sel:
        r[sel.index('value')] = float('NaN')
    return r


def _create_nan_(cur_date_index, last_row, sel, cal):
    cur_date_index += 1
    while cur_date_index < len(cal):
        cal_row = cal[cur_date_index]
        yield _replace_list_(list(last_row), cal_row, sel)
        cur_date_index += 1
    cur_date_index = -1
    return cur_date_index


def _get_sel_indices_(sel):
    _sel_keys_ = ['pol', 'sta', 'date', 'value']
    index = dict(zip(_sel_keys_,
                     [sel.index(i) if i in sel else -1 for i in _sel_keys_]))
    for k, v in index.items():
        if v < 0:
            raise Exception(k + ' cannot be found')
    return index


def data_generator(query, sel, opt_queries, include_nan=True):
    """ Query result generator """
    cal = _get_cal_table_(opt_queries)
    index = _get_sel_indices_(sel)
    cur = execute(query)

    prev_pol = ''
    prev_sta = ''
    last_row = None
    cur_date_index = -1
    while True:
        rows = cur.fetchmany()
        if not rows:
            break
        for r in rows:
            if include_nan:
                cur_pol = r[index['pol']]  # current pollutant
                cur_sta = r[index['sta']]  # current station

                if prev_pol != cur_pol and cur_date_index > -1:
                    for i in _create_nan_(cur_date_index, last_row, sel, cal):
                        yield i
                    cur_date_index = -1

                if prev_sta != cur_sta and cur_date_index > -1:
                    for i in _create_nan_(cur_date_index, last_row, sel, cal):
                        yield i
                    cur_date_index = -1

                prev_pol = cur_pol
                prev_sta = cur_sta
                cur_date_index += 1  # current date index
                cur_date = cal[cur_date_index][1]
                row_date = r[index['date']]

                while cur_date < row_date:
                    yield _replace_list_(list(r), cal[cur_date_index], sel)
                    cur_date_index += 1
                    cur_date = cal[cur_date_index][1]
            last_row = list(r)
            yield last_row
    for i in _create_nan_(cur_date_index, last_row, sel, cal):
        yield i


def query_data(args, kwargs, as_list=False, include_nan=True):
    """ Query database """
    # args = ()
    # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
    #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}
    query, sel, opt_queries = build_query(args, kwargs)
    ret = data_generator(query, sel, opt_queries, include_nan)
    if as_list:
        ret = list(ret)
    return ret, sel, query


def connect(file=_get_db_file_(), in_memory=False):
    """
    Connect to database
    :param file: Path to file
    :type file: str
    :param in_memory: Load database to memory
    :type in_memory: bool
    :return: Sqlite3 Connection Object
    """
    return _sh.create_con(file, in_memory)


def close():
    """ Close database connection """
    _sh.close_con()


def execute(sql):
    """ Execute sql statement """
    return _sh.execute(sql)
