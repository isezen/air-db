"""
airdb package.

~~~~~~~~~~~~~~~~~~~~~
A data access layer (DAL) to easily query environmental time series datasets
obtained from various sources.

version : 0.1.8
github  : https://github.com/isezen/airdb
author  : Ismail SEZEN
email   : sezenismail@gmail.com
license : AGPLv3
date    : 2021
"""

# pylint: disable=C0103, C0201
from contextlib import closing as _closing
from time import time as _time
# from warnings import warn as _warn
# import itertools as _itertools
# from collections.abc import Iterable as _Iterable

import os as _os
from os import path as _path
import sqlite3 as _sq
import pandas as _pd
import xarray as _xr
import numpy as _np

from .config import Options as _Options
from . import utils as _utils
from .utils import Build as _build
from .__errors__ import DatabaseVersionError as _DatabaseVersionError

__version__ = '0.2.0'
__author__ = 'Ismail SEZEN'
__email__ = 'sezenismail@gmail.com'
__license__ = 'AGPLv3'
__year__ = '2021'


options = _Options()


class DatabaseQueryArguments:
    """Database Query Arguments."""

    _args_int = ['year', 'month', 'day', 'hour',
                 'week', 'doy', 'hoy']
    _names = ['param', 'city', 'sta', 'reg', 'date', 'select'] + _args_int
    _types = [(str, list)] * 6 + [(str, int, list)] * 7
    _values = [''] * (6 + 7)

    def __init__(self, *args, **kwargs):
        """
        Initialize DatabaseQueryArguments object.

        Args:
            param (str, list)      : parameter name
            city  (str, list)      : City Name
            sta   (str, list)      : Station Name
            reg   (str, list)      : Region Name

            date  (str, list)      : Date
            year  (int, str, list) : Year
            month (int, str, list) : Month
            day   (int, str, list) : Day
            hour  (int, str, list) : Hour
            week  (int, str, list) : Week of year
            doy   (int, str, list) : Day of year
            hoy   (int, str, list) : Hour ofyear
        """
        names = self.__class__._names
        types = self.__class__._types
        values = self.__class__._values

        default_values = dict(zip(names, values))
        default_types = dict(zip(names, types))
        # Merge args and kwargs and set default values
        args = _utils.get_args(args, kwargs, default_values)
        dqa = [args.pop(k) for k, v in args.copy().items()
               if isinstance(v, DatabaseQueryArguments)]
        rest = {k: v for k, v in args.items() if k not in names}

        args = {k: v for k, v in args.items() if k in names}
        # Check argument types
        _utils.check_arg_types(args, default_types)
        _utils.int2str(args, self.__class__._args_int)
        args = {k: _utils.split_str(v) for k, v in args.items()}

        for k, v in args.items():
            self.__dict__[k] = v
        self.__dict__['_args'] = args
        self._rest = rest
        self._args = args

        if len(dqa) > 0:
            for i in dqa:
                self.update(i)

    def __repr__(self):
        """Represent class object as a string."""
        s = ''
        for k, v in self._args.items():
            if v is not None and v != '':
                s += f' {k: <6}: {v}\n'
        return 'empty' if s == '' else 'Arguments:\n' + s

    def __setattr__(self, name, value):
        """Set class attributes."""
        if name in self._args.keys():
            raise Exception("Read only property")
        super().__setattr__(name, value)

    def __iter__(self):
        """Iterate over arguments."""
        for k, v in self._args.items():
            if v is not None and v != '':
                yield (k, v)

    def update(self, x):
        """
        Merge a DatabaseQueryArguments object to self.

        Args:
            x (DatabaseQueryArguments): A DatabaseQueryArguments object
        """
        if not isinstance(x, DatabaseQueryArguments):
            raise ValueError('x must be a DatabaseQueryArguments object')
        merge_dict = DatabaseQueryArguments._merge_dict
        args = merge_dict(self._args, x._args)  # pylint: disable=W0212
        rest = merge_dict(self._rest, x._rest)  # pylint: disable=W0212
        args.update(rest)
        new = self.__class__(**args)
        for k, v in new._args.items():  # pylint: disable=W0212
            self.__dict__[k] = v
        self.__dict__['_args'] = new._args  # pylint: disable=W0212
        self._rest = new._rest  # pylint: disable=W0212

    @staticmethod
    def _merge_dict(d1, d2):
        result = dict(d1)
        merge = DatabaseQueryArguments._merge
        for k, v in d2.items():
            result[k] = merge(result[k], v) if k in result else v
        return result

    @staticmethod
    def _merge(x, y):
        if not isinstance(x, list):
            x = [x]
        if not isinstance(y, list):
            y = [y]
        m = list(set(x + y))
        if len(m) > 1:
            m = [i for i in m if i != '']
        if len(m) == 1:
            return m[0]
        return m

    @property
    def rest(self):
        """Unprocessed args."""
        return self._rest

    @property
    def names(self):
        """Database argument names."""
        return list(self._args.keys())

    def keys(self):
        """Database argument names."""
        return self._args.keys()

    def values(self):
        """Database argument values."""
        return self._args.values()

    def to_list(self, all_args=False):
        """Get list of vargument values."""
        if all_args:
            return list(self._args.values())
        return [v for v in self._args.values()
                if v is not None and v != '']

    def to_dict(self, all_args=False):
        """Get dict of vargument values."""
        if all_args:
            return dict(self._args.items())
        return {k: v for k, v in self._args.items()
                if v is not None and v != ''}


class Database:
    """This class is used to connect to a specific airdb database."""

    _keys_date = ('date', 'year', 'month', 'day', 'hour', 'week', 'doy', 'hoy')
    _keys = ('param', 'reg', 'city', 'sta', 'lat', 'lon') + _keys_date + \
            ('value',)
    # %%--------

    def __init__(self, name, return_type='gen'):
        """
        Create a Database object.

        Args:
            name        (str): Database name without extension
            return_type (str): One of gen, list, long_list, [df], xarray
        """
        return_types = ['gen', 'list', 'long_list', 'df', 'xarray']
        self._name = name
        self._path = _path.join(options.db_path, name + '.db')
        if not _path.exists(self._path):
            raise FileNotFoundError('Database ' + name + ' cannot be found')

        if return_type not in return_types:
            raise TypeError("return_type must be one of " + str(return_types))
        self._return_type = return_type
        self._con = _sq.connect(self._path, detect_types=_sq.PARSE_DECLTYPES)
        self._cur = self._con.cursor()
        target_version = 0.3
        if self.version < target_version:
            raise _DatabaseVersionError(self.version, target_version)
        # self._set_table_method('id,name,lat,lon', 'reg', 'region')

    def __enter__(self):
        """Return self in with statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection when exit from with."""
        self._con.close()

    def __del__(self):
        """If Database deleted, close connection."""
        self._con.close()

    # %%--------

    @property
    def path(self):
        """Path to database file."""
        return self._path

    @property
    def name(self):
        """Name of database."""
        return self._name

    @property
    def version(self):
        """Database version."""
        sql = 'SELECT value FROM version'
        self._cur = self._con.cursor().execute(sql)
        v = self._return('list', 'value')[0][0]
        v = "".join(i for i in v if i in "0123456789.")
        return float(v)

    @property
    def is_open(self):
        """Check if connection to the database is open."""
        try:
            self._con.cursor()
            return True
        except Exception:  # pylint: disable=W0703
            return False

    def _return(self, return_type, columns):

        def generator():
            for i in self._cur:
                yield list(i)
            self._cur.close()

        ret = generator()
        if return_type == 'list':
            ret = list(ret)
        elif return_type == 'long_list':
            ret = list(map(list, zip(*ret)))
        elif return_type == 'df':
            ret = _pd.DataFrame(ret, columns=columns)
        return ret

    def _set_table_method(self, sel, table_name, func_name):

        def add_doc(value):
            def _doc(func):
                func.__doc__ = value
                return func
            return _doc

        @add_doc(f"""
            Return data from {table_name} table
            Args:
                name        (str): {func_name} name
                return_type (str): One of gen, list, long_list, [df]
            """)
        def _table(name=None, return_type='df'):
            sql = f'SELECT {sel} FROM {table_name}'
            if name is not None:
                sql += f' WHERE name LIKE \'{name.lower()}\''
            self._cur = self._con.cursor().execute(sql)
            return self._return(return_type, sel.split(','))

        setattr(self, func_name, _table)

    # %%--------

    def _check_opt_queries(self, opt_queries):
        """Check option queries from args."""
        x = {k: v for k, v in opt_queries.items()
             if k in ["param", "reg", "city", "sta"]}
        for k, v in x.items():
            if isinstance(v, (int, str)):
                v = [v]
            exist = getattr(Database, f'exist_{k}')
            for i in v:
                if i != '' and not exist(self, i):
                    raise ValueError(f"{k}: '{i}' does not exist.")

    def _build_query(self, qa):
        """Build query."""
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}

        def _get_ids_for_tables(opt_queries):
            """
            Get query results from side tables.

            Args:
                opt_queries (dict): Query parameters
            Return (dict):
                A dict of query results
            """

            def _end_points_(date_ids):
                """Return end points of consecutive integers."""
                diff_date_ids = [date_ids[i] - date_ids[i - 1] for i in
                                 range(1, len(date_ids))]
                endpi = [i for i, v in enumerate(diff_date_ids) if v != 1]
                endpi = [-1] + endpi + [len(date_ids) - 1]
                endpi = [[endpi[i - 1] + 1, endpi[i]]
                         for i in range(1, len(endpi))]
                endp = [[date_ids[i[0]], date_ids[i[1]]] for i in endpi]
                return endp

            def _get_ids_(where, table):
                with _closing(self._con.cursor()) as cur:
                    sql = _build.select('id', where, table)
                    x = cur.execute(sql).fetchall()
                if len(x) > 0:
                    x = [i[0] for i in x]
                return x

            param_ids = _get_ids_({'name': opt_queries['param']}, 'param')
            reg_ids = _get_ids_({'name': opt_queries['reg']}, 'reg')
            city_ids = _get_ids_({'name': opt_queries['city'], 'reg': reg_ids},
                                 'city')
            sta_ids = _get_ids_({'name': opt_queries['sta'], 'city': city_ids},
                                'sta')
            date_ids = _get_ids_({k: opt_queries[k] for k in
                                  Database._keys_date}, 'cal')
            if len(date_ids) == 0:
                date_ids = []
            else:
                if len(date_ids) > 1:
                    with _closing(self._con.cursor()) as cur:
                        ret = cur.execute('SELECT MAX(date) FROM data')
                        max_date = ret.fetchone()[0]
                    date_ids = [i for i in date_ids if i <= max_date]
                    date_ids = _end_points_(date_ids)

            # create query for dat table
            where = {'param': param_ids, 'sta': sta_ids, 'date': date_ids}
            return where

        select = _build.main_select_string(qa.select)

        args = qa.to_dict(all_args=True)

        self._check_opt_queries(args)
        where_ids = _get_ids_for_tables(args)
        select_data = _build.select('*', where_ids, 'data')
        sql = """
            SELECT
                {select}
            FROM
            (SELECT
                param.name AS param,
                reg.name AS reg,
                city.name AS city_ascii,
                city.nametr AS city,
                sta.name AS sta_ascii,
                sta.nametr AS sta,
                sta.lat AS lat,
                sta.lon AS lon,
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
            INNER JOIN param ON param.id = data.param
            INNER JOIN reg ON reg.id = city.reg
            INNER JOIN city ON city.id = sta.city
            INNER JOIN sta ON sta.id = data.sta
            INNER JOIN cal ON cal.id = data.date);"""
        return (sql.format(select=select, data=select_data),
                select.split(','),
                args)

    def _generator(  # pylint: disable=R0914,R0915
        self,
        query,
        sel,
        opt_queries,
        include_nan=True
    ):
        """Query result generator."""

        def get_cal_table(opt_queries):
            where = {k: opt_queries[k] for k in Database._keys_date}
            sql = _build.select('*', where, 'cal')
            if len(sql) == 0:
                sql = 'SELECT * FROM cal'
            with _closing(self._con.cursor()) as cur:
                x = cur.execute(sql).fetchall()
            return x

        def replace_list(r, cal_row, sel):
            for s in sel:
                if s in Database._keys_date:
                    i = sel.index(s)
                    j = Database._keys_date.index(s) + 1
                    r[i] = cal_row[j]
            if 'value' in sel:
                r[sel.index('value')] = float('NaN')
            return r

        def create_nan(cur_date_index, last_row, sel, cal):
            cur_date_index += 1
            while cur_date_index < len(cal):
                cal_row = cal[cur_date_index]
                yield replace_list(list(last_row), cal_row, sel)
                cur_date_index += 1
            cur_date_index = -1
            return cur_date_index

        def get_sel_indices(sel):
            _sel_keys_ = ['param', 'sta', 'date', 'value']
            index = dict(zip(_sel_keys_,
                             [sel.index(i) if i in sel else -1
                              for i in _sel_keys_]))
            for k, v in index.items():
                if v < 0:
                    raise Exception(k + ' cannot be found')
            return index

        cal = get_cal_table(opt_queries)
        index = get_sel_indices(sel)
        cur = self._con.cursor().execute(query)

        prev_param = ''
        prev_sta = ''
        last_row = None
        cur_date_index = -1
        while True:
            rows = cur.fetchmany()
            if not rows:
                break
            for r in rows:
                if include_nan:
                    cur_pol = r[index['param']]  # current parameter
                    cur_sta = r[index['sta']]  # current station

                    if prev_param != cur_pol and cur_date_index > -1:
                        for i in create_nan(cur_date_index,
                                            last_row, sel, cal):
                            yield i
                        cur_date_index = -1

                    if prev_sta != cur_sta and cur_date_index > -1:
                        for i in create_nan(cur_date_index,
                                            last_row, sel, cal):
                            yield i
                        cur_date_index = -1

                    prev_param = cur_pol
                    prev_sta = cur_sta
                    cur_date_index += 1  # current date index
                    cur_date = cal[cur_date_index][1]
                    row_date = r[index['date']]

                    while cur_date < row_date:
                        yield replace_list(list(r), cal[cur_date_index], sel)
                        cur_date_index += 1
                        cur_date = cal[cur_date_index][1]
                last_row = list(r)
                yield last_row
        if include_nan and last_row is not None:
            for i in create_nan(cur_date_index, last_row, sel, cal):
                yield i
        cur.close()

    def _query_data(self, qa, as_list=False, include_nan=True):
        """Query database."""
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}
        query, sel, opt_queries = self._build_query(qa)
        ret = self._generator(query, sel, opt_queries, include_nan)
        if as_list:
            ret = list(ret)
        return ret, sel, query

    def _query(self, *args, **kwargs):
        """Query database (Internal)."""
        data, _, _ = self._query_data(
            args, kwargs, include_nan=False)
        return data

    # %%--------

    def is_measured(self, param, city, station):
        """
        Check if a single parameter is measured at station or not.

        Args:
            param   (str) : Parameter to search in database
            city    (str) : City to search in database
            station (str) : Station to search in database
        """
        return len(self.measured(param, city, station,
                   return_type='list')) > 0

    def exist_param(self, name):
        """
        Check if parameter exists or not.

        Args:
            name (str) : Parameter to search in database
        Return:
            True/False
        """
        return len(self.param(name, return_type='list')) > 0

    def exist_reg(self, name):
        """
        Check if region exists or not.

        Args:
            name (str) : Region to search in database
        Return:
            True/False
        """
        return len(self.reg(name, return_type='list')) > 0

    def exist_city(self, name, region=''):
        """
        Check if city exists or not.

        Args:
            name   (str) : City name to search in database.
            region (str) : Region name to search in database.
                                 [Default: Empty]
        Return:
            True/False
        """
        return len(self.city(name, region, return_type='list')) > 0

    def exist_sta(self, name, city='', region=''):
        """
        Check if station exist or not.

        Args:
            name   (str) : Station name to search in database.
            city   (str) : City name to search in database [Default: Empty].
            region (str) : Region name to search in database [Default: Empty].
        Return:
            True/False
        """
        return len(self.sta(name, city, region, return_type='list')) > 0

    def query(self, *args, **kwargs):
        """
        Query database by various arguments.

        Args:
            param (str, list)      : parameter name
            reg   (str, list)      : Region Name
            city  (str, list)      : City Name
            sta   (str, list)      : Station Name
            date  (str, list)      : Date
            year  (str, list, int) : Year
            month (str, list, int) : Month
            day   (str, list, int) : Day
            hour  (str, list, int) : Hour
            week  (str, list, int) : Week of year
            doy   (str, list, int) : Day of year
            hoy   (str, list, int) : Hour ofyear
        --
            include_nan (bool): Include NaN in results?
            verbose     (bool): Detailed output
        """

        qa = DatabaseQueryArguments(*args, **kwargs)
        args = _utils.get_args(
            {}, qa.rest, {'include_nan': True, 'verbose': False})

        include_nan = args.pop('include_nan')
        verbose = args.pop('verbose')

        t1 = _time()
        data, colnames, query = self._query_data(
            qa, as_list=self._return_type == 'list',
            include_nan=include_nan)

        if verbose:
            print(query)
        ret = data
        if self._return_type == 'long_list':
            ret = list(map(list, zip(*data)))
        elif self._return_type == 'df':
            ret = _pd.DataFrame(data, columns=colnames)
        elif self._return_type == 'xarray':
            param_to_variable = False
            if 'param_to_variable' in kwargs.keys():
                param_to_variable = kwargs.pop('param_to_variable')
            ret = list(map(list, zip(*data)))
            ret = _utils.long_to_xarray(ret, colnames,
                                        self.name, param_to_variable)

        t2 = _time()
        elapsed = t2 - t1
        if verbose:
            print(f'Query completed in {elapsed:.3f} seconds.')
        return ret

    def param(self, *args, **kwargs):
        """
        Parameter data.

        Args:
            name   (str, list) : Parameter to search in database
            select (str, list) : Select parameters
                                 Possible values: ['id']
            return_type (str)  : One of 'gen', 'list', 'long_list', ['df']
        Return:
            Parameter data
        """
        args = _utils.get_args(
            args, kwargs,
            {'name': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['id', 'name', 'long_name', 'unit'],
                    [False, True, True, True])))

        sql = f"""
            SELECT
                {sel}
            FROM
            (SELECT
                param.id AS id,
                param.name AS name,
                param.long_name AS long_name,
                unit.ascii AS unit
            FROM
                param
            INNER JOIN unit ON unit.id = param.unit)""" + \
            _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        return self._return(return_type, sel.split(','))

    def unit(self, *args, **kwargs):
        """
        Get unit data.

        Args:
            name   (str, list) : Unit to search in database
            param  (str, list) : Parameter to search in database
            select (str, list) : Select parameters
                                 Possible values: ['id']
            return_type (str)  : One of 'gen', 'list', 'long_list', ['df']
        Return:
            Unit data
        """
        args = _utils.get_args(
            args, kwargs,
            {'name': '', 'param': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['id', 'param', 'name', 'ascii', 'long_name', 'latex'],
                    [False, True, True, True, True, True])))

        sql = f"""
            SELECT
                {sel}
            FROM
            (SELECT
                unit.id AS id,
                param.name AS param,
                unit.name AS name,
                unit.ascii AS ascii,
                unit.long_name AS long_name,
                unit.latex AS latex
            FROM
                param
            INNER JOIN unit ON unit.id = param.unit)""" + \
            _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        return self._return(return_type, sel.split(','))

    def reg(self, *args, **kwargs):
        """
        Parameter data.

        Args:
            name   (str, list) : Region to search in database
            select (str, list) : Select parameters
                                 Possible values:
                                    ['id', 'nametr', 'lat', 'lon']
            return_type (str)  : One of 'gen', 'list', 'long_list', ['df']
        Return:
            Region data
        """
        args = _utils.get_args(
            args, kwargs,
            {'name': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')
        args['name'] = _utils.to_ascii(args['name'])

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['id', 'name', 'nametr', 'lat', 'lon'],
                    [False, True, False, False, False])))
        sql = f"""
            SELECT
                {sel}
            FROM
                reg""" + _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        return self._return(return_type, sel.split(','))

    def city(self, *args, **kwargs):
        """
        City data.

        Args:
            name   (str, list) : City name to search in database.
            region (str, list) : Region name to search in database.
            select (str, list) : Select parameters
                                 Possible values:
                                    ['id', 'nametr', 'region', 'regiontr',
                                     'lat', 'lon']
            return_type (str)  : One of 'gen', 'list', 'long_list', ['df']
        Return:
            City data
        """
        args = _utils.get_args(
            args, kwargs,
            {'name': '', 'region': '', 'select': '', 'return_type': 'df',
             'set_index': False})
        return_type = args.pop('return_type')
        set_index = args.pop('set_index')
        for k in list(args.keys())[0:2]:
            args[k] = _utils.to_ascii(args[k])

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['region', 'regiontr', 'id', 'name', 'nametr',
                     'lat', 'lon'],
                    [True, False, False, True, False, False, False])))

        sql = f"""
            SELECT
                {sel}
            FROM
            (SELECT
                city.id AS id,
                city.name AS name,
                city.nametr AS nametr,
                reg.name AS region,
                reg.nametr AS regiontr,
                city.lat AS lat,
                city.lon AS lon
            FROM
                city
            INNER JOIN reg ON reg.id = city.reg)""" + \
            _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        ret = self._return(return_type, sel.split(','))
        if return_type == 'df' and set_index:
            cols = ret.columns.tolist()
            for n in ['id', 'name', 'nametr', 'lat', 'lon']:
                if n in cols:
                    cols.remove(n)
            ret = ret.set_index(cols)
        return ret

    def sta(self, *args, **kwargs):
        """
        Station data.

        Args:
            name   (str, list) : Station name to search in database.
            city   (str, list) : City name to search in database.
            region (str, list) : Region name to search in database.
            select (str, list) : Select parameters
                                 Possible values:
                                    ['id', 'nametr', 'region', 'regiontr',
                                     'lat', 'lon']
            return_type (str)  : One of 'gen', 'list', 'long_list', ['df']
        Return:
            Station data
        """
        args = _utils.get_args(
            args, kwargs,
            {'name': '', 'city': '', 'region': '', 'select': '',
             'return_type': 'df', 'set_index': False})
        return_type = args.pop('return_type')
        set_index = args.pop('set_index')
        for k in list(args.keys())[0:3]:
            args[k] = _utils.to_ascii(args[k])

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['region', 'regiontr', 'id', 'city', 'citytr',
                     'name', 'nametr', 'lat', 'lon'],
                    [True, False, False, True, False,
                     True, False, False, False])))

        sql = f"""
            SELECT
                {sel}
            FROM
            (SELECT
                sta.id AS id,
                sta.name AS name,
                city.name AS city,
                city.nametr AS citytr,
                reg.name AS region,
                reg.nametr AS regiontr,
                city.lat AS lat,
                city.lon AS lon
            FROM
                sta
            INNER JOIN reg ON reg.id = city.reg
            INNER JOIN city ON city.id = sta.city)""" + \
            _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        ret = self._return(return_type, sel.split(','))
        if return_type == 'df' and set_index:
            cols = ret.columns.tolist()
            for n in ['id', 'name', 'nametr', 'lat', 'lon']:
                if n in cols:
                    cols.remove(n)
            ret = ret.set_index(cols)
        return ret

    def measured(self, *args, **kwargs):
        """
        Show measured parameters by city - station.

        If a parameter is measured in a station, result is True,
        otherwise False.

        Args:
            param       (str, list) : Parameter(s) to search in database
            city        (str, list) : City/cities to search in database
            station     (str, list) : Station(s) to search in database
            region      (str, list) : Region(s) to search in database

            select      (str, list) : Select parameters
                                      Possible values: ['id', 'region']
            wide        (bool)      : Convert to wide DataFrame
                                      if return_type='df'. Default is False.
            as_str      (bool)      : Convert True to 'X' and False to ''
                                      if return_type='df'
            return_type (str)       : One of 'gen', 'list', 'long_list', ['df']
        Return: :
            Measured data for stations
        """
        # Get default function arguments
        args = _utils.get_args(
            args, kwargs,
            {'param': '', 'city': '', 'station': '',
             'region': '', 'select': '', 'wide': False,
             'as_str': False, 'return_type': 'df', 'set_index': False})

        wide = args.pop('wide')
        as_str = args.pop('as_str')
        return_type = args.pop('return_type')
        set_index = args.pop('set_index')
        for k in list(args.keys())[0:4]:
            args[k] = _utils.to_ascii(args[k])

        sel = _build.select_string(
            args.pop('select'),
            dict(
                zip(['param', 'region', 'city', 'id', 'station', 'value'],
                    [True, True, True, False, True, True])))
        sql = f"""
            SELECT
                {sel}
            FROM
            (SELECT
                param.name AS param,
                reg.name AS region,
                city.name AS city,
                sta.id AS id,
                sta.name AS station,
                measurement.value AS value
            FROM
                measurement
            INNER JOIN sta ON sta.id = measurement.sta
            INNER JOIN param ON param.id = measurement.param
            INNER JOIN city ON city.id = sta.city
            INNER JOIN reg ON reg.id = city.reg)""" + \
            _build.where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        df = self._return('df', sel.split(','))
        T = 'X' if as_str else True
        F = '' if as_str else False
        df['value'] = T
        cols = df.columns.tolist()
        cols.remove('value')
        if wide:
            cols.remove('param')
            df = df.pivot(index=cols,
                          columns='param',
                          values='value')
            df = df.replace(_np.nan, F)

        else:
            if set_index:
                df = df.set_index(cols)

        if return_type != 'df':
            df = df.reset_index()
            df = [v.to_list() for k, v in df.items()]
            if return_type == 'list':
                df = list(map(list, zip(*df)))
            if return_type == 'gen':
                df = list(map(list, zip(*df)))

                def generator():
                    for i in df:
                        yield i
                return generator()

        return df

    def print_lic(self):
        """Print license information."""
        fn = _path.join(options.db_path, self._name + '.LICENSE')
        if _path.exists(fn):
            with open(fn, "r", encoding="utf-8") as f:
                print(f.read())
        else:
            print('LICENSE file cannot be found for', self._name, 'database.')

    @staticmethod
    def dropna(x):
        """Simplify xarray object by dropping all NA dims."""
        if not isinstance(x, _xr.core.dataarray.DataArray):
            raise ValueError('x must be a DataArray object')
        for d in tuple(d for d in x.dims if d != 'date'):
            x = x.dropna(dim=d, how='all')
        return x

    @staticmethod
    def to_netcdf(x, file):
        """
        Save xarray as netcdf file.

        Args:
            x (DataArray or Dataset): xarray object
            file (str): File name to save
        """
        enc = {}
        encoding = {'dtype': _np.dtype('float32'), 'zlib': True,
                    'complevel': 5}
        if isinstance(x, _xr.core.dataarray.DataArray):
            enc.update({x.name: encoding})
        elif isinstance(x, _xr.core.dataset.Dataset):
            for k in x.keys():
                enc.update({k: encoding})
        x.to_netcdf(file, encoding=enc)

    @staticmethod
    def install(pth):  # pylint: disable=R0914
        """
        Install a database.

        Args:
            pth   (str): A local path or URL to database installation file
        """
        # pylint: disable=C0415

        pat = options.github_pat
        import shutil as sh
        from tempfile import TemporaryDirectory as tmpdir
        with tmpdir() as tdir:
            if pth.startswith('http://') or pth.startswith('https://'):
                from urllib.request import Request
                from urllib.request import urlopen
                from urllib.parse import urlparse
                fn = _path.basename(urlparse(pth).path)
                path_to_file = _path.join(tdir, fn)
                print('Downloading database...')
                req = Request(pth)

                if pat != '':
                    req.add_header("Authorization", f"token {pat}")

                with urlopen(req) as resp, open(path_to_file, 'wb') as f:
                    sh.copyfileobj(resp, f)
            elif _path.exists(pth):
                fn = _path.basename(pth)
                path_to_file = _path.join(tdir, fn)
                sh.copyfile(pth, path_to_file)
            else:
                raise ValueError('pth argument is not valid.')

            sh.unpack_archive(path_to_file, tdir)
            archive_dir = [p for p in _os.scandir(tdir) if p.is_dir()]
            if len(archive_dir) > 0:
                archive_dir = _path.join(tdir, archive_dir[0].name)
            else:
                archive_dir = tdir

            install_script = _path.join(archive_dir, 'install.py')
            if _path.exists(install_script):
                from importlib.util import spec_from_file_location
                from importlib.util import module_from_spec
                spec = spec_from_file_location("install", install_script)
                script = module_from_spec(spec)
                spec.loader.exec_module(script)
                if script.agree_to_lic():
                    script.install(options.db_path)
            else:
                raise FileNotFoundError('Installation script was not found')

    @staticmethod
    def install_github(user, repo):
        """
        Install a database from github.

        Args:
            user (str): User nor organization name
            repo (str): repository name
        """
        pth = 'https://github.com/{}/{}/archive/main.tar.gz'
        Database.install(pth.format(user, repo))

    @staticmethod
    def install_sample():
        """Install sample database."""
        Database.install_github('isezen', 'air-db.samp')
