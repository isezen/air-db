"""
airdb package.

~~~~~~~~~~~~~~~~~~~~~
A data access layer (DAL) to easily query environmental time series datasets
obtained from various sources.

version : 0.0.5
github  : https://github.com/isezen/airdb
author  : Ismail SEZEN
email   : sezenismail@gmail.com
license : AGPLv3
date    : 2021
"""

# pylint: disable=C0103, C0201
from collections import defaultdict
from contextlib import closing
from time import time
from warnings import warn as _warn
import itertools as _itertools
from collections.abc import Iterable as _Iterable

import os
import sqlite3 as sq
import pandas as pd
import xarray as _xr
import numpy as _np

__version__ = '0.0.9'
__author__ = 'Ismail SEZEN'
__email__ = 'sezenismail@gmail.com'
__license__ = 'AGPLv3'
__year__ = '2021'


class DatabaseVersionError(Exception):
    """Exception raised for errors Database version mismatch."""

    def __init__(self, db_version, target_version, message=None):
        """
        Create a DatabaseVersionError.

        Args:
            version (str): Database version
            message (str): Error message
        """
        self.db_version = db_version
        self.target_version = target_version
        if message is None:
            message = "Database version must be greater than " + \
                f"{target_version}. Current version is {db_version}."
        self.message = message
        super().__init__(self.message)


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args,
                                                                  **kwargs)
        return cls._instances[cls]


class Options(metaclass=_Singleton):  # pylint: disable=R0903
    """Options."""

    def __init__(self):
        """Initialize."""
        self._db_path = None
        self._github_pat = None

    @property
    def db_path(self):
        """Database path."""
        if self._db_path is not None:
            return self._db_path
        try:
            rpth = os.path.realpath(__file__)
            db_path = os.path.join(os.path.dirname(rpth), 'data')
        except NameError:
            try:
                db_path = os.environ['AIRDB_PATH']
            except KeyError:
                db_path = os.path.join(os.getcwd(), 'data')
        if not os.path.isdir(db_path):
            raise FileNotFoundError("Directory not found '" + db_path + "'.")
        return db_path

    @db_path.setter
    def db_path(self, new_path):
        if not os.path.isdir(new_path):
            raise FileNotFoundError("Directory not found '" + new_path + "'.")
        self._db_path = new_path

    @property
    def github_pat(self):
        """Github Personal Access Token."""
        if self._github_pat is not None:
            return self._github_pat
        try:
            self._github_pat = os.environ['GITHUB_PAT']
        except KeyError:
            self._github_pat = ''
        return self._github_pat

    @github_pat.setter
    def github_pat(self, new_pat):
        if isinstance(new_pat, str):
            self._github_pat = new_pat
        else:
            ValueError('github_pat is not correct')


options = Options()


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
        self._name = name
        self._path = os.path.join(options.db_path, name + '.db')
        if not os.path.exists(self._path):
            raise FileNotFoundError('Database ' + name + ' cannot be found')
        if return_type not in self._opt_ret:
            raise TypeError("return_type must be one of " + str(self._opt_ret))
        self._return_type = return_type
        self._con = sq.connect(self._path, detect_types=sq.PARSE_DECLTYPES)
        self._cur = self._con.cursor()
        target_version = 0.3
        if self.version < target_version:
            raise DatabaseVersionError(self.version, target_version)
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

    @property
    def _opt_ret(self):
        return ['gen', 'list', 'long_list', 'df', 'xarray']

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
            ret = pd.DataFrame(ret, columns=columns)
        return ret

    @staticmethod
    def _to_ascii_(s):
        """Convert chars to ascii counterparts."""
        if s is not None:
            if isinstance(s, list):
                return [Database._to_ascii_(i) for i in s]
            if isinstance(s, str):
                for i, j in zip(list('ğüşıöçĞÜŞİÖÇ'), list('gusiocGUSIOC')):
                    s = s.replace(i, j)
                return s.lower()
            raise ValueError(f'Unknown Value ({s})')
        return s

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

    @staticmethod
    def _build_where_like(query):
        """
        Build a where query with like operator.

        Args:
            query (dict): A dict object contains key-value pairs to construct
                          a WHERE query
        Return: (str):
            WHERE query
        """
        sql = ''
        where_clauses = []
        if any(v is not None and v != '' for v in query.values()):
            where_clauses = []
            for k, v in query.items():
                if v is not None:
                    if isinstance(v, list):
                        where_clauses += [
                            '(' + ' OR '.join(
                                f"{k} LIKE '{i.lower()}'"
                                for i in v if i != '' and i is not None) + ')']
                    else:
                        if isinstance(v, str) and v:
                            where_clauses += [f"({k} LIKE '{v.lower()}')"]
        if len(where_clauses) > 0:
            if any(v is not None and v != '' for v in where_clauses):
                sql += ' WHERE ' + ' AND '.join(where_clauses)
        return sql

    @staticmethod
    def _get_args(args, kwargs, default):
        """Get option queries from args."""
        arguments = default.copy()
        for a, k in zip(args, arguments.keys()):
            arguments[k] = a

        for k in kwargs.keys():
            if k in arguments.keys():
                arguments[k] = kwargs[k]
                if arguments[k] is None:
                    arguments[k] = default[k]
            else:
                raise ValueError(f"Unknown argument ({k})")
        return arguments

    @staticmethod
    def _build_where2(args):
        """
        Build a where query.

        Args:
            args (dict): A dict object contains key-value pairs to construct
                         a WHERE query
        Return: (str):
            WHERE query
        """
        where = args.copy()
        where = {k: v for k, v in where.items()
                 if len(str(v)) > 0 and str(v) != '[]'}

        where = ' AND '.join(
            [Database._build_where(k, v) for k, v in where.items() if v != ''])
        if where != '':
            where = ' WHERE ' + where
        return where

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
        args = Database._get_args(
            args, kwargs,
            {'name': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')

        sel = Database._build_select_string(
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
            Database._build_where_like(args)

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
        args = Database._get_args(
            args, kwargs,
            {'name': '', 'param': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')

        sel = Database._build_select_string(
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
            Database._build_where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        return self._return(return_type, sel.split(','))

    def region(self, *args, **kwargs):
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
        args = Database._get_args(
            args, kwargs,
            {'name': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')
        args['name'] = Database._to_ascii_(args['name'])

        sel = Database._build_select_string(
            args.pop('select'),
            dict(
                zip(['id', 'name', 'nametr', 'lat', 'lon'],
                    [False, True, False, False, False])))
        sql = f"""
            SELECT
                {sel}
            FROM
                reg""" + Database._build_where_like(args)

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
        args = Database._get_args(
            args, kwargs,
            {'name': '', 'region': '', 'select': '', 'return_type': 'df'})
        return_type = args.pop('return_type')
        for k in list(args.keys())[0:2]:
            args[k] = Database._to_ascii_(args[k])

        sel = Database._build_select_string(
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
            Database._build_where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        ret = self._return(return_type, sel.split(','))
        if return_type == 'df':
            cols = ret.columns.tolist()
            for n in ['id', 'name', 'nametr', 'lat', 'lon']:
                if n in cols:
                    cols.remove(n)
            ret = ret.set_index(cols)
        return ret

    def station(self, *args, **kwargs):
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
        args = Database._get_args(
            args, kwargs,
            {'name': '', 'city': '', 'region': '', 'select': '',
             'return_type': 'df'})
        return_type = args.pop('return_type')
        for k in list(args.keys())[0:3]:
            args[k] = Database._to_ascii_(args[k])

        sel = Database._build_select_string(
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
            Database._build_where_like(args)

        self._cur = self._con.cursor().execute(sql + ';')
        ret = self._return(return_type, sel.split(','))
        if return_type == 'df':
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
            region      (str, list) : Region(s) to search in database
            city        (str, list) : City/cities to search in database
            station     (str, list) : Station(s) to search in database

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
        args = Database._get_args(
            args, kwargs,
            {'param': '', 'region': '', 'city': '',
             'station': '', 'select': '', 'wide': False,
             'as_str': False, 'return_type': 'df'})

        wide = args.pop('wide')
        as_str = args.pop('as_str')
        return_type = args.pop('return_type')
        for k in list(args.keys())[0:4]:
            args[k] = Database._to_ascii_(args[k])

        sel = Database._build_select_string(
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
            Database._build_where_like(args)

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

    @staticmethod
    def _split(x, f):
        """
        R-style split function.

        Args:
            x (list): List to be split
            f (list of str or tuple): Factor list split by
        Return (dict):
            Splitted dict of x
        """
        res = defaultdict(list)
        for v, k in zip(x, f):
            res[k].append(v)
        return res

    @staticmethod
    def _concat_(x, dim_names, recursive=True):
        if len(dim_names) != len(list(x.keys())[0]):
            msg = "length of x.keys() must be equal to length of dim_names"
            raise ValueError(msg)

        g2 = list(set(i[:-1] for i in x.keys()))
        g2 = [i[0] if len(i) == 1 else i for i in g2]
        doms = {i: [] for i in g2}
        for k in doms:
            for k2, d2 in x.items():
                k2 = k2[:-1]
                if isinstance(k2, tuple):
                    if len(k2) == 1:
                        k2 = k2[0]
                if k == k2:
                    doms[k].append(d2)

        # combine by last dim
        doms = {k: _xr.concat(v, dim_names[-1]) if len(doms[k]) > 1 else v[0]
                for k, v in doms.items()}

        if recursive:
            k = list(doms.keys())[0]
            if not isinstance(k, str):
                if len(k) > 1:
                    if len(doms) > 1:
                        return Database._concat_(doms, dim_names[:-1])

        return doms

    @staticmethod
    def _long_to_xarray(q, dim_names, db_name, param_to_variable=False):
        """
        Convert long list query result to xarray.

        Args:
            q (list): Long-list result of query
        Return (xarray):
            Combined xarray result of query
        """
        if len(q) == 0:
            raise ValueError('q cannot be empty.')

        if not isinstance(dim_names, list):
            raise ValueError('dim_names must be a list of strings.')

        if len(q) != len(dim_names):
            msg = "length of q must be equal to length of dim_names."
            raise ValueError(msg)

        for d in ['date', 'value']:
            dim_names.remove(d)

        def _ts_to_xr(k, v):
            coords = dict(zip(dim_names, [[i] for i in k]))

            if 'sta' in coords.keys():
                for j in ['lat', 'lon']:
                    if j in coords.keys():
                        if coords[j][0] is None:
                            coords[j] = [float('NaN')]
                        coords[j] = ('sta', coords[j])
                if 'city' in coords.keys():
                    sta_long = [j[0] for i, j in coords.items()
                                if i in ['city', 'sta']]
                    sta_long = [i.title() for i in sta_long]
                    sta_long = " - ".join(sta_long)
                    coords['sta_long'] = ('sta', [sta_long])
                coords['has_measurement'] = ('sta', [True])

            dims = [i for i in coords if i not in
                    ['lat', 'lon', 'sta_long', 'has_measurement']]

            v = list(map(list, zip(*v)))
            obs = _xr.DataArray(v[1], dims=['date'],
                                coords={'date': (('date'), v[0])})
            obs = obs.expand_dims(dict(zip(dims, [1] * len(dims))))

            obs = obs.assign_coords(coords)
            return obs

        x = Database._split(list(map(list, zip(*q[-2:]))),
                            list(map(tuple, zip(*q[:(len(q) - 2)]))))
        ll = {k: _ts_to_xr(k, v) for k, v in x.items()}

        if len(ll) > 0:
            x2 = Database._concat_(ll, dim_names)

            # TODO: Re-order coordinates

            xarr = x2.copy()
            for k, xa in xarr.items():
                xa.name = xa.coords['param'].values.tolist()[0]

            if param_to_variable:
                for k, v in xarr.items():
                    v.name = v.coords['param'].values.tolist()[0]
                    xarr[k] = v[0].drop('param')
                da = _xr.merge(list(xarr.values()), compat='override')
            else:
                da = _xr.concat(xarr.values(), dim='param')
                da.name = db_name

            if 'has_measurement' in da.coords.keys():
                da.coords['has_measurement'].values = da.coords['has_measurement'] == 1

            return da

        return None

    @staticmethod
    def _build_where(var, val):  # pylint: disable=R0912
        """
        Build where part of the query.

        Args:
            var (str): Name of variable
            val (str, list, list of list): Value of variable
        Return (str):
            A where statement for query
        """

        def _get_cmp_(val):
            """Get comparison values as tuple."""
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

        ret = ''
        if isinstance(val, str):
            if ',' in val:
                ret = Database._build_where(var, val.split(','))
            else:
                cmp, val = _get_cmp_(val)
                val = Database._to_ascii_(val).lower()
                if not val.isnumeric():
                    val = '\'' + val + '\''
                ret = cmp.join([var, val])
        elif isinstance(val, list):
            if all(isinstance(v, str) for v in val):  # all is str
                if all(v.startswith(('>', '<')) for v in val):
                    ret = ' AND '.join(
                        [Database._build_where(var, v) for v in val])
                    ret = '(' + ret + ')'
                else:
                    ret = var + ' IN (' + \
                          ','.join(['\'' + Database._to_ascii_(str(i)).lower() + '\''
                                   for i in val]) + ')'
            elif all(isinstance(v, list) for v in val):  # all is list
                val = [['>=' + str(v[0]), '<=' + str(v[1])] for v in val]
                ret = '(' + ' OR '.join(
                    [Database._build_where(var, v) for v in val]) + ')'

            if ret == '':
                if len(val) > 1:
                    ret = var + ' IN (' + ','.join([str(i) for i in val]) + ')'
                else:
                    ret = var + ' = ' + str(val[0])
        else:
            ret = var + ' = ' + str(val)
        return ret

    @staticmethod
    def _build_select(select, where, table):
        """
        Create a select statement for a table.

        Args:
            select (dict, list, str): A dictionary of key:value of boolean or
                string of list or a comma sepereated values as string.
            where (dict): A dictionary of key:value of where statements
            table (str: Name of table in database
        Return (str): Select query
        """
        if isinstance(select, dict):
            select = ','.join([k for k, v in select.items() if v])

        if isinstance(select, list):
            select = ','.join([str(i) for i in select])

        where = {k: v for k, v in where.items()
                 if len(str(v)) > 0 and str(v) != '[]'}

        where = ' AND '.join(
            [Database._build_where(k, v) for k, v in where.items() if v != ''])
        if where != '':
            where = ' WHERE ' + where

        return 'SELECT ' + select + ' FROM ' + table + where

    @staticmethod
    def _build_select_string(sel, default):
        """Build select statement for the db query."""
        opt_select = default.copy()

        if isinstance(sel, str):
            sel = sel.split(',')

        if isinstance(sel, list):

            if any(s in opt_select.keys() for s in sel):
                for s in sel:
                    if s in opt_select.keys():
                        opt_select[s] = True
        else:
            if sel is not None:
                raise ValueError('select string must be comma seperated ' +
                                 'string or list of strings.')

        return ','.join([list(opt_select.keys())[i] for i, x in
                         enumerate(list(opt_select.values())) if x])

    @staticmethod
    def _build_main_select_string(sel):
        """Build select statement for the db query."""
        opt_select = {'param': True, 'reg': False,
                      'city': True, 'sta': True,
                      'lat': False, 'lon': False,
                      'year': False, 'month': False,
                      'day': False, 'hour': False,
                      'week': False, 'doy': False,
                      'hoy': False, 'date': True, 'value': True}

        if isinstance(sel, str):
            sel = sel.split(',')

        if isinstance(sel, list):

            for i in ['param', 'city', 'sta', 'date', 'value']:
                if i not in sel:
                    sel += [i]

            if any(s in opt_select.keys() for s in sel):
                opt_select = {k: False for k in opt_select.keys()}
                for s in sel:
                    if s in opt_select.keys():
                        opt_select[s] = True
        else:
            raise ValueError('select string must be comma seperated string' +
                             ' or list of strings.')

        return ','.join([list(opt_select.keys())[i] for i, x in
                         enumerate(list(opt_select.values())) if x])

    @staticmethod
    def _get_opt_queries(args, kwargs):
        """Get option queries from args."""
        opt_queries = dict.fromkeys(Database._keys, '')
        for a, k in zip(args, opt_queries.keys()):
            opt_queries[k] = a
        # get options from kwargs
        for k in kwargs.keys():
            if k in opt_queries.keys():
                opt_queries[k] = kwargs[k]
        return opt_queries

    def _check_opt_queries(self, opt_queries):
        """Check option queries from args."""
        for k, v in opt_queries.items():
            if isinstance(v, str):
                v = [v]
            for i in v:
                if i != '':
                    if k == 'param':
                        x = self.param(i, return_type='list')
                    if k == 'reg':
                        x = self.region(i, return_type='list')
                    if k == 'city':
                        x = self.city(i, return_type='list')
                    if k == 'sta':
                        x = self.station(i, return_type='list')
                    if len(x) == 0:
                        raise ValueError(f"{k}: '{i}' does not exist.")

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

    def _get_ids_for_tables(self, opt_queries):
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
            with closing(self._con.cursor()) as cur:
                sql = Database._build_select('id', where, table)
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
        date_ids = _get_ids_({k: opt_queries[k] for k in Database._keys_date},
                             'cal')
        if len(date_ids) == 0:
            date_ids = []
        else:
            if len(date_ids) > 1:
                with closing(self._con.cursor()) as cur:
                    ret = cur.execute('SELECT MAX(date) FROM data')
                    max_date = ret.fetchone()[0]
                date_ids = [i for i in date_ids if i <= max_date]
                date_ids = _end_points_(date_ids)

        # create query for dat table
        where = {'param': param_ids, 'sta': sta_ids, 'date': date_ids}
        return where

    def _build_query(self, args, kwargs):
        """Build query."""
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}

        select = kwargs.pop('select') if 'select' in kwargs.keys() else ''
        select = Database._build_main_select_string(select)

        opt_queries = Database._get_opt_queries(args, kwargs)
        self._check_opt_queries(opt_queries)
        where_ids = self._get_ids_for_tables(opt_queries)
        select_data = Database._build_select('*', where_ids, 'data')
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
                opt_queries)

    def _data_generator(  # pylint: disable=R0914,R0915
        self,
        query,
        sel,
        opt_queries,
        include_nan=True
    ):
        """Query result generator."""

        def get_cal_table(opt_queries):
            where = {k: opt_queries[k] for k in Database._keys_date}
            sql = Database._build_select('*', where, 'cal')
            if len(sql) == 0:
                sql = 'SELECT * FROM cal'
            with closing(self._con.cursor()) as cur:
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

    def _query_data(self, args, kwargs, as_list=False, include_nan=True):
        """Query database."""
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}
        query, sel, opt_queries = self._build_query(args, kwargs)
        ret = self._data_generator(query, sel, opt_queries, include_nan)
        if as_list:
            ret = list(ret)
        return ret, sel, query

    def _query(self, *args, **kwargs):
        """Query database (Internal)."""
        data, _, _ = self._query_data(
            args, kwargs, include_nan=False)
        return data

    def query(self, *args, **kwargs):
        """
        Query database by various arguments.

        Args:
            param (str): parameter name
            reg   (str): Region Name
            city  (str): City Name
            sta   (str): Station Name
            date  (str): Date
            year  (str): Year
            month (str): Month
            day   (str): Day
            hour  (str): Hour
            week  (str): Week of year
            doy   (str): Day of year
            hoy   (str): Hour ofyear
        --
            include_nan (bool): Include NaN in results?
            verbose     (bool): Detailed output
        """
        include_nan = True  # default NaN behaviour
        if 'include_nan' in kwargs.keys():
            include_nan = kwargs.pop('include_nan')

        verbose = False  # verbose option
        if 'verbose' in kwargs.keys():
            verbose = kwargs.pop('verbose')

        t1 = time()
        data, colnames, query = self._query_data(
            args, kwargs, as_list=self._return_type == 'list',
            include_nan=include_nan)

        if verbose:
            print(query)
        ret = data
        if self._return_type == 'long_list':
            ret = list(map(list, zip(*data)))
        elif self._return_type == 'df':
            ret = pd.DataFrame(data, columns=colnames)
        elif self._return_type == 'xarray':
            param_to_variable = False
            if 'param_to_variable' in kwargs.keys():
                param_to_variable = kwargs.pop('param_to_variable')
            ret = list(map(list, zip(*data)))
            ret = Database._long_to_xarray(ret, colnames,
                                           self.name, param_to_variable)

        t2 = time()
        elapsed = t2 - t1
        if verbose:
            print(f'Query completed in {elapsed:.3f} seconds.')
        return ret

    def print_lic(self):
        """Print license information."""
        fn = os.path.join(options.db_path, self._name + '.LICENSE')
        if os.path.exists(fn):
            with open(fn, "r", encoding="utf-8") as f:
                print(f.read())
        else:
            print('LICENSE file cannot be found for', self._name, 'database.')

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
                fn = os.path.basename(urlparse(pth).path)
                path_to_file = os.path.join(tdir, fn)
                print('Downloading database...')
                req = Request(pth)

                if pat != '':
                    req.add_header("Authorization", f"token {pat}")

                with urlopen(req) as resp, open(path_to_file, 'wb') as f:
                    sh.copyfileobj(resp, f)
            elif os.path.exists(pth):
                fn = os.path.basename(pth)
                path_to_file = os.path.join(tdir, fn)
                sh.copyfile(pth, path_to_file)
            else:
                raise ValueError('pth argument is not valid.')

            sh.unpack_archive(path_to_file, tdir)
            archive_dir = [p for p in os.scandir(tdir) if p.is_dir()]
            if len(archive_dir) > 0:
                archive_dir = os.path.join(tdir, archive_dir[0].name)
            else:
                archive_dir = tdir

            install_script = os.path.join(archive_dir, 'install.py')
            if os.path.exists(install_script):
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
