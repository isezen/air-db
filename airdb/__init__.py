"""
airdb package
~~~~~~~~~~~~~~~~~~~~~
A data access layer (DAL) to easily query environmental time series datasets
obtained from various sources.

version : 0.0.2
github  : https://github.com/isezen/airdb
author  : Ismail SEZEN
email   : sezenismail@gmail.com
license : AGPLv3
date    : 2021
"""

# pylint: disable=C0103, C0201
from contextlib import closing
from time import time

import os
import sqlite3 as sq
import pandas as pd

__version__ = '0.0.2'
__author__ = 'Ismail SEZEN'
__email__ = 'sezenismail@gmail.com'
__license__ = 'AGPLv3'
__year__ = '2021'


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args,
                                                                  **kwargs)
        return cls._instances[cls]


class Options(metaclass=_Singleton):  # pylint: disable=R0903
    """ Options """

    def __init__(self):
        self._db_path = None
        self._github_pat = None

    @property
    def db_path(self):
        """ Database path """
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
        """ Github Personal Access Token """
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
    """
    This class is used to connect to a specific airdb database.
    """

    _keys_date = ('date', 'year', 'month', 'day', 'hour', 'week', 'doy', 'hoy')
    _keys = ('param', 'reg', 'city', 'sta') + _keys_date + ('value',)

    # %%--------

    def __init__(self, name, return_type='gen'):
        """
        Create a Database object
        Args:
            name        (str): Database name without extension
            return_type (str): One of gen, list, long_list, [df]
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
        self._set_table_method('id,name,lat,lon', 'reg', 'region')
        self._set_table_method('id,nametr,lat,lon', 'city', 'city')
        self._set_table_method('id,nametr,cat,lat,lon', 'sta', 'station')
        self._set_table_method('name,long_name,short_name,unit', 'param',
                               'parameter')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._con.close()

    def __del__(self):
        self._con.close()

    @property
    def _opt_ret(self):
        return ['gen', 'list', 'long_list', 'df']

    # %%--------

    @property
    def path(self):
        """ Path to database file """
        return self._path

    @property
    def name(self):
        """ Name of database """
        return self._name

    @property
    def is_open(self):
        """ Check if connection to the database is open """
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

    def _set_table_method(self, sel, table_name, func_name):

        def add_doc(value):
            def _doc(func):
                func.__doc__ = value
                return func
            return _doc

        @add_doc("""
            Return data from {0} table
            Args:
                name        (str): {1} name
                return_type (str): One of gen, list, long_list, [df]
            """.format(table_name, func_name))
        def _table(name=None, return_type='df'):
            sql = 'SELECT {} FROM {}'.format(sel, table_name)
            if name is not None:
                sql += ' WHERE name LIKE \'{}\''.format(name.lower())
            self._cur = self._con.cursor().execute(sql)
            return self._return(return_type, sel.split(','))

        setattr(self, func_name, _table)

    @staticmethod
    def _build_where(var, val):  # pylint: disable=R0912
        """
        Build where part of the query
        Args:
            var (str): Name of variable
            val (str, list, list of list): Value of variable
        Return (str):
            A where statement for query
        """

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

        ret = ''
        if isinstance(val, str):
            if ',' in val:
                ret = Database._build_where(var, val.split(','))
            else:
                cmp, val = _get_cmp_(val)
                val = _to_ascii_(val).lower()
                if not val.isnumeric():
                    val = '\'' + val + '\''
                ret = cmp.join([var, val])
        elif isinstance(val, list):
            if all([isinstance(v, str) for v in val]):  # all is str
                if all([v.startswith(('>', '<')) for v in val]):
                    ret = ' AND '.join(
                        [Database._build_where(var, v) for v in val])
                    ret = '(' + ret + ')'
                else:
                    ret = var + ' IN (' + \
                          ','.join(['\'' + str(i) + '\'' for i in val]) + ')'
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
        Create a select statement for a table
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
    def _build_main_select_string(sel):
        """ build select statement for the db query """
        opt_select = dict(zip(Database._keys,
                              [True] * 5 + [False] * 7 + [True]))
        if isinstance(sel, str):
            sel = sel.split(',')
        if isinstance(sel, list):
            if any([s in opt_select.keys() for s in sel]):
                opt_select = {k: False for k in opt_select.keys()}
                for s in sel:
                    if s in opt_select.keys():
                        opt_select[s] = True
        return ','.join([list(opt_select.keys())[i] for i, x in
                         enumerate(list(opt_select.values())) if x])

    @staticmethod
    def _get_opt_queries(args, kwargs):
        """ Get option queries from args """
        opt_queries = dict.fromkeys(Database._keys, '')
        for a, k in zip(args, opt_queries.keys()):
            opt_queries[k] = a
        # get options from kwargs
        for k in kwargs.keys():
            if k in opt_queries.keys():
                opt_queries[k] = kwargs[k]
        return opt_queries

    def _get_ids_for_tables(self, opt_queries):
        """
        Get query results from side tables
        Args:
            opt_queries (dict): Query parameters
        Return (dict):
            A dict of query results
        """

        def _end_points_(date_ids):
            """ return end points of consecutive integers """
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
        """ build query """
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}

        select = kwargs.pop('select') if 'select' in kwargs.keys() else ''
        select = Database._build_main_select_string(select)

        opt_queries = Database._get_opt_queries(args, kwargs)
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

    def _data_generator(self, query,  # pylint: disable=R0914,R0915
                        sel, opt_queries, include_nan=True):
        """ Query result generator """

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
        """ Query database """
        # args = ()
        # kwargs = {'pol': 'pm10', 'city': 'adana', 'sta': 'çatalan',
        #           'date': ['>=2015-01-01', '<=2019-01-01'], 'month': 3}
        query, sel, opt_queries = self._build_query(args, kwargs)
        ret = self._data_generator(query, sel, opt_queries, include_nan)
        if as_list:
            ret = list(ret)
        return ret, sel, query

    def query(self, *args, **kwargs):
        """
        Query database by various arguments
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

        t2 = time()
        elapsed = t2 - t1
        if verbose:
            print('Query completed in %f seconds.' % elapsed)
        return ret

    def print_lic(self):
        """ print license information """
        fn = os.path.join(options.db_path, self._name + '.LICENSE')
        if os.path.exists(fn):
            print(open(fn, "r").read())
        else:
            print('LICENSE file cannot be found for', self._name, 'database.')

    @staticmethod
    def install(pth):  # pylint: disable=R0914
        """
        Install a database
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
                    req.add_header("Authorization", "token {}".format(pat))

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
        Install a database from github
        Args:
            user (str): User nor organization name
            repo (str): repository name
        """
        pth = 'https://github.com/{}/{}/archive/main.tar.gz'
        Database.install(pth.format(user, repo))

    @staticmethod
    def install_sample():
        """ Install sample database """
        Database.install_github('isezen', 'air-db.samp')
