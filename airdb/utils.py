"""
airdb utils module.

~~~~~~~~~~~~~~~~~~~~~
This module contains utils to help Database class.
"""

# pylint: disable=C0103, C0201
from collections import defaultdict as _defaultdict
import xarray as _xr


def to_ascii(s):
    """
    Convert chars to ascii counterparts.

    Args:
        s (str, list): Value to convert ascii.
    Return (str, list):
        Converted objects
    """
    if s is not None:
        if isinstance(s, list):
            return [to_ascii(i) for i in s]
        if isinstance(s, str):
            for i, j in zip(list('ğüşıöçĞÜŞİÖÇ'), list('gusiocGUSIOC')):
                s = s.replace(i, j)
            return s.lower()
        raise ValueError(f'Unknown Value ({s})')
    return s


def split_str(x):
    """Split string by '',' if contains."""
    if isinstance(x, str) and ',' in x:
        return x.split(',')
    if isinstance(x, list):
        return [split_str(i) for i in x]
    return x


def split(x, f):
    """
    R-style split function.

    Args:
        x (list): List to be split
        f (list of str or tuple): Factor list split by
    Return (dict):
        Splitted dict of x
    """
    res = _defaultdict(list)
    for v, k in zip(x, f):
        res[k].append(v)
    return res


def concat(x, dim_names, recursive=True):
    """Concat xarray objects."""
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
                    return concat(doms, dim_names[:-1])

    return doms


def int2str(x, keys=None):
    """
    Convert <int> to <str> or check if a <str> is convertible to <int>.

    Args:
        x (int, str, list): Arguments to be converted to int
                            or to be checked if it is convertible or not.
        keys (list)       : List of keys to choose in x if x is a <dict>.
    Return:
        Same type with x
    """
    if isinstance(x, int):
        return str(x)
    if isinstance(x, str) and x != '':
        try:
            int(x)
        except ValueError as e:
            msg = f"Cannot parse value to int. '{x}'"
            raise ValueError(msg) from e
    if isinstance(x, list):
        return [int2str(i) for i in x]
    if isinstance(x, dict) and isinstance(keys, list):
        keys2 = [k for k in keys if k in x.keys()]
        for k in keys2:
            try:
                x[k] = int2str(x[k])
            except ValueError as e:  # pylint: disable=W0703
                msg = f"Cannot parse value to int. '{k}: {x[k]}'"
                raise ValueError(msg) from e

    return x


def get_args(args, kwargs, default):
    """Get option queries from args."""
    arguments = default.copy()
    for a, k in zip(args, arguments.keys()):
        arguments[k] = a

    for k in kwargs.keys():
        arguments[k] = kwargs[k]
        if arguments[k] is None:
            arguments[k] = default[k]
    return arguments


def check_arg_types(args, default):
    """
    Check argument types.

    If an argument does not conform to specified type,
    raise TypeError.

    Args:
        args    (dict): A dict of arguments.
        default (dict): A dict of default arguments and their types.
    Return:
        None
    """
    for k, v in args.items():
        if v is not None:
            if k in default.keys() and not isinstance(v, default[k]):
                raise TypeError(f"{k} must be type of {default[k]}")


def check_is_empty_or_none(args):
    """
    Check arguments are empty.

    If an argument is empty or None, raise ValueError.

    Args:
        args: (dict): A dict of arguments.
    Return:
        None
    """
    for k, v in args.items():
        if v == '' or v is None:
            raise ValueError(f"{k} cannot be empty or None")


def long_to_xarray(q, dim_names, db_name, param_to_variable=False):
    """
    Convert long list query result to xarray.

    Args:
        q (list): Long-list result of query
    Return (xarray):
        Combined xarray result of query
    """
    if len(q) == 0:
        return _xr.DataArray([], dims=['date'])

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

    x = split(list(map(list, zip(*q[-2:]))),
              list(map(tuple, zip(*q[:(len(q) - 2)]))))
    ll = {k: _ts_to_xr(k, v) for k, v in x.items()}

    if len(ll) > 0:
        x2 = concat(ll, dim_names)

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


class Build:
    """Build Static Class."""

    def __new__(cls, *args, **kwargs):
        """Instantiate a new instance."""
        raise NotImplementedError("You cannot instantiate this class")

    @staticmethod
    def where_like(query):
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
    def where(var, val):  # pylint: disable=R0912
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
                ret = Build.where(var, val.split(','))
            else:
                cmp, val = _get_cmp_(val)
                val = to_ascii(val).lower()
                if not val.isnumeric():
                    val = '\'' + val + '\''
                ret = cmp.join([var, val])
        elif isinstance(val, list):
            if all(isinstance(v, str) for v in val):  # all is str
                if all(v.startswith(('>', '<')) for v in val):
                    ret = ' AND '.join(
                        [Build.where(var, v) for v in val])
                    ret = '(' + ret + ')'
                else:
                    ret = var + ' IN (' + \
                          ','.join(['\'' + to_ascii(str(i)).lower() + '\''
                                   for i in val]) + ')'
            elif all(isinstance(v, list) for v in val):  # all is list
                val = [['>=' + str(v[0]), '<=' + str(v[1])] for v in val]
                ret = '(' + ' OR '.join(
                    [Build.where(var, v) for v in val]) + ')'

            if ret == '':
                if len(val) > 1:
                    ret = var + ' IN (' + ','.join([str(i) for i in val]) + ')'
                else:
                    ret = var + ' = ' + str(val[0])
        else:
            ret = var + ' = ' + str(val)
        return ret

    @staticmethod
    def where2(args):
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
            [Build.where(k, v) for k, v in where.items() if v != ''])
        if where != '':
            where = ' WHERE ' + where
        return where

    @staticmethod
    def select(value, where, table):
        """
        Create a select statement for a table.

        Args:
            value (dict, list, str): A dictionary of key:value of boolean or
                string of list or a comma sepereated values as string.
            where (dict): A dictionary of key:value of where statements
            table (str: Name of table in database
        Return (str): Select query
        """
        if isinstance(value, dict):
            value = ','.join([k for k, v in value.items() if v])

        if isinstance(value, list):
            value = ','.join([str(i) for i in value])

        where = {k: v for k, v in where.items()
                 if len(str(v)) > 0 and str(v) != '[]'}

        where = ' AND '.join(
            [Build.where(k, v) for k, v in where.items() if v != ''])
        if where != '':
            where = ' WHERE ' + where

        return 'SELECT ' + value + ' FROM ' + table + where

    @staticmethod
    def select_string(sel, default):
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
    def main_select_string(sel):
        """Build select statement for the database query."""
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
            sel2 = sel.copy()
            for i in ['param', 'city', 'sta', 'date', 'value']:
                if i not in sel2:
                    sel2 += [i]

            if any(s in opt_select.keys() for s in sel2):
                opt_select = {k: False for k in opt_select.keys()}
                for s in sel2:
                    if s in opt_select.keys():
                        opt_select[s] = True
        else:
            raise ValueError('select string must be comma seperated string' +
                             ' or list of strings.')

        return ','.join([list(opt_select.keys())[i] for i, x in
                         enumerate(list(opt_select.values())) if x])
