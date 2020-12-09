#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0321, W0621
"""
Process data files from CSB and convert them to SQLite database
Download CSV files from
https://drive.google.com/file/d/1y0y91IW1RIDAb42jfuvNzBWx3ij_Ka2A/view?ts=5f746696
Author : isezen sezenismail@gmail.com
date: 2020-10-22

NOTES:
datetime example:
c.execute("SELECT datetime((date * 3600) + 1199145600,'unixepoch'), value FROM
           data WHERE station=1002 AND pollutant=6")
"""

import os
import re
from glob import glob
import sqlite3 as sq3
import hashlib
import pickle
from timeit import default_timer as timer
from datetime import timezone
from datetime import datetime as dt
from collections import namedtuple as nt
import datatable as dtable
from datatable import f, by, first
from datatable import Frame


def fix_header(header):
    """
    fix specific  errors in the header list.
    :param header: A string object contains csv header information
    :type header: str
    """
    searches = ['Tarih&Saat', 'Date & Time',
                'EMEP - İzmir Seferihisar', 'EMEP - Ankara Çubuk',
                'EMEP - Kırklareli Vize', 'MTHM-MTHM', '-MTHM',
                '-İMES', 'Gemilercekeği', 'Kadiköy', 'Besiktaş',
                'Sirinyer', 'Bayrakli', 'Sıhhıye', 'Bozoyuk',
                'Sonmez', 'İçel', 'Üniv.', '-Erenköy-Belediye',
                '-Karatay-Belediye', '-Selçuklu-Belediye',
                'Seyyar - 1 (06 THL 77) - Düzce Akcakoca',
                'Seyyar - 2 (06 THL 79) - Karabük Ortaokulu',
                'Seyyar - 3 (06 DV 9980) Uzunköprü',
                'Seyyar - 4 (06 DV 9975) - Muş', 'İstasyonkavşağı']
    replaces = ['date', 'date',
                'İzmir - Seferihisar - EMEP', 'Ankara - Çubuk - EMEP',
                'Kırklareli - Vize - EMEP', '- MTHM - MTHM', ' - MTHM',
                ' - İMES', 'Gemilerçekeği', 'Kadıköy', 'Beşiktaş',
                'Şirinyer', 'Bayraklı', 'Sıhhiye', 'Bozüyük',
                'Sönmez', 'Mersin', 'Üni.', ' - Erenköy - Belediye',
                ' - Karatay - Belediye', ' - Selçuklu - Belediye',
                'Düzce - Akçakoca - Seyyar 1 (06 THL 77)',
                'Karabük - Karabük Ortaokulu - Seyyar 2 (06 THL 79)',
                'Edirne - Uzunköprü - Seyyar 3 (06 DV 9980)',
                'Muş - Seyyar 4 (06 DV 9975)', 'İstasyon Kavşağı']
    for s, r in zip(searches, replaces):
        header = header.strip().replace(s, r)
    return header


def fix_dates(dates):
    """
    Fix inappropriate date formats.
    :param dates: list of strings contains any date.
    :type dates: list
    """
    dates = [re.sub(r'([0-9]{2})\.([0-9]{2})\.([0-9]{4})', r'\3-\2-\1', d)
             for d in dates]
    dates = [re.sub(r'([0-9]{2})\/([0-9]{2})\/([0-9]{4})', r'\3-\2-\1', d)
             for d in dates]
    return [d.replace('40676', '2011-05-13 00:00') for d in dates]


def split_val_meta(x):
    """
    if x is numeric return float else return x
    :param x: a string
    :type x: str
    """
    ret = None
    if isinstance(x, list):
        ret = [split_val_meta(i) for i in x]
    elif isinstance(x, dtable.Frame):
        if x.ncols > 1:
            ret = [split_val_meta(i) for i in x]
        else:
            ret = split_val_meta(x.to_list()[0])
    else:
        s = x.lower().replace(',', '.')
        v = s.replace('.', '').replace('-', '')
        ret = float(s) if v.isnumeric() else s
    return ret


def remove_dups(data, fast=True):
    """ Remove duplicate rows from datatable """
    if fast:
        data = data[:, first(f[1:]), by(f[0])]  # remove duplicate dates
    else:
        hashes = []
        r = []
        for i in range(data.nrows):
            x = [j[0] for j in data[i, :].to_list()]
            x = ';'.join(x)
            h = hashlib.md5(x.encode()).hexdigest()
            if h in hashes:
                r.append(i)
            else:
                hashes.append(h)
        if len(r) > 0:
            del data[r, :]
    return data


def read_cols(data):
    """ Generator for datatable columns """
    for i in data:
        yield split_val_meta(i)
        # yield [split_val_meta(j) for j in i.to_list()[0]]


def read_csv(csv_file):
    """
    Read data from csv file
    :param csv_file: File name
    """
    dates = []
    with open(csv_file) as file:  # read header
        for i, line in enumerate(file):
            colnames = fix_header(line).split(';')
            break
    colnames = ['date' if 'date' in i else i for i in colnames]
    ind2del = [i for i, x in enumerate(colnames) if x in ('', '\n')]
    dtable.options.progress.enabled = False
    data = dtable.fread(csv_file)
    for j in sorted(ind2del, reverse=True):
        del colnames[j]
        del data[:, j]
    data.names = colnames
    data = remove_dups(data)
    cols2del = [i for i, x in enumerate(colnames) if x in ('', '\n')]
    for j in sorted(cols2del, reverse=True):
        del colnames[j]
        del data[j]
    data['date'] = dtable.Frame(fix_dates(data[0].to_list()[0]))
    data = data.sort('date')
    dates = [dt.strptime(d, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
             for d in data[0].to_list()[0]]
    del colnames[0]
    del data[0]

    # cols = read_cols(data)
    cols = data
    data = nt('data', ['header', 'date', 'cols'])
    data = data(colnames, dates, cols)
    return data


def to_ascii(s):
    """ Convert chars to ascii counterparts"""
    frm = list('ğüşıöçĞÜŞİÖÇ')
    to = list('gusiocGUSIOC')
    for i, j in zip(frm, to):
        s = s.replace(i, j)
    return s


def get_city_id(header, cur):
    """ get id's from city table """
    cities = [to_ascii(i.split('-')[0].strip()).lower() for i in header]
    ids = []
    for c in cities:
        cur.execute("SELECT id FROM city WHERE name=?", (c,))
        rows = cur.fetchall()
        ids.append(rows[0][0])
    return ids


def get_sta_names_from_header(header):
    """ get station names from header list"""
    sta_names = []
    for i in header:
        s = i.split('-', 1)
        sta_names.append(s[0] if len(s) == 1 else s[1])
    sta_names = [to_ascii(i.strip()).lower() for i in sta_names]
    sta_names = [i.replace('-', '').replace('  ', ' ') for i in sta_names]
    return sta_names


def get_station_id(header, cur):
    """ get station ids from database """
    city_ids = get_city_id(header, cur)
    sta_names = get_sta_names_from_header(header)
    ids = []
    for n, i in zip(sta_names, city_ids):
        cur.execute("SELECT id FROM sta WHERE name=? AND city=?", (n, i))
        rows = cur.fetchall()
        ids.append(rows[0][0])
    return ids


def get_pol_id(pol_name, cur):
    """ get pollutant id from database """
    cur.execute("SELECT id FROM pol WHERE name=?", (pol_name.lower(),))
    rows = cur.fetchall()
    return rows[0][0]


def get_meta_id_list(cur):
    """ get meta table as list """
    cur.execute("SELECT id, name FROM meta")
    rows = cur.fetchall()
    ids = list(map(list, zip(*rows)))
    return ids


def get_meta_id(meta_name, meta_list):
    """ get meta id from database """
    return [meta_list[0][meta_list[1].index(v)] for v in meta_name]


def save_pkl(path, obj):
    """ Save object as pickle """
    with open(path, 'wb') as f:
        pickle.dump(obj, f)


def save_index_pkl(dates, path_pkl='pickle'):
    """ Save index pkl file """
    path = os.path.join(path_pkl, 'index.pkl')

    if not os.path.exists(path_pkl):
        os.makedirs(path_pkl)

    if not os.path.exists(path):
        b = dt.strptime('2008-01-01', '%Y-%m-%d').replace(tzinfo=timezone.utc)
        dates = [int((i.timestamp() - b.timestamp())/3600)for i in dates]
        save_pkl(path, dates)


def progressBar(iterable, prefix='', suffix='', decimals=1, length=100,
                fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent
                                  complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    total = len(iterable)

    # Progress Bar Printing Function
    def printProgressBar(iteration):
        percent = ("{0:." + str(decimals) + "f}").format(
            100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        br = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{br}| {percent}% {suffix}', end=printEnd)
    # Initial Call
    if len(iterable) > 0:
        printProgressBar(0)
        # Update Progress Bar
        for i, item in enumerate(iterable):
            yield item
            printProgressBar(i + 1)
        # Print New Line on Complete
        print()


def save_data_to_pickle(csv_file, cur, path_pkl='pickle'):
    """ Save csv as pickle files """
    pol_id = get_pol_id(os.path.basename(csv_file).split('.')[0], cur)
    dat = read_csv(csv_file)
    sta_ids = get_station_id(dat.header, cur)
    save_index_pkl(dat.date, path_pkl)
    pkl_files = [os.path.join(path_pkl, '{:d}_{:05d}.pkl'.format(pol_id, s))
                 for s in sta_ids]
    pkl_files = [(i, v) for i, v in enumerate(pkl_files)
                 if not os.path.exists(v)]
    prefix = os.path.basename(csv_file).split('.')[0]
    for i, p in progressBar(pkl_files, prefix=prefix.ljust(4), length=50):
        save_pkl(p, split_val_meta(dat.cols[i]))


def main(path=os.path.dirname(__file__)):
    """ Main method """
    files_raw_data = os.path.join(path, 'raw-data', '*.csv')
    path_data = os.path.join(os.path.split(path)[0], 'airpy', 'data')
    path_pkl = os.path.join(path_data, 'pkl')
    file_db = os.path.join(path_data, 'airpy.db')
    # db_file = 'database/airpy.db'

    s = timer()
    print('Creating pkl files:')
    with sq3.connect(file_db) as con:
        c = con.cursor()
        for csv_file in glob(files_raw_data):
            save_data_to_pickle(csv_file, c, path_pkl)

    elapsed = timer() - s
    print('Elapsed time:', int(elapsed // 60), 'min.',
          int(elapsed % 60), 'sec.')


if __name__ == '__main__':
    main()
