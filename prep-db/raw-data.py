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
import glob
import sqlite3 as sq3
import hashlib
from timeit import default_timer as timer
# from sqlite3 import IntegrityError
from datetime import timezone
from datetime import datetime as dt
from collections import namedtuple as nt


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
        header = header.replace(s, r)
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
    dates = [d.replace('40676', '2011-05-13 00:00') for d in dates]
    return [dt.strptime(d, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            for d in dates]


def split2meta(line):
    """
    split string into value and meta parts.
    :param line: A string of comma seperated values
    :type line: str
    """
    lno = line.lower().split(';')
    lns = line.replace('.', '').replace('-', '').split(';')
    m = ['ok' if c.isnumeric() else v for c, v in zip(lns, lno)]
    v = [v if c.isnumeric() else 'NaN' for c, v in zip(lns, lno)]
    v = [float(v) for v in v]
    return v, m


def read_csv_gen(csv_file, colnames, dates):
    """ CSV generator """
    b = dt.strptime('2008-01-01', '%Y-%m-%d').replace(tzinfo=timezone.utc)
    hashes = []
    with open(csv_file) as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            h = hashlib.md5(line.encode()).hexdigest()
            if h in hashes:
                continue
            hashes.append(h)
            line = line.replace(',', '.').strip()
            ln = line.split(';')
            del ln[0]
            for j in [i for i, x in enumerate(colnames) if x in ('', '\n')]:
                # del colnames[j]
                del ln[j]
            ln = ';'.join(ln)
            v, m = split2meta(ln)
            d = [int((dates[i - 1].timestamp() - b.timestamp())/3600)] * len(v)
            yield d, v, m


def read_csv(csv_file):
    """
    Read data from csv file
    :param csv_file: File name
    """
    dates = []
    with open(csv_file) as file:
        for i, line in enumerate(file):
            if i == 0:
                colnames = fix_header(line).split(';')
            else:
                dates.append(line.split(';')[0])
    del colnames[0]
    dates = fix_dates(dates)
    colnames2 = colnames.copy()
    lines = read_csv_gen(csv_file, colnames, dates)
    for j in [i for i, x in enumerate(colnames2) if x in ('', '\n')]:
        del colnames2[j]
    data = nt('data', ['header', 'date', 'lines'])
    data = data(colnames2, dates, lines)
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


def save_data_to_db(csv_file, cur):
    """ save content of csv file to database """
    pol_id = get_pol_id(os.path.basename(csv_file).split('.')[0], cur)
    dat = read_csv(csv_file)
    print(csv_file, 'has been read.', end=' ', flush=True)
    sta_ids = get_station_id(dat.header, cur)
    meta_list = get_meta_id_list(cur)
    for d, v, m in dat.lines:
        m = get_meta_id(m, meta_list)
        x = [[pol_id] * len(d), sta_ids, d, v, m]
        cur.executemany('INSERT INTO data VALUES(?,?,?,?,?);',
                        list(map(tuple, zip(*x))))
    print(' [SAVED]')


if __name__ == '__main__':
    db_file = 'database/airpy.db'
    s = timer()

    with sq3.connect(db_file) as con:
        c = con.cursor()
        for csv_file in glob.glob('raw-data/*.csv'):
            save_data_to_db(csv_file, c)
        con.commit()
        c.execute('CREATE INDEX index_data ON data (pol)')
        c.execute('CREATE INDEX index_data2 ON data (date)')
        c.execute('CREATE INDEX index_data3 ON data (date, pol)')

    elapsed = timer() - s
    print('Elapsed time:', int(elapsed // 60), 'min.',
          int(elapsed % 60), 'sec.')
