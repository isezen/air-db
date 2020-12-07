#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0103, W0621
'''
Initialize SQLite airpy.db database
Author : İsmail SEZEN sezenismail@gmail.com
date: 2020-11-14
'''
import os
from glob import glob
import sqlite3 as sq3
from sqlite3 import Error
from datetime import timezone
from datetime import datetime as dt
from datetime import timedelta as td


def calendar_table(start='2008-01-01 00:00:00', end='2019-01-01 00:00:00'):
    """
     Create calendar table between start and end date
    :param start, end: start and end date in %Y-%m-%d format
    :type start, end: str
    :return list of list
    """
    fmt = '%Y-%m-%d %H:%M:%S'
    b = dt.strptime('2008-01-01 00:00:00', fmt).replace(tzinfo=timezone.utc)
    s = dt.strptime(start, fmt).replace(tzinfo=timezone.utc)
    e = dt.strptime(end, fmt).replace(tzinfo=timezone.utc)
    dates = [s + td(hours=x) for x in range(0, (e - s).days * 24)]
    uid = [int((i.timestamp() - b.timestamp())/3600) for i in dates]
    year = [i.year for i in dates]
    month = [i.month for i in dates]
    day = [i.day for i in dates]
    hour = [i.hour for i in dates]
    week = [i.isoweekday() for i in dates]
    day_of_year = [i.timetuple().tm_yday for i in dates]
    hour_of_year = [i * 24 + j - 23 for i, j in zip(day_of_year, hour)]
    dates = [d.strftime('%Y-%m-%d %H:%M:%S') for d in dates]
    return list(map(tuple, zip(*[uid, dates, year, month, day, hour, week,
                                 day_of_year, hour_of_year])))


def create_con(file=':memory:'):
    """ create a database connection to a SQLite database """
    con = None
    try:
        con = sq3.connect(file, detect_types=sq3.PARSE_DECLTYPES |
                          sq3.PARSE_COLNAMES)
    except Error as e:
        print(e)
    return con


def exec_sql(con, sql):
    """ Execute an sql statement
    :param con: Connection object
    :param sql: an SQL statement
    :return: None
    """
    try:
        c = con.cursor()
        c.executescript(sql)
    except Error as e:
        print(e)


def create_calendar_table(con):
    """ Create calendar table """
    c = con.cursor()
    c.executemany("""INSERT INTO 'cal'
                     ('id', 'date', 'year', 'month', 'day', 'hour',
                      'week', 'doy', 'hoy') VALUES(?,?,?,?,?,?,?,?,?);""",
                  calendar_table())
    con.commit()


def create_db(file=':memory:', overwrite=True):
    """ create databaser and return connection object
    :param file: Path to file
    :return: connection object
    """
    sql = """
        PRAGMA foreign_keys = off;
        BEGIN TRANSACTION;

        -- Table: calendar
        CREATE TABLE cal (
            id    INTEGER   NOT NULL,
            date  TIMESTAMP NOT NULL,
            year  INTEGER   NOT NULL,
            month INTEGER   NOT NULL,
            day   INTEGER   NOT NULL,
            hour  INTEGER   NOT NULL,
            week  INTEGER   NOT NULL,
            doy   INTEGER   NOT NULL,
            hoy   INTEGER   NOT NULL,
            PRIMARY KEY (id)
        );

        -- Table: city
        CREATE TABLE city (
            id     INTEGER NOT NULL,
            name   TEXT    NOT NULL,
            nametr TEXT    NOT NULL,
            lat    REAL    DEFAULT NULL,
            lon    REAL    DEFAULT NULL,
            reg INTEGER REFERENCES reg (id),
            PRIMARY KEY (id),
            FOREIGN KEY (reg)
            REFERENCES reg (id)
        );

        -- Table: status
        CREATE TABLE status (
            id     INTEGER NOT NULL,
            name   TEXT NOT NULL,
            [desc] TEXT,
            PRIMARY KEY (id)
        );


        -- Table: data
        CREATE TABLE data (
            pol    INTEGER REFERENCES pol (id)
                   NOT NULL,
            sta    INTEGER NOT NULL
                   REFERENCES sta (id),
            date   INTEGER NOT NULL
                   REFERENCES cal (id),
            value  REAL,
            status INTEGER NOT NULL
                   REFERENCES status (id),
            FOREIGN KEY (pol)
            REFERENCES pol (id),
            FOREIGN KEY (sta)
            REFERENCES sta (id),
            FOREIGN KEY (date)
            REFERENCES cal (id),
            FOREIGN KEY (status)
            REFERENCES status (id)
        );

        -- Table: meta
        CREATE TABLE meta (
            id     INTEGER NOT NULL,
            name   TEXT NOT NULL,
            [desc] TEXT,
            PRIMARY KEY (id)
        );

        -- Table: meta data
        CREATE TABLE data_meta (
            pol   INTEGER REFERENCES pol (id)
                  NOT NULL,
            sta   INTEGER NOT NULL
                  REFERENCES sta (id),
            date  INTEGER NOT NULL
                  REFERENCES cal (id),
            value INTEGER NOT NULL
                  REFERENCES meta (id),
            FOREIGN KEY (pol)
            REFERENCES pol (id),
            FOREIGN KEY (sta)
            REFERENCES sta (id),
            FOREIGN KEY (date)
            REFERENCES cal (id)
        );

        -- Table: pollutant
        CREATE TABLE pol (
            id         INTEGER NOT NULL,
            name       TEXT    NOT NULL
                               UNIQUE,
            long_name  TEXT    NOT NULL,
            short_name TEXT,
            unit       TEXT,
            PRIMARY KEY (id)
        );


        -- Table: region
        CREATE TABLE reg (
            id        INTEGER   NOT NULL,
            name      TEXT NOT NULL,
            lat       REAL,
            lon       REAL,
            parent_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY (parent_id)
            REFERENCES reg (id)
        );


        -- Table: station
        CREATE TABLE sta (
            id     INTEGER,
            name   TEXT,
            nametr TEXT,
            cat    TEXT,
            lat    REAL,
            lon    REAL,
            city   INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY (city)
            REFERENCES city (id)
        );

        CREATE VIEW vlong
        AS
        SELECT
            pol.name AS pol,
            reg.name AS reg,
            city.name AS city_ascii,
            city.nametr AS city,
            sta.name AS sta_ascii,
            sta.nametr AS station,
            cal.date AS date,
            cal.year AS year,
            cal.month AS month,
            cal.day AS day,
            cal.hour AS hour,
            cal.week AS week,
            cal.doy AS doy,
            cal.hoy AS hoy,
            value,
            meta.name AS meta
        FROM
            data
        INNER JOIN pol ON pol.id = data.pol
        INNER JOIN reg ON reg.id = city.reg
        INNER JOIN city ON city.id = sta.city
        INNER JOIN sta ON sta.id = data.sta
        INNER JOIN cal ON cal.id = data.date
        INNER JOIN meta ON meta.id = data.meta;

        CREATE VIEW vpm10
        AS
        SELECT
            reg.name AS reg,
            city.name AS city_ascii,
            city.nametr AS city,
            sta.name AS sta_ascii,
            sta.nametr AS station,
            cal.date AS date,
            cal.year AS year,
            cal.month AS month,
            cal.day AS day,
            cal.hour AS hour,
            cal.week AS week,
            cal.doy AS doy,
            cal.hoy AS hoy,
            value,
            meta.name AS meta
        FROM
            data
        INNER JOIN pol ON pol.id = data.pol AND pol.name = 'pm10'
        INNER JOIN reg ON reg.id = city.reg
        INNER JOIN city ON city.id = sta.city
        INNER JOIN sta ON sta.id = data.sta
        INNER JOIN cal ON cal.id = data.date
        INNER JOIN meta ON meta.id = data.meta;

        COMMIT TRANSACTION;
        PRAGMA foreign_keys = on;
    """

    if overwrite & os.path.exists(file):
        os.remove(file)
    con = create_con(file)
    exec_sql(con, sql)
    return con


def run_sql_script(con, f):
    """ Read sql script file and run"""
    if os.path.exists(f):
        with open(f) as file:
            x = file.read()
        exec_sql(con, x)
    else:
        print(f + ' does not exist')


def main(path=os.path.dirname(__file__)):
    """ Main Method """
    path_sql = os.path.join(path, 'SQL')
    db_file = os.path.join(os.path.split(path)[0], 'airpy/data/airpy.db')
    with create_db(db_file) as con:
        for f in glob(os.path.join(path_sql, '*.sql')):
            run_sql_script(con, f)
        create_calendar_table(con)
    print('* Database created at {}'.format(db_file))


if __name__ == '__main__':
    main()
