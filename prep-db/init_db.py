#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0103, W0621
'''
Initialize SQLite airpy.db database
Author : İsmail SEZEN sezenismail@gmail.com
date: 2020-11-14
'''
import os
import sqlite3 as sq3
from sqlite3 import Error
from datetime import timezone
from datetime import datetime as dt
from datetime import timedelta as td


def calendar_table(start='2008-01-01', end='2029-12-31'):
    """
     Create calendar table between start and end date
    :param start, end: start and end date in %Y-%m-%d format
    :type start, end: str
    :return list of list
    """
    b = dt.strptime('2008-01-01', '%Y-%m-%d').replace(tzinfo=timezone.utc)
    s = dt.strptime(start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    e = dt.strptime(end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    dates = [s + td(hours=x) for x in range(0, (e - s).days * 24)]
    uid = [int((i.timestamp() - b.timestamp())/3600) for i in dates]
    date = [i for i in dates]
    year = [i.year for i in dates]
    month = [i.month for i in dates]
    day = [i.day for i in dates]
    hour = [i.hour for i in dates]
    week = [i.isoweekday() for i in dates]
    day_of_year = [i.timetuple().tm_yday for i in dates]
    hour_of_year = [i * 24 + j - 23 for i, j in zip(day_of_year, hour)]
    return list(map(tuple, zip(*[uid, date, year, month, day, hour, week,
                                 day_of_year, hour_of_year])))


def create_con(file=':memory:'):
    """ create a database connection to a SQLite database """
    con = None
    try:
        con = sq3.connect(file)
        print(sq3.version)
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
    c.executemany('INSERT INTO calendar VALUES(?,?,?,?,?,?,?,?,?);',
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
        CREATE TABLE calendar (
            id    INTEGER NOT NULL,
            date  TEXT    NOT NULL,
            year  INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day   INTEGER NOT NULL,
            hour  INTEGER NOT NULL,
            week  INTEGER NOT NULL,
            doy   INTEGER NOT NULL,
            hoy   INTEGER NOT NULL,
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


        -- Table: data
        CREATE TABLE data (
            date  INTEGER NOT NULL
                  REFERENCES date_table (id),
            value REAL,
            meta  INTEGER NOT NULL
                  REFERENCES meta (id),
            sta   INTEGER NOT NULL
                  REFERENCES sta (id),
            pol   INTEGER REFERENCES pol (id)
                  NOT NULL,
            FOREIGN KEY (sta)
            REFERENCES sta (id),
            FOREIGN KEY (pol)
            REFERENCES pol (id)
            FOREIGN KEY (meta)
            REFERENCES meta (id)
        );

        -- Table: meta
        CREATE TABLE meta (
            id     INTEGER NOT NULL,
            name   TEXT NOT NULL,
            [desc] TEXT,
            PRIMARY KEY (id)
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
            FOREIGN KEY (city)
            REFERENCES city (id),
            PRIMARY KEY (id)
        );


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


if __name__ == '__main__':
    con = create_db('database/airpy.db')
    run_sql_script(con, 'SQL/meta.sql')
    run_sql_script(con, 'SQL/reg.sql')
    run_sql_script(con, 'SQL/pol.sql')
    run_sql_script(con, 'SQL/city.sql')
    run_sql_script(con, 'SQL/sta.sql')
    create_calendar_table(con)
    con.close()
