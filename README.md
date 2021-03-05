# air-db

_air-db_ is a data access layer (DAL) to easily query atmospheric time series datasets from various sources. _air-db_ does not include any database. It is required to install corresponding database to work _air-db_ properly.

To install sample database:

```python3
from airdb import Database

Database.install_sample()
```

A database query can be implemented as follows:

```python3
from airdb import Database

db = Database('samp', return_type='df')

q = db.query(param=['so2', 'pm10'], city='istanbul', date=['>2010-05-10', '<2012-10-07'], month=5)

print(q)

del db  # close connection to database

```

and the output is:
```
      param      reg      city              sta                date     value
0      pm10  marmara  istanbul      çatladıkapı 2010-05-10 00:00:00  0.798218
1      pm10  marmara  istanbul      çatladıkapı 2010-05-10 01:00:00  0.946180
2      pm10  marmara  istanbul      çatladıkapı 2010-05-10 02:00:00  0.884385
3      pm10  marmara  istanbul      çatladıkapı 2010-05-10 03:00:00  0.537993
4      pm10  marmara  istanbul      çatladıkapı 2010-05-10 04:00:00  0.136689
...     ...      ...       ...              ...                 ...       ...
16123   so2  marmara  istanbul  şirinevler mthm 2012-05-31 19:00:00  0.697663
16124   so2  marmara  istanbul  şirinevler mthm 2012-05-31 20:00:00  0.615755
16125   so2  marmara  istanbul  şirinevler mthm 2012-05-31 21:00:00  0.489289
16126   so2  marmara  istanbul  şirinevler mthm 2012-05-31 22:00:00  0.385102
16127   so2  marmara  istanbul  şirinevler mthm 2012-05-31 23:00:00  0.039451

[16128 rows x 6 columns]
```
