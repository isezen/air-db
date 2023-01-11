"""
airdb config module.

~~~~~~~~~~~~~~~~~~~~~
This module keeps configuration realated classes
"""

# pylint: disable=C0103, C0201
import os as _os
from os import path as _path


class _Singleton(type):
    """Singleton class for Options."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args,
                                                                  **kwargs)
        return cls._instances[cls]


class Options(metaclass=_Singleton):
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
            rpth = _path.realpath(__file__)
            db_path = _path.join(_path.dirname(rpth), 'data')
        except NameError:
            try:
                db_path = _os.environ['AIRDB_PATH']
            except KeyError:
                db_path = _path.join(_os.getcwd(), 'data')
        if not _path.isdir(db_path):
            raise FileNotFoundError(f"Directory not found '{db_path}'.")
        return db_path

    @db_path.setter
    def db_path(self, new_path):
        if not _path.isdir(new_path):
            raise FileNotFoundError(f"Directory not found '{new_path}'.")
        self._db_path = new_path

    @property
    def github_pat(self):
        """Github Personal Access Token."""
        if self._github_pat is not None:
            return self._github_pat
        try:
            self._github_pat = _os.environ['GITHUB_PAT']
        except KeyError:
            self._github_pat = ''
        return self._github_pat

    @github_pat.setter
    def github_pat(self, new_pat):
        if isinstance(new_pat, str):
            self._github_pat = new_pat
        else:
            ValueError('github_pat is not correct')
