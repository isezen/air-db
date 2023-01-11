"""
airdb errors module.

~~~~~~~~~~~~~~~~~~~~~
This module keeps custom error classes for airdb.
"""


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
