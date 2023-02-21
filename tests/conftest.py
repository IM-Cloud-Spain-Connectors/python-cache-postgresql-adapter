from __future__ import annotations

from abc import ABCMeta, abstractmethod
from logging import LoggerAdapter
from typing import Dict, List, Optional, Union
from unittest.mock import patch

import pytest
from rndi.cache.adapters.postgresql.adapter import PostgreSQLCacheAdapter
from rndi.cache.contracts import Cache
from rndi.cache.provider import provide_cache


@pytest.fixture
def adapters(logger):
    def __adapters() -> List[Cache]:
        setups = [
            {
                'CACHE_DRIVER': 'postgresql',
                'CACHE_POSTGRESQL_HOST': 'localhost',
                'CACHE_POSTGRESQL_DATABASE_NAME': 'cache',
                'CACHE_POSTGRESQL_USER_NAME': 'postgres',
                'CACHE_POSTGRESQL_PASSWORD': 'postgres',
            },
        ]

        extra = {
            'postgresql': provide_test_postgresql_cache_adapter,
        }

        return [provide_cache(setup, logger(), extra) for setup in setups]

    return __adapters


@pytest.fixture()
def logger():
    def __logger() -> LoggerAdapter:
        with patch('logging.LoggerAdapter') as logger:
            return logger

    return __logger


@pytest.fixture
def counter():
    class Counter:
        instance: Optional[Counter] = None

        def __init__(self):
            self.count = 0

        @classmethod
        def make(cls, reset: bool = False) -> Counter:
            if not isinstance(cls.instance, Counter) or reset:
                cls.instance = Counter()
            return cls.instance

        def increase(self, step: int = 1) -> Counter:
            self.count = self.count + step
            return self

    def __(reset: bool = False) -> Counter:
        return Counter.make(reset)

    return __


class HasEntry(metaclass=ABCMeta):  # pragma: no cover
    @abstractmethod
    def get_entry(self, key: str) -> Optional[Dict[str, Union[str, int]]]:
        """
        Get an entry from the cache, not only the value.
        This is useful for testing purposes when we want to validate the TTL.
        :param key: str The key to search for.
        :return: Optional[Dict[str, Union[str, int]]] The entry if found, None otherwise.
        """


class PostgreSQLCacheAdapterTester(PostgreSQLCacheAdapter, HasEntry):
    def get_entry(self, key: str) -> Optional[Dict[str, Union[str, int]]]:
        with self.connection as _connection:
            cur = _connection.cursor()
            cur.execute(self._get_sql, (key,))
            entry = cur.fetchone()

        if entry is None:
            return None

        return {
            'value': entry[0],
            'expire_at': entry[1],
        }


def provide_test_postgresql_cache_adapter(config: dict) -> Cache:
    return PostgreSQLCacheAdapterTester(
        host=config.get('CACHE_POSTGRESQL_HOST', 'localhost'),
        database_name=config.get('CACHE_POSTGRESQL_DATABASE_NAME', 'cache'),
        user_name=config.get('CACHE_POSTGRESQL_USER_NAME', 'postgres'),
        password=config.get('CACHE_POSTGRESQL_PASSWORD', 'postgres'),
    )
