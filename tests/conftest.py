from __future__ import annotations

from logging import LoggerAdapter
from typing import List, Optional
from unittest.mock import patch

import pytest
from rndi.cache.adapters.postgresql.adapter import provide_postgresql_cache_adapter
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
            'postgresql': provide_postgresql_cache_adapter,
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
