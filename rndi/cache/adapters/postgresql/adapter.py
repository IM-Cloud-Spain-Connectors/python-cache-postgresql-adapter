#
# This file is part of the Ingram Micro CloudBlue RnD Integration Connectors SDK.
#
# Copyright (c) 2023 Ingram Micro. All Rights Reserved.
#
import time
from typing import Any, Optional

import jsonpickle
from psycopg2 import connect
from psycopg2.extensions import connection
from rndi.cache.contracts import Cache


def provide_postgresql_cache_adapter(config: dict) -> Cache:
    return PostgreSQLCacheAdapter(
        host=config.get('CACHE_POSTGRESQL_HOST', 'localhost'),
        database_name=config.get('CACHE_POSTGRESQL_DATABASE_NAME', 'cache'),
        user_name=config.get('CACHE_POSTGRESQL_USER_NAME', 'postgres'),
        password=config.get('CACHE_POSTGRESQL_PASSWORD', 'postgres'),
        ttl=int(config.get('CACHE_TTL', 900)),
        port=int(config.get('CACHE_POSTGRESQL_PORT', 5432)),
    )


class PostgreSQLCacheAdapter(Cache):
    _connection = None

    _create_sql = (
        'CREATE TABLE IF NOT EXISTS entries '
        '( key VARCHAR PRIMARY KEY, value VARCHAR, expire_at INTEGER)'
    )
    _create_index = 'CREATE INDEX IF NOT EXISTS keyname_index ON entries (key)'
    _get_sql = 'SELECT value, expire_at FROM entries WHERE key = %s'
    _del_sql = 'DELETE FROM entries WHERE key = %s'
    _del_expired_sql = 'DELETE FROM entries WHERE %s >= expire_at'
    _insert_sql = 'INSERT INTO entries (key, value, expire_at) VALUES (%s, %s, %s) ' \
                  ' ON CONFLICT(key) DO UPDATE SET key = EXCLUDED.key, value = EXCLUDED.value,' \
                  ' expire_at = EXCLUDED.expire_at'
    _update_ttl = 'UPDATE entries SET expire_at = %s WHERE key = %s'
    _clear_sql = 'DELETE FROM entries'

    def __init__(
            self,
            host: str,
            database_name: str,
            user_name: str,
            password: str,
            ttl: int = 900,
            port: int = 5432,
    ):
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.host = host
        self.ttl = ttl
        self.port = port

    @property
    def connection(self) -> connection:
        """
        Returns the Connection object.
        :return: connection
        """
        if self._connection:
            return self._connection

        new_connection = connect(
            host=self.host,
            database=self.database_name,
            user=self.user_name,
            password=self.password,
            port=self.port,
        )

        with new_connection:
            cur = new_connection.cursor()
            cur.execute(self._create_sql)
            cur.execute(self._create_index)

        self._connection = new_connection

        return self._connection

    def has(self, key: str) -> bool:
        return self.get(key) is not None

    def get(self, key: str, default: Any = None, ttl: Optional[int] = None) -> Any:
        try:
            with self.connection as _connection:
                cur = _connection.cursor()
                cur.execute(self._get_sql, (key,))
                row = cur.fetchone()
                if row is None:
                    raise StopIteration

            # entry[1] is the expire_at column, if the current time is greater than the expire_at
            # then delete the entry because it is expired.
            if round(time.time()) >= row[1]:
                with self.connection as _connection:
                    _connection.cursor().execute(self._del_sql, (key,))
                raise StopIteration
            if ttl is not None:
                with self.connection as _connection:
                    _connection.cursor().execute(self._update_ttl, (ttl + round(time.time()), key))
            return jsonpickle.decode(row[0])
        except StopIteration:
            ttl = ttl if ttl else self.ttl
            value = default() if callable(default) else default

            if isinstance(value, tuple):
                value, ttl = value

        return value if value is None else self.put(key, value, ttl)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> Any:
        serialized = jsonpickle.encode(value)
        expire_at = (self.ttl if ttl is None else ttl) + round(time.time())
        with self.connection as _connection:
            cursor = _connection.cursor()
            cursor.execute(self._insert_sql, (key, serialized, expire_at))

        return value

    def delete(self, key: str) -> None:
        with self.connection as _connection:
            cursor = _connection.cursor()
            cursor.execute(self._del_sql, (key,))

    def flush(self, expired_only: bool = False) -> None:
        with self.connection as _connection:
            cursor = _connection.cursor()
            if expired_only:
                cursor.execute(self._del_expired_sql, (round(time.time()),))
            else:
                cursor.execute(self._clear_sql, ())

    def __del__(self):
        if self.connection:
            self.connection.close()
