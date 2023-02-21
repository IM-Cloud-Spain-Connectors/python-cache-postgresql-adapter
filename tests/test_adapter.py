#
# This file is part of the Ingram Micro CloudBlue RnD Integration Connectors SDK.
#
# Copyright (c) 2023 Ingram Micro. All Rights Reserved.
#

import time


def test_adapter_cache_should_return_false_if_has_not_cached_key(adapters):
    for adapter in adapters():
        assert not adapter.has('missing-key')


def test_adapter_cache_should_cache_values_by_key(adapters, counter):
    complex_key = 'https://some.url.com/id/8b3a6052-621e-45cc-be5a-316f486c50aa'
    for adapter in adapters():
        a = adapter.get(complex_key, lambda: (counter(True).increase(1).count, 20))
        b = adapter.get(complex_key)
        assert 1 == a == b
        assert adapter.has(complex_key)


def test_adapter_cache_should_delete_expired_values(adapters):
    for adapter in adapters():
        adapter.put('x', 'some-value-1', 3600)
        adapter.put('y', 'some-value-2', 0)
        adapter.put('z', 'some-value-3', 0)
        assert adapter.has('x')
        assert not adapter.has('y')
        assert not adapter.has('z')


def test_adapter_cache_should_delete_value_by_key(adapters):
    for adapter in adapters():
        adapter.put('x', 'some-value')
        adapter.delete('x')
        adapter.delete('missing-key')
        assert not adapter.has('x')
        assert not adapter.has('missing-key')


def test_adapter_cache_should_flush_only_expired_values(adapters):
    for adapter in adapters():
        adapter.put('x', 'some-value-1', 3600)
        adapter.put('y', 'some-value-2', 0)
        adapter.put('z', 'some-value-3', 0)
        adapter.flush(expired_only=True)
        assert adapter.has('x')
        assert not adapter.has('y')
        assert not adapter.has('z')


def test_adapter_cache_get_should_update_expiration_if_provided(adapters, counter):
    for adapter in adapters():
        adapter.put('x', 'some-value-1', 500)
        adapter.get('x', ttl=300)
        assert adapter.get_entry('x').get('expire_at') == round(time.time() + 300)


def test_adapter_cache_should_flush_all_values(adapters):
    for adapter in adapters():
        adapter.put('x', 'some-value-1')
        adapter.put('y', 'some-value-2')
        adapter.put('z', 'some-value-3')
        adapter.flush()
        assert not adapter.has('x')
        assert not adapter.has('y')
        assert not adapter.has('z')
