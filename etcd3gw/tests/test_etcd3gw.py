# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
test_etcd3-gateway
----------------------------------

Tests for `etcd3gw` module.
"""

import time

from testtools.testcase import unittest
import urllib3

from etcd3gw.client import Client
from etcd3gw.tests import base


def _is_etcd3_running():
    try:
        urllib3.PoolManager().request('GET', '127.0.0.1:2379')
        return True
    except urllib3.exceptions.HTTPError:
        return False


class TestEtcd3Gateway(base.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_status(self):
        response = self.client.status()
        self.assertIsNotNone(response)
        self.assertIn('version', response)
        self.assertIn('header', response)
        self.assertIn('cluster_id', response['header'])

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_with_keys_and_values(self):
        self.assertTrue(self.client.put('foo0', 'bar0'))
        self.assertTrue(self.client.put('foo1', 2001))
        self.assertTrue(self.client.put('foo2', 'bar2'.encode("utf-8")))

        self.assertEqual(['bar0'], self.client.get('foo0'))
        self.assertEqual(['2001'], self.client.get('foo1'))
        self.assertEqual(['bar2'], self.client.get('foo2'))

        self.assertEqual(True, self.client.delete('foo0'))
        self.assertEqual([], self.client.get('foo0'))

        self.assertEqual(False, self.client.delete('foo0'))

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix(self):
        for i in range(20):
            self.client.put('/doot1/range{}'.format(i), 'i am a range')

        values = list(self.client.get_prefix('/doot1/range'))
        assert len(values) == 20
        for value, metadata in values:
            self.assertEqual('i am a range', value)
            self.assertTrue(metadata['key'].startswith('/doot1/range'))

    def test_get_prefix_sort_order(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/doot2/{}'.format(k), v)

        keys = ''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='ascend'):
            keys += remove_prefix(meta['key'], '/doot2/')

        assert keys == initial_keys

        reverse_keys = ''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='descend'):
            reverse_keys += remove_prefix(meta['key'], '/doot2/')

        assert reverse_keys == ''.join(reversed(initial_keys))

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lease(self):
        lease = self.client.lease(ttl=60)
        self.assertIsNotNone(lease)

        ttl = lease.ttl()
        self.assertTrue(0 <= ttl <= 60)

        keys = lease.keys()
        self.assertEqual([], keys)

        ttl = lease.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lease.revoke())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lease_with_keys(self):
        lease = self.client.lease(ttl=60)
        self.assertIsNotNone(lease)

        self.assertTrue(self.client.put('foo12', 'bar12', lease))
        self.assertTrue(self.client.put('foo13', 'bar13', lease))

        keys = lease.keys()
        self.assertEqual(2, len(keys))
        self.assertIn('foo12', keys)
        self.assertIn('foo13', keys)

        self.assertEqual(['bar12'], self.client.get('foo12'))
        self.assertEqual(['bar13'], self.client.get('foo13'))

        self.assertTrue(lease.revoke())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lock_acquire_release(self):
        with self.client.lock(ttl=60) as lock:
            ttl = lock.refresh()
            self.assertTrue(0 <= ttl <= 60)
        self.assertFalse(lock.is_acquired())

        with self.client.lock(ttl=60) as lock:
            self.assertFalse(lock.acquire())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_locks(self):
        lock = self.client.lock(id='xyz-%s' % time.clock(), ttl=60)
        self.assertIsNotNone(lock)

        self.assertTrue(lock.acquire())
        self.assertIsNotNone(lock.uuid)

        ttl = lock.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lock.is_acquired())
        self.assertTrue(lock.release())
        self.assertFalse(lock.release())
        self.assertFalse(lock.is_acquired())
