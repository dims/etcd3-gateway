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
        self.assertTrue(self.client.put('foo1', 'bar1'))

        self.assertEqual(['bar0'], self.client.get('foo0'))
        self.assertEqual(['bar1'], self.client.get('foo1'))

        self.assertEqual(True, self.client.delete('foo0'))
        self.assertEqual([], self.client.get('foo0'))

        self.assertEqual(False, self.client.delete('foo0'))

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

        self.assertTrue(self.client.put('foo2', 'bar2', lease))
        self.assertTrue(self.client.put('foo3', 'bar3', lease))

        keys = lease.keys()
        self.assertEqual(2, len(keys))
        self.assertIn('foo2', keys)
        self.assertIn('foo3', keys)

        self.assertEqual(['bar2'], self.client.get('foo2'))
        self.assertEqual(['bar3'], self.client.get('foo3'))

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

        ttl = lock.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lock.is_acquired())
        self.assertTrue(lock.release())
        self.assertFalse(lock.release())
        self.assertFalse(lock.is_acquired())
