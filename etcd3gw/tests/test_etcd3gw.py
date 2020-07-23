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

import six
import threading
import time
import uuid

from testtools.testcase import unittest
import urllib3

from etcd3gw.client import Etcd3Client
from etcd3gw import exceptions
from etcd3gw.tests import base

try:
    # Python 3.8 : time.clock was deprecated and removed.
    from time import perf_counter as clock
except ImportError:
    from time import clock


def _is_etcd3_running():
    try:
        urllib3.PoolManager().request('GET', '127.0.0.1:2379')
        return True
    except urllib3.exceptions.HTTPError:
        return False


class TestEtcd3Gateway(base.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Etcd3Client()

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
    def test_client_members(self):
        response = self.client.members()
        self.assertTrue(len(response) > 0)
        self.assertIn('clientURLs', response[0])
        self.assertIn('peerURLs', response[0])

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_with_keys_and_values(self):
        self.assertTrue(self.client.put('foo0', 'bar0'))
        self.assertTrue(self.client.put('foo1', 2001))
        self.assertTrue(self.client.put('foo2', 'bar2'.encode("utf-8")))

        self.assertEqual([six.b('bar0')], self.client.get('foo0'))
        self.assertEqual([six.b('2001')], self.client.get('foo1'))
        self.assertEqual([six.b('bar2')], self.client.get('foo2'))

        self.assertEqual(True, self.client.delete('foo0'))
        self.assertEqual([], self.client.get('foo0'))

        self.assertEqual(False, self.client.delete('foo0'))
        self.assertTrue(len(self.client.get_all()) > 0)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_and_delete_prefix(self):
        for i in range(20):
            self.client.put('/doot1/range{}'.format(i), 'i am a range')

        values = list(self.client.get_prefix('/doot1/range'))
        assert len(values) == 20
        for value, metadata in values:
            self.assertEqual(six.b('i am a range'), value)
            self.assertTrue(metadata['key'].startswith(six.b('/doot1/range')))

        self.assertEqual(True, self.client.delete_prefix('/doot1/range'))
        values = list(self.client.get_prefix('/doot1/range'))
        assert len(values) == 0

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/doot2/{}'.format(k), v)

        keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='ascend'):
            keys += remove_prefix(meta['key'], '/doot2/')

        assert keys == six.b(initial_keys)

        reverse_keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='descend'):
            reverse_keys += remove_prefix(meta['key'], '/doot2/')

        assert reverse_keys == six.b(''.join(reversed(initial_keys)))

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order_explicit_sort_target_key(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys_ordered = 'abcde'
        initial_keys = 'aebdc'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/doot2/{}'.format(k), v)

        keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='ascend', sort_target='key'):
            keys += remove_prefix(meta['key'], '/doot2/')

        assert keys == six.b(initial_keys_ordered)

        reverse_keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='descend', sort_target='key'):
            reverse_keys += remove_prefix(meta['key'], '/doot2/')

        assert reverse_keys == six.b(''.join(reversed(initial_keys_ordered)))

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order_explicit_sort_target_rev(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'aebdc'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/expsortmod/{}'.format(k), v)

        keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/expsortmod', sort_order='ascend', sort_target='mod'):
            keys += remove_prefix(meta['key'], '/expsortmod/')

        assert keys == six.b(initial_keys)

        reverse_keys = six.b('')
        for value, meta in self.client.get_prefix(
                '/expsortmod', sort_order='descend', sort_target='mod'):
            reverse_keys += remove_prefix(meta['key'], '/expsortmod/')

        assert reverse_keys == six.b(''.join(reversed(initial_keys)))

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_replace_success(self):
        key = '/doot/thing' + str(uuid.uuid4())
        self.client.put(key, 'toot')
        status = self.client.replace(key, 'toot', 'doot')
        v = self.client.get(key)
        self.assertEqual([six.b('doot')], v)
        self.assertTrue(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_replace_fail(self):
        key = '/doot/thing' + str(uuid.uuid4())
        self.client.put(key, 'boot')
        status = self.client.replace(key, 'toot', 'doot')
        v = self.client.get(key)
        self.assertEqual([six.b('boot')], v)
        self.assertFalse(status)

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
        self.assertIn(six.b('foo12'), keys)
        self.assertIn(six.b('foo13'), keys)

        self.assertEqual([six.b('bar12')], self.client.get('foo12'))
        self.assertEqual([six.b('bar13')], self.client.get('foo13'))

        self.assertTrue(lease.revoke())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_watch_key(self):
        key = '/%s-watch_key/watch' % str(uuid.uuid4())

        def update_etcd(v):
            self.client.put(key, v)
            out = self.client.get(key)
            self.assertEqual([six.b(v)], out)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = self.client.watch(key)
        for event in events_iterator:
            self.assertEqual(event['kv']['key'], six.b(key))
            self.assertEqual(event['kv']['value'], six.b(str(change_count)))

            # if cancel worked, we should not receive event 3
            assert event['kv']['value'] != six.b('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_watch_prefix(self):
        key = '/%s-watch_prefix/watch/prefix/' % str(uuid.uuid4())

        def update_etcd(v):
            self.client.put(key + v, v)
            out = self.client.get(key + v)
            self.assertEqual([six.b(v)], out)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key_prefix", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = self.client.watch_prefix(key)
        for event in events_iterator:
            if not event['kv']['key'].startswith(six.b(key)):
                continue

            self.assertEqual(event['kv']['key'],
                             six.b('%s%s' % (key, change_count)))
            self.assertEqual(event['kv']['value'], six.b(str(change_count)))

            # if cancel worked, we should not receive event 3
            assert event['kv']['value'] != six.b('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_sequential_watch_prefix_once(self):
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass

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
        lock = self.client.lock(id='xyz-%s' % clock(), ttl=60)
        self.assertIsNotNone(lock)

        self.assertTrue(lock.acquire())
        self.assertIsNotNone(lock.uuid)

        ttl = lock.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lock.is_acquired())
        self.assertTrue(lock.release())
        self.assertFalse(lock.release())
        self.assertFalse(lock.is_acquired())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_create_success(self):
        key = '/foo/unique' + str(uuid.uuid4())
        # Verify that key is empty
        self.assertEqual([], self.client.get(key))

        status = self.client.create(key, 'bar')
        # Verify that key is 'bar'
        self.assertEqual([six.b('bar')], self.client.get(key))
        self.assertTrue(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_create_fail(self):
        key = '/foo/' + str(uuid.uuid4())
        # Assign value to the key
        self.client.put(key, 'bar')
        self.assertEqual([six.b('bar')], self.client.get(key))

        status = self.client.create(key, 'goo')
        # Verify that key is still 'bar'
        self.assertEqual([six.b('bar')], self.client.get(key))
        self.assertFalse(status)
