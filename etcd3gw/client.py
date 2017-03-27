#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import threading
import uuid

import requests
from six.moves import queue

from etcd3gw.lease import Lease
from etcd3gw.lock import Lock
from etcd3gw.utils import _decode
from etcd3gw.utils import _encode
from etcd3gw.utils import _increment_last_byte
from etcd3gw.utils import DEFAULT_TIMEOUT
from etcd3gw import watch

_SORT_ORDER = ['ascend', 'descend']
_SORT_TARGET = ['key', 'version', 'create', 'mod', 'value']


class Client(object):
    def __init__(self, host="localhost", port=2379, protocol="http"):
        """Construct an client to talk to etcd3's grpc-gateway's /v3alpha HTTP API

        :param host:
        :param port:
        :param protocol:
        """
        self.host = host
        self.port = port
        self.protocol = protocol
        self.session = requests.Session()

    def get_url(self, path):
        """Construct a full url to the v3alpha API given a specific path

        :param path:
        :return: url
        """
        base_url = self.protocol + '://' + self.host + ':' + str(self.port)
        return base_url + '/v3alpha/' + path.lstrip("/")

    def post(self, *args, **kwargs):
        """helper method for HTTP POST

        :param args:
        :param kwargs:
        :return: json response
        """
        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            raise requests.exceptions.RequestException(
                'Bad response code : %d' % resp.status_code)
        return resp.json()

    def status(self):
        """Status gets the status of the etcd cluster member.

        :return: json response
        """
        return self.post(self.get_url("/maintenance/status"),
                         json={})

    def lease(self, ttl=DEFAULT_TIMEOUT):
        """Create a Lease object given a timeout

        :param ttl: timeout
        :return: Lease object
        """
        result = self.post(self.get_url("/lease/grant"),
                           json={"TTL": ttl, "ID": 0})
        return Lease(int(result['ID']), client=self)

    def lock(self, id=str(uuid.uuid4()), ttl=DEFAULT_TIMEOUT):
        """Create a Lock object given an ID and timeout

        :param id: ID for the lock, creates a new uuid if not provided
        :param ttl: timeout
        :return: Lock object
        """
        return Lock(id, ttl=ttl, client=self)

    def put(self, key, value, lease=None):
        """Put puts the given key into the key-value store.

        A put request increments the revision of the key-value store
        and generates one event in the event history.

        :param key:
        :param value:
        :param lease:
        :return: boolean
        """
        payload = {
            "key": _encode(key),
            "value": _encode(value)
        }
        if lease:
            payload['lease'] = lease.id
        self.post(self.get_url("/kv/put"), json=payload)
        return True

    def get(self, key, metadata=False, sort_order=None,
            sort_target=None, **kwargs):
        """Range gets the keys in the range from the key-value store.

        :param key:
        :param metadata:
        :param sort_order: 'ascend' or 'descend' or None
        :param sort_target: 'key' or 'version' or 'create' or 'mod' or 'value'
        :param kwargs:
        :return:
        """
        try:
            order = 0
            if sort_order:
                order = _SORT_ORDER.index(sort_order) + 1
        except ValueError:
            raise ValueError('sort_order must be one of "ascend" or "descend"')

        try:
            target = 0
            if sort_target:
                target = _SORT_TARGET.index(sort_target) + 1
        except ValueError:
            raise ValueError('sort_target must be one of "key", '
                             '"version", "create", "mod" or "value"')

        payload = {
            "key": _encode(key),
            "sort_order": order,
            "sort_target": target,
        }
        payload.update(kwargs)
        result = self.post(self.get_url("/kv/range"),
                           json=payload)
        if 'kvs' not in result:
            return []

        if metadata:
            def value_with_metadata(item):
                item['key'] = _decode(item['key'])
                value = _decode(item.pop('value'))
                return value, item

            return [value_with_metadata(item) for item in result['kvs']]
        else:
            return [_decode(item['value']) for item in result['kvs']]

    def get_all(self, sort_order=None, sort_target='key'):
        """Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        return self.get(
            key=_encode(b'\0'),
            metadata=True,
            sort_order=sort_order,
            sort_target=sort_target,
            range_end=_encode(b'\0'),
        )

    def get_prefix(self, key_prefix, sort_order=None, sort_target=None):
        """Get a range of keys with a prefix.

        :param sort_order: 'ascend' or 'descend' or None
        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        return self.get(key_prefix,
                        metadata=True,
                        range_end=_encode(_increment_last_byte(key_prefix)),
                        sort_order=sort_order)

    def replace(self, key, initial_value, new_value):
        """Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes or string
        :param new_value: new value of the key
        :type new_value: bytes or string
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        base64_key = _encode(key)
        base64_initial_value = _encode(initial_value)
        base64_new_value = _encode(new_value)
        txn = {
            'compare': [{
                'key': base64_key,
                'result': 'EQUAL',
                'target': 'VALUE',
                'value': base64_initial_value
            }],
            'success': [{
                'request_put': {
                    'key': base64_key,
                    'value': base64_new_value,
                }
            }],
            'failure': []
        }
        result = self.transaction(txn)
        if 'succeeded' in result:
            return result['succeeded']
        return False

    def delete(self, key, **kwargs):
        """DeleteRange deletes the given range from the key-value store.

        A delete request increments the revision of the key-value store and
        generates a delete event in the event history for every deleted key.

        :param key:
        :param kwargs:
        :return:
        """
        payload = {
            "key": _encode(key),
        }
        payload.update(kwargs)

        result = self.post(self.get_url("/kv/deleterange"),
                           json=payload)
        if 'deleted' in result:
            return True
        return False

    def delete_prefix(self, key_prefix):
        """Delete a range of keys with a prefix in etcd."""
        return self.delete(
            key_prefix, range_end=_encode(_increment_last_byte(key_prefix)))

    def transaction(self, txn):
        """Txn processes multiple requests in a single transaction.

        A txn request increments the revision of the key-value store and
        generates events with the same revision for every completed request.
        It is not allowed to modify the same key several times within one txn.

        :param txn:
        :return:
        """
        return self.post(self.get_url("/kv/txn"),
                         data=json.dumps(txn))

    def watch(self, key, **kwargs):
        """Watch a key.

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request
        """
        event_queue = queue.Queue()

        def callback(event):
            event_queue.put(event)

        w = watch.Watcher(self, key, callback, **kwargs)
        canceled = threading.Event()

        def cancel():
            canceled.set()
            event_queue.put(None)
            w.stop()

        def iterator():
            while not canceled.is_set():
                event = event_queue.get()
                if event is None:
                    canceled.set()
                if not canceled.is_set():
                    yield event

        return iterator(), cancel

    def watch_prefix(self, key_prefix, **kwargs):
        """The same as ``watch``, but watches a range of keys with a prefix."""
        kwargs['range_end'] = \
            _increment_last_byte(_encode(key_prefix))
        return self.watch(key_prefix, **kwargs)

    def watch_once(self, key, timeout=None, **kwargs):
        """Watch a key and stops after the first event.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: event
        """
        event_queue = queue.Queue()

        def callback(event):
            event_queue.put(event)

        w = watch.Watcher(self, key, callback, **kwargs)
        try:
            return event_queue.get(timeout=timeout)
        except queue.Empty:
            raise watch.WatchTimedOut()
        finally:
            w.stop()

    def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """Watches a range of keys with a prefix, similar to watch_once"""
        kwargs['range_end'] = \
            _increment_last_byte(_encode(key_prefix))
        return self.watch_once(key_prefix, timeout=timeout, **kwargs)
