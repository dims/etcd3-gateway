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
import uuid

import requests

from etcd3gw.lease import Lease
from etcd3gw.lock import Lock
from etcd3gw.utils import _decode
from etcd3gw.utils import _encode
from etcd3gw.utils import _increment_last_byte
from etcd3gw.utils import DEFAULT_TIMEOUT


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
        if sort_order is None:
            order = 0
        elif sort_order == 'ascend':
            order = 1
        elif sort_order == 'descend':
            order = 2
        else:
            raise ValueError('unknown sort order: "{}"'.format(sort_order))

        if sort_target is None or sort_target == 'key':
            target = 0
        elif sort_target == 'version':
            target = 1
        elif sort_target == 'create':
            target = 2
        elif sort_target == 'mod':
            target = 3
        elif sort_target == 'value':
            target = 4
        else:
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
