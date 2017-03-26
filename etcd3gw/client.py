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

import base64
import json
import uuid

import requests
import six

from etcd3gw.lease import Lease
from etcd3gw.lock import Lock
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

    def get(self, key, **kwargs):
        """Range gets the keys in the range from the key-value store.

        :param key:
        :param kwargs:
        :return:
        """
        payload = {
            "key": _encode(key),
        }
        payload.update(kwargs)
        result = self.post(self.get_url("/kv/range"),
                           json=payload)
        if 'kvs' not in result:
            return []
        return [base64.b64decode(six.b(item['value'])).decode('utf-8')
                for item in result['kvs']]

    def get_prefix(self, key_prefix, sort_order=None):
        """Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        return self.get(key_prefix,
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
