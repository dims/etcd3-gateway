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
from etcd3gw.utils import DEFAULT_TIMEOUT


class Client(object):
    def __init__(self, host="localhost", port=2379, protocol="http"):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.session = requests.Session()

    def get_url(self, path):
        base_url = self.protocol + '://' + self.host + ':' + str(self.port)
        return base_url + '/v3alpha/' + path.lstrip("/")

    def post(self, *args, **kwargs):
        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            raise requests.exceptions.RequestException(
                'Bad response code : %d' % resp.status_code)
        return resp.json()

    def status(self):
        return self.post(self.get_url("/maintenance/status"),
                         json={})

    def lease(self, ttl=DEFAULT_TIMEOUT):
        result = self.post(self.get_url("/lease/grant"),
                           json={"TTL": ttl, "ID": 0})
        return Lease(int(result['ID']), client=self)

    def lock(self, id=str(uuid.uuid4()), ttl=DEFAULT_TIMEOUT):
        return Lock(id, ttl=ttl, client=self)

    def put(self, key, value, lease=None):
        payload = {
            "key": _encode(key),
            "value": _encode(value)
        }
        if lease:
            payload['lease'] = lease.id
        self.post(self.get_url("/kv/put"), json=payload)
        return True

    def get(self, key, **kwargs):
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

    def delete(self, key, **kwargs):
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
        return self.post(self.get_url("/kv/txn"),
                                 data=json.dumps(txn))
