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

from etcd3gw.utils import _decode
from etcd3gw.utils import _encode


class WatchTimedOut(Exception):
    pass


class Watcher(threading.Thread):
    def __init__(self, client, key, callback, **kwargs):
        threading.Thread.__init__(self)
        self.client = client
        self._key = key
        self._callback = callback
        self._kwargs = kwargs
        self._stopped = False
        self.daemon = True
        self.start()

    def run(self):
        create_watch = {
            'key': _encode(self._key)
        }
        if 'range_end' in self._kwargs:
            create_watch['range_end'] = _encode(self._kwargs['range_end'])
        if 'start_revision' in self._kwargs:
            create_watch['start_revision'] = self._kwargs['start_revision']
        if 'progress_notify' in self._kwargs:
            create_watch['progress_notify'] = self._kwargs['progress_notify']
        if 'filters' in self._kwargs:
            create_watch['filters'] = self._kwargs['filters']
        if 'prev_kv' in self._kwargs:
            create_watch['prev_kv'] = self._kwargs['prev_kv']

        create_request = {
            "create_request": create_watch
        }
        resp = self.client.session.post(self.client.get_url('/watch'),
                                        json=create_request,
                                        stream=True)
        for line in resp.iter_content(chunk_size=None, decode_unicode=True):
            if self._stopped:
                break
            if not line:
                continue
            decoded_line = line.decode('utf-8')
            payload = json.loads(decoded_line)
            if 'created' in payload['result']:
                if payload['result']['created']:
                    continue
                else:
                    raise Exception('Unable to create watch')
            if 'events' in payload['result']:
                for event in payload['result']['events']:
                    event['kv']['key'] = _decode(event['kv']['key'])
                    if 'value' in event['kv']:
                        event['kv']['value'] = _decode(event['kv']['value'])
                    self._callback(event)

    def stop(self):
        self._stopped = True
