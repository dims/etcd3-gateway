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

import futurist

from etcd3gw.utils import _encode


class WatchTimedOut(Exception):
    pass


def _watch(client, key, callback, **kwargs):
    create_watch = {
        'key': _encode(key)
    }
    if 'range_end' in kwargs:
        create_watch['range_end'] = _encode(kwargs['range_end'])
    if 'start_revision' in kwargs:
        create_watch['start_revision'] = kwargs['start_revision']
    if 'progress_notify' in kwargs:
        create_watch['progress_notify'] = kwargs['progress_notify']
    if 'filters' in kwargs:
        create_watch['filters'] = kwargs['filters']
    if 'prev_kv' in kwargs:
        create_watch['prev_kv'] = kwargs['prev_kv']

    create_request = {
        "create_request": create_watch
    }
    resp = client.session.post(client.get_url('/watch'),
                               json=create_request,
                               stream=True)
    for line in resp.iter_content(chunk_size=None, decode_unicode=True):
        if line:
            decoded_line = line.decode('utf-8')
            callback(json.loads(decoded_line))


class Watcher(object):
    def __init__(self, client, key, callback, **kwargs):
        self._executor = futurist.ThreadPoolExecutor(max_workers=1)
        self._executor.submit(_watch, client, key, callback, **kwargs)

    def alive(self):
        return self._executor.alive

    def shutdown(self):
        self._executor.shutdown()
