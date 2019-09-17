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


class Etcd3Exception(Exception):
    def __init__(self, detail_text=None, *args):
        super(Etcd3Exception, self).__init__(*args)
        self.detail_text = detail_text


class WatchTimedOut(Etcd3Exception):
    pass


class InternalServerError(Etcd3Exception):
    pass


class ConnectionFailedError(Etcd3Exception):
    pass


class ConnectionTimeoutError(Etcd3Exception):
    pass


class PreconditionFailedError(Etcd3Exception):
    pass
