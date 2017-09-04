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

from etcd3gw.client import Etcd3Client
from etcd3gw.tests import base


class TestEtcd3Gateway(base.TestCase):

    def test_client_default(self):
        client = Etcd3Client()
        self.assertEqual("http://localhost:2379/v3alpha/lease/grant",
                         client.get_url("/lease/grant"))

    def test_client_ipv4(self):
        client = Etcd3Client(host="127.0.0.1")
        self.assertEqual("http://127.0.0.1:2379/v3alpha/lease/grant",
                         client.get_url("/lease/grant"))

    def test_client_ipv6(self):
        client = Etcd3Client(host="::1")
        self.assertEqual("http://[::1]:2379/v3alpha/lease/grant",
                         client.get_url("/lease/grant"))
