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

from etcd3gw.utils import _decode


class Lease(object):
    def __init__(self, id, client=None):
        """Lease object for expiring keys

        :param id:
        :param client:
        """
        self.id = id
        self.client = client

    def revoke(self):
        """LeaseRevoke revokes a lease.

        All keys attached to the lease will expire and be deleted.
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.

        :return:
        """
        self.client.post(self.client.get_url("/kv/lease/revoke"),
                         json={"ID": self.id})
        return True

    def ttl(self):
        """LeaseTimeToLive retrieves lease information.

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.

        :return:
        """
        result = self.client.post(self.client.get_url("/kv/lease/timetolive"),
                                  json={"ID": self.id})
        return int(result['TTL'])

    def refresh(self):
        """LeaseKeepAlive keeps the lease alive

        By streaming keep alive requests from the client to the server and
        streaming keep alive responses from the server to the client.
        This method makes a synchronous HTTP request by default.

        :return:
        """
        result = self.client.post(self.client.get_url("/lease/keepalive"),
                                  json={"ID": self.id})
        return int(result['result']['TTL'])

    def keys(self):
        """Get the keys associated with this lease.

        :return:
        """
        result = self.client.post(self.client.get_url("/kv/lease/timetolive"),
                                  json={"ID": self.id,
                                        "keys": True})
        keys = result['keys'] if 'keys' in result else []
        return [_decode(key) for key in keys]
