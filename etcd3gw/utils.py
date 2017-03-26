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

import six

bytes_types = (bytes, bytearray)


def _encode(data):
    """Encode the given data using base-64

    :param data:
    :return: base-64 encoded string
    """
    if not isinstance(data, bytes_types):
        data = six.b(str(data))
    return base64.b64encode(data).decode("utf-8")


def _decode(data):
    """Decode the base-64 encoded string

    :param data:
    :return: decoded string
    """
    if not isinstance(data, bytes_types):
        data = six.b(str(data))
    return base64.b64decode(data).decode("utf-8")


DEFAULT_TIMEOUT = 30
LOCK_PREFIX = '/locks/'
