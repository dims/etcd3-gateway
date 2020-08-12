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
import sys

import futurist
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
    :return: decoded data
    """
    if not isinstance(data, bytes_types):
        data = six.b(str(data))
    return base64.b64decode(data.decode("utf-8"))


def _increment_last_byte(data):
    """Get the last byte in the array and increment it

    :param bytes_string:
    :return:
    """
    if not isinstance(data, bytes_types):
        if isinstance(data, six.string_types):
            data = data.encode('utf-8')
        else:
            data = six.b(str(data))
    s = bytearray(data)
    s[-1] = s[-1] + 1
    return bytes(s)


DEFAULT_TIMEOUT = 30
LOCK_PREFIX = '/locks/'


def _import_module(import_str):
    """Import a module."""
    __import__(import_str)
    return sys.modules[import_str]


def _try_import(import_str, default=None):
    """Try to import a module and if it fails return default."""
    try:
        return _import_module(import_str)
    except ImportError:
        return default


# These may or may not exist; so carefully import them if we can...
_eventlet = _try_import('eventlet')
_patcher = _try_import('eventlet.patcher')


def _get_threadpool_executor():
    if all((_eventlet, _patcher)) and _patcher.is_monkey_patched('thread'):
        return futurist.GreenThreadPoolExecutor
    return futurist.ThreadPoolExecutor
