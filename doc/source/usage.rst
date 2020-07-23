========
Usage
========

You can find examples in ``etcd3gw/examples`` and look at ``etcd3gw/client.py``.

Basic usage example::

    from etcd3gw.client import Etcd3Client
 
    client = Etcd3Client(host='localhost', port=2379)

    # Put key
    client.put(key='foo', value='bar')

    # Get key
    client.get(key='foo')

    # Get all keys
    client.get_all()


    # Create lease and use it
    lease = client.lease(ttl=100)

    client.put(key='foo', value='bar', lease=lease)

    # Get lease keys
    lease.keys()

    # Refresh lease
    lease.refresh()


    # Use watch
    watcher, watch_cancel = client.watch(key='KEY')

    for event in watcher: # blocks until event comes, cancel via watch_cancel()
        print(event)
        # modify event: {u'kv': {u'mod_revision': u'8', u'version': u'3', u'value': 'NEW_VAL', u'create_revision': u'2', u'key': 'KEY', u'lease': u'7587847878767953426'}}
