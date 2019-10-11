#!/usr/bin/env python
# encoding: utf-8

'''
Python script to sync the contents of one S3 user with another S3 user.
This script will work even if the buckets are in different cluster.
'''

from boto.exception import S3ResponseError
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from Queue import LifoQueue
import threading
import time
import logging
import os
import argparse
import json

# Log everything, and send it to stderr.
logging.basicConfig(level=logging.WARN)

def sync_acl(f,t):
    acl = f.get_acl()
    t.set_acl(acl)

def sync_policy(f,t):
    try:
        policy = f.get_policy()
    except S3ResponseError:
        logging.exception('None policy %s' % f.name)
        return
    t.set_policy(policy)

def sync_cors(f,t):
    try:
        cors = f.get_cors()
    except S3ResponseError:
        logging.exception('None CORS %s' % f.name)
        return
    t.set_cors(cors)

def sync_tags(f,t):
    tags = f.get_tags()
    if tags:
        t.set_tags(tags)

def sync_lifecycle(f,t):
    try:
        lc = f.get_lifecycle_config()
    except S3ResponseError:
        logging.exception('None LC %s' % f.name)
        return
    t.configure_lifecycle(lc)

def sync_versioning(f,t):
    v = f.get_versioning_status()
    t.configure_versioning(v)


class S3Connector(S3Connection):
    def __init__(self, name, addr, port=7480, access=None, secret=None, https=False, cformat=None):
        self.name = name
        self.calling_format = OrdinaryCallingFormat()
        if cformat != None:
            self.calling_format = cformat
        super(S3Connector, self).__init__(access, secret, https, port, host=addr, calling_format=self.calling_format)
        # S3Connection.__init__(self, access, secret, https, port, host=addr, calling_format=self.calling_format)

def sync_obj_attrs(f,t):
    # acl
    sync_acl(f,t)

class ObjWorker(threading.Thread):
    def __init__(self, thread_id, queue, from_connector, from_bucket, to_connector, to_bucket, overwrite=False, verbose=False):
        super(ObjWorker, self).__init__()
        self.thread_id = thread_id
        self.queue = queue
        self.from_connector = from_connector
        self.from_bucket = from_bucket
        self.to_connector = to_connector
        self.to_bucket = to_bucket
        self.verbose_enable = verbose
        self.overwrite = overwrite

    def run(self):
        while True:
            try:
                key_name = self.queue.get()
                from_key = self.from_bucket.get_key(key_name)
                to_key = self.to_bucket.get_key(from_key.key)
                # across cluster copy object
                # Judge key exists
                # Judge diffrent etag and allow overwrite
                if (not to_key and not to_key.exists()) or (self.overwrite and from_key.etag != to_key.etag) :
                    from_key.get_contents_to_filename(from_key.name)
                    to_key = self.to_bucket.new_key(from_key.name)
                    to_key.set_contents_from_filename(from_key.name)
                    if os.path.exists(from_key.name):
                        os.remove(from_key.name)
                    print '  t%s: Copy: %s' % (self.thread_id, from_key.key)
                else:
                    if self.verbose_enable:
                        print '  t%s: Exists and etag matches: %s' % (self.thread_id, to_key)
            except BaseException:
                logging.exception('  t%s: error during copy' % self.thread_id)

            # Set object's ACL
            # Don't sync ACL , diffrent key's etag and not allow overwrite
            if not ( not self.overwrite and from_key.etag != to_key.etag ):
                sync_obj_attrs(from_key, to_key)
            # Phase done
            self.queue.task_done()

def copy_objects(from_conn, to_conn, from_bkt, to_bkt, thd_count, overwrite=False, verbose=False):
    max_keys = 1000
    result_marker = ''
    q = LifoQueue(maxsize=5000)

    # Dispatch tasks to ObjWorker from LifoQueue
    for i in xrange(thd_count):
        t = ObjWorker(i, q, from_conn, from_bkt, to_conn, to_bkt, overwrite, verbose)
        t.daemon = True
        t.start()

    # Add tasks to LifoQueue
    i = 0
    while True:
        try:
            keys = from_bkt.get_all_keys(max_keys=max_keys, marker=result_marker)
            if len(keys) == 0:
                break
            for i_key in keys:
                i += 1
                q.put(i_key.key)
            if len(keys) < max_keys:
                break
            result_marker = keys[max_keys - 1].key
            while q.qsize() > (q.maxsize - max_keys):
                time.sleep(1)
        except BaseException:
            logging.exception('error during fetch, quit')
            break
    q.join()

def sync_bucket_attrs(f,t):
    # acl
    sync_acl(f,t)
    # policy
    sync_policy(f,t)
    # cors
    sync_cors(f,t)
    # tags
    sync_tags(f,t)
    # lifecycle
    sync_lifecycle(f,t)
    # versioning
    sync_versioning(f,t)

def copy_buckets(from_user_connector = None, to_user_connector = None, thread_count=1, overwrite=False, verbose=False, bmap=None):
    if from_user_connector == None or to_user_connector == None:
        return
    from_conn = from_user_connector
    to_conn = to_user_connector
    # For each bucket via 'from_conn'
    for item_frm in from_conn.get_all_buckets():
        if bmap != None and bmap.get(item_frm.name, None) != None:
            to_bkt_name = bmap.get(item_frm.name)
        else:
            to_bkt_name = item_frm.name
        lookup_to_bkt = to_conn.lookup(to_bkt_name)
        if lookup_to_bkt == None:
            # Create bucket via 'to_conn'
            to_conn.create_bucket(to_bkt_name)
        item_to = to_conn.get_bucket(to_bkt_name)
        # If to_bucket is exists, don't overwrite bucket's attrs.
        # If overwrite == False, don't overwrite bucket's attrs.
        # Else only sync objects
        if overwrite or lookup_to_bkt == None:
            # sync bucket attrs
            sync_bucket_attrs(item_frm, item_to)
        # sync objects
        copy_objects(from_conn, to_conn, item_frm, item_to, thread_count, overwrite, verbose)

def sync_users(users_config = None, thread_count=1, allow_ow=False, verbose=False):
    users_info = None
    if os.path.exists(users_config):
        f = open(users_config)
        users_info = json.load(f)
        
    if users_info != None:
        for user in users_info:
            inf_from = user.get('from')
            inf_to = user.get('to')
            bkt_map = user.get('bucketmap', None)

            from_host = inf_from.get('host')
            from_port = inf_from.get('port')
            from_a_key = inf_from.get('access')
            from_s_key = inf_from.get('secret')

            to_host = inf_to.get('host')
            to_port = inf_to.get('port')
            to_a_key = inf_to.get('access')
            to_s_key = inf_to.get('secret')

            from_conn = S3Connector("From Connector", from_host, from_port, access=from_a_key, secret=from_s_key)
            to_conn = S3Connector("To Connector", to_host, to_port, access=to_a_key, secret=to_s_key)

            copy_buckets(from_conn, to_conn, thread_count, allow_ow, verbose, bmap=bkt_map)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = "Sync user to user via S3")
    parser.add_argument('-c', '--config', 
                        help="users.json Configuration file.",
                        required = False,
                        default = 'users.json')
    parser.add_argument('-v', '--verbose',
                        help="Verbose output",
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-w', '--overwrite',
                        help = 'overwrite bucket\'s attributes and objects with the same key',
                        dest='overwrite',
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-t', '--threadcount',
                        help='Number of worker threads processing the objects',
                        dest='thread_count',
                        required = False,
                        type = int,
                        default = 1)
    args = parser.parse_args()

    config = args.config
    thd_count = args.thread_count
    verbose = args.verbose
    overwrite = args.overwrite

    sync_users(config, thd_count, overwrite, verbose)

