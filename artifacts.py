#!/usr/bin/env python

import sys
sys.path.append("boto-2.0b4-py2.6-egg")

import boto
from boto.s3.key import Key
import optparse
import configobj
from os.path import basename

def print_progress(current, total):
    print "%s%s (%s/%s)\r" % ('#' * int(current/total*10), ' ' *
            int((total-current)/total*10), current, total)

class Uploader(object):
    def __init__(self, bucket, access_key, secret_key):
        s3 = boto.connect_s3(access_key, secret_key)
        self.bucket = s3.get_bucket(bucket)

    def upload(self, filename, product, environment=None, version=None):
        k = Key(self.bucket)
        k.key = product
        if environment:
            k.key = "%s/%s" % (k.key, environment)
        k.key = "%s/%s" % (k.key, basename(filename))
        if version:
            k.set_metadata("version", version)
        cb = None
        if sys.stdout.isatty():
            cb = print_progress
        k.set_contents_from_filename(filename, cb=cb, reduced_redundancy=True)

class Downloader(object):
    def __init__(self, bucket, access_key, secret_key):
        s3 = boto.connect_s3(access_key, secret_key)
        self.bucket = s3.get_bucket(bucket)

    def get_versions(self, filename, product, environment=None):
        prefix = product
        if environment:
            prefix = "%s/%s" % (prefix, environment)
        prefix = "%s/%s" % (prefix, filename)
        keys = self.bucket.get_all_versions(prefix=prefix)
        return [ self.bucket.get_key(k.name, version_id=k.version_id) for k in keys ]

def construct_optparse():
    parser = optparse.OptionParser("usage: %prog [options] product filename [filename ...]")
    parser.add_option("-c", "--config", dest="config",
            default="artifacts.ini", type="string",
            metavar="FILE", help="read config from FILE")
    parser.add_option("-b", "--bucket", dest="bucket",
            type="string",
            metavar="BUCKET", help="upload to S3 bucket BUCKET")
    parser.add_option("", "--access-key", dest="access_key",
            type="string",
            help="S3 access key")
    parser.add_option("", "--secret-key", dest="secret_key",
            type="string",
            help="S3 secret key")
    parser.add_option("-v", "--version", dest="version",
            type="string",
            help="version to label artifact")
    parser.add_option("-e", "--env", dest="environment",
            type="string", metavar="ENV",
            help="upload artifact under environment ENV")
    return parser

def run_optparse(parser):
    (options, args) = parser.parse_args()
    if len(args) < 2:
        parser.error("incorrect number of arguments")

    config = configobj.ConfigObj(options.config)

    for option in ['bucket', 'access_key', 'secret_key', 'version', 'environment']:
        if getattr(options, option): config[option] = getattr(options, option)

    config['product'] = args[0]
    args = args[1:]

    for option in ['bucket', 'access_key', 'secret_key', 'product']:
        if not config[option]:
            print "Missing %s" % option
            sys.exit(1)

    return config
