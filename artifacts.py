#!/usr/bin/env python

import sys
import logging
import os
import fcntl
import termios
import struct
import base64

log = logging.getLogger(__name__)

sys.path.append("%s/boto.egg" % os.path.abspath(os.path.dirname(__file__)))
import boto


def __print_progress__(current, total):
    '''Print a progress bar if on a terminal'''
    PROGRESS_WIDTH = int(0.15 * (struct.unpack('hh', fcntl.ioctl(sys.stdin,
                                            termios.TIOCGWINSZ, '1234'))[1]))
    # print a progress bar of how complete the download is
    if total > 0:
        done_string = '#' * int((1.0 * current / total * PROGRESS_WIDTH))
        notdone_string = ' ' * int(1.0 * (total - current) / total *
                                   PROGRESS_WIDTH)
        sys.stdout.write("\r[%s%s] (%.1f%% of %iKb)" % (done_string,
                        notdone_string, (1.0 * current / total) * 100,
                                                        total / 1024))


class S3Artifacts(object):
    '''Artifacts backend that uses S3 for storage'''

    def __init__(self, bucket, access_key, secret_key, prefix=None):
        s3 = boto.connect_s3(access_key, secret_key)
        self.bucket = s3.get_bucket(bucket)

    def _get_path(self, product, section, filename=None):
        prefix = "%s/%s" % (self.prefix, product)
        if section:
            prefix = "%s/%s" % (prefix, section)
        if filename:
            return "%s/%s" % (prefix, filename)
        return prefix

    def upload(self, filename, product, section=None, version=None,
               target=None, quiet=False):
        k = boto.s3.key.Key(self.bucket)

        if not target:
            target = os.path.basename(filename)

        k.key = self._get_path(product, section, target)

        if version:
            k.set_metadata("version", version)

        cb = None
        if sys.stdout.isatty() and not quiet:
            cb = __print_progress__

        k.set_contents_from_filename(filename, cb=cb, num_cb=-1,
                                     reduced_redundancy=True)
        if cb:
            sys.stdout.write("\n")

    def download(self, filename, product, section=None, version=None,
                 target=None, quiet=False):
        if version:
            version = base64.b64decode(version).strip()

        path = self._get_path(product, section, filename)

        if not target:
            target = os.path.basename(filename)

        cb = None
        if sys.stdout.isatty() and not quiet:
            cb = __print_progress__

        k = self.bucket.get_key(path, version_id=version)
        k.get_contents_to_filename(target, cb=cb, num_cb=-1,
                                   version_id=version)
        if cb:
            sys.stdout.write("\n")

    def get_versions(self, filename, product, section=None):
        path = self._get_path(product, section, filename)
        keys = self.bucket.get_all_versions(prefix=path)
        return [self.bucket.get_key(k.name, version_id=k.version_id)
                    for k in keys if isinstance(k, boto.s3.key.Key)]

    def get_listing(self, product, section=None, limit=25):
        '''
        Get file listing for a product and environment
        Takes the directory tree form of 'product/[env]/filename'
        '''
        files = []
        prefix = self._get_path(product, section)
        for k in self.bucket.list(delimiter='/', prefix=prefix):
            if isinstance(k, boto.s3.key.Key):
                files.append(k.name[len(prefix):])
            if isinstance(k, boto.s3.prefix.Prefix):
                files.append(k.name[len(prefix):])
        return files

