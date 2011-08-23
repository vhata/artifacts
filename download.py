#!/usr/bin/env python

import base64
import logging
import sys

import artifacts

log = logging.getLogger('artifacts')
log.addHandler(logging.StreamHandler(sys.stdout))

parser = artifacts.construct_optparse()
parser.add_option("-l", "--list", dest="list", action="store_true", help="list versions of artifacts", default=False)
config, args = artifacts.run_optparse(parser)

if not config.get('quiet', False): log.setLevel(logging.INFO)

d = artifacts.Downloader(config['bucket'], config['access_key'], config['secret_key'])

if config.get('list', False):
    for f in args:
        vs = d.get_versions(f, config['product'], config.get('environment', None))
        for v in vs:
            print "%s : %s (external version %s) at %s" % (base64.b64encode(v.version_id+" "*((3-len(v.version_id)%3)%3)), v.name, v.get_metadata('version'),
                v.last_modified)
else:
    for f in args:
        log.info("Downloading %s from %s:", f, "%s%s" % (config['product'], ["","/%s" % config.get('environment','')][config.has_key('environment')]))
        d.download(f, config['product'], config.get('environment', None), config.get('version', None), config.get('target', None), config.get('quiet', False))
        log.info("Done.")

