#!/usr/bin/env python

import logging
import artifacts

log = logging.getLogger('artifacts')
log.addHandler(logging.StreamHandler(sys.stdout))

parser = artifacts.construct_optparse()
config, args = artifacts.run_optparse(parser)

if not config.get('quiet', False): log.setLevel(logging.INFO)

u = artifacts.Uploader(config['bucket'], config['access_key'], config['secret_key'])
for f in args:
    log.info("Uploading %s to %s:", f, "%s%s" % (config['product'], ["","/%s" % config.get('environment','')][config.has_key('environment')]))
    u.upload(f, config['product'], config.get('environment', None), config.get('version', None), config.get('target', None), config.get('quiet', False))
    log.info("Done.")
