#!/usr/bin/env python

import artifacts

parser = artifacts.construct_optparse()
config, args = artifacts.run_optparse(parser)

d = artifacts.Downloader(config['bucket'], config['access_key'], config['secret_key'])
for f in args:
    vs = d.get_versions(f, config['product'], config.get('environment', None))
    for v in vs:
        print "%s (%s) at %s" % (v.name, v.get_metadata('version'),
                v.last_modified)
