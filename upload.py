#!/usr/bin/env python

import artifacts

parser = artifacts.construct_optparse()
config, args = artifacts.run_optparse(parser)

u = artifacts.Uploader(config['bucket'], config['access_key'], config['secret_key'])
for f in args:
    u.upload(f, config['product'], config.get('environment', None),
            config.get('version', None))

