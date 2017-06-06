#!/usr/bin/env python

import base64
import logging
from optparse import OptionParser
import sys
import artifacts


log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


def main():
    usage = "usage: %prog [ options ] COMMAND product [ filename ... ]"
    description = "Operate on artifacts stored in a product bucket.  " + \
        "Valid commands are 'get', 'put' and 'list'.  You may optionally " + \
        "store the objects in a separate section of the bucket for " + \
        "classification (e.g. for different environments)."
    parser = OptionParser(usage=usage, description=description,
                          version=None, target=None)
    parser.set_defaults(quiet=False, prefix='', section='')
    parser.add_option("-c", "--config", dest="config",
            default="artifacts.ini",
            metavar="FILE", help="read config from FILE")
    parser.add_option("-b", "--bucket", dest="bucket",
            metavar="BUCKET", help="use S3 bucket BUCKET")
    parser.add_option("-p", "--prefix", dest="prefix",
            metavar="PREFIX", help="Store artifacts prefixed with path PREFIX")
    parser.add_option("", "--access-key", dest="access_key",
            help="S3 access key")
    parser.add_option("", "--secret-key", dest="secret_key",
            help="S3 secret key")
    parser.add_option("-v", "--version", dest="version",
            help="specify artifact version")
    parser.add_option("-s", "--section", dest="section", metavar="SECTION",
            help="Store objects in subsection SECTION of the product bucket.")
    parser.add_option("-t", "--target", dest="target", metavar="FILENAME",
            help="Use FILENAME for destination")
    parser.add_option("-q", "--quiet", dest="quiet",
            action="store_true", help="execute quietly")

    (options, args) = parser.parse_args()

    config = configobj.ConfigObj(options.config)

    for option in parser.defaults.keys():
        if getattr(options, option):
            config[option] = getattr(options, option)

    missing_opts = []
    for option in ['bucket', 'access_key', 'secret_key', 'product']:
        if not option in config:
            missing_opts.append(option)
    if missing_opts:
        log.error("Missing configuration: %s", ", ".join(missing_opts))
        sys.exit(1)

    if len(args) < 1:
        log.error("No product specified.  A product is a grouping of " +
                  "artifacts in your artifact store.")
        sys.exit(1)

    config['product'] = args[0]
    args = args[1:]

    if len(args) < 1:
        log.error("Must specify a command: get, put or list")
        sys.exit(1)

    command = args[0].lower()
    args = args[1:]

    if command != 'list':
        if len(args) < 1:
            log.error("You must specify filenames for 'get' or 'put'")
            sys.exit(1)
        if config.get('target') and len(args) > 1:
            log.error("Cannot specify a target filename when %sting " +
                    "multiple files", command)
            sys.exit(1)

    if not config['quiet']:
        log.setLevel(logging.INFO)

    a = artifacts.S3Artifacts(config['bucket'], config['access_key'],
                              config['secret_key'], config['prefix'])
    if command == 'put':
        for f in args:
            log.info("Uploading %s.", f)
            a.upload(f, config['product'], config['section'],
                     config['version'], config['target'], config['quiet'])

    if command == 'list':
        for f in args:
            vs = a.get_versions(f, config['product'], config['section'])
            for v in vs:
                print "%s : %s (external version %s) at %s" % (base64.b64encode(v.version_id+" "*((3-len(v.version_id)%3)%3)), v.name, v.get_metadata('version'),
                    v.last_modified)

    if command == 'put':
        for f in args:
            log.info("Downloading %s.", f)
            a.download(f, config['product'], config['section'], config['version'], config['target'], config['quiet'])

