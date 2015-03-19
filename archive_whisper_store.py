#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# archive_whisper_store.py: Backup Whisper Store to a Gzipped Tar Archive.
#
# Author: Lior Goikhburg <goikhburg at gmail.com >
#
# Set the WHISPER_LOCK_WRITES = True in carbon.conf for consistent backups

import argparse
import fcntl
import tarfile
import logging
import os
import signal
import sys


def list_files(storage_dir):
    storage_dir = storage_dir.rstrip(os.sep)
    for root, dirnames, filenames in os.walk(storage_dir):
        for filename in filenames:
            if filename.endswith('.wsp'):
                yield os.path.join(root, filename)


def main():
    tar = None
    def_sigint_handler = signal.getsignal(signal.SIGINT)


    def sigint_handler(ignum, frame):
        signal.signal(signal.SIGINT, def_sigint_handler)
        logger.debug('SIGINT encountered, exiting...')
        if tar:
            tar.close()
        sys.exit()


    signal.signal(signal.SIGINT, sigint_handler)

    logger.info("Creating tar file: %s" % args.tar_file)
    try:
        tar = tarfile.open(args.tar_file, 'w:gz')
    except Exception as error:
        logger.error("Cannot create archive file %s: %s" % (args.tar_file, str(error)))
        sys.exit(1)

    for source_file in list_files(args.whisper_dir):
        logger.debug("Locking file: %s" % source_file)
        try:
            sh = open(source_file, 'rb')
            fcntl.flock(sh.fileno(), fcntl.LOCK_EX)
            logger.debug("File %s was successfully locked." % source_file)
        except IOError as error:
            logger.warning("An Error occured locking file %s: %s" % (source_file, str(error)))
            continue
        except Exception as error:
            logger.error("Unknown Error occured: %s" % str(error))
            sys.exit(1)

        source_file_rel_path = os.path.relpath(source_file, args.whisper_dir)
        logger.info("Adding %s to tar." % source_file)
        try:
            tarinfo = tar.gettarinfo(arcname=source_file_rel_path, fileobj=sh)
            tar.addfile(tarinfo, fileobj=sh)
            logger.debug("File %s was successfully added to tar." % source_file)
        except Exception as error:
            logger.error("An Error occured archiving file %s: %s" % (source_file, str(error)))
            sys.exit(1)

        sh.close()

    tar.close()

if __name__ == "__main__":
    PROGRAM_NAME = os.path.basename(__file__)
    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        description='Backup Whisper Store to a Gzipped Tar Archive.',
        epilog="Example: %(prog)s -v -s /var/lib/graphite/whisper -t /storage/backup/whisper.tar.gz"
    )
    parser.add_argument('-w', '--whisper-dir', type=str, required=True, help='Whisper Store Directory')
    parser.add_argument('-t', '--tar-file', type=str, required=True, help='Destination Tar File')
    parser.add_argument('-v', '--verbose', default=0, help='Verbose', action='count')
    args = parser.parse_args()

    LogLevel = logging.WARNING
    if args.verbose:
        LogLevel = logging.INFO
    if args.verbose >= 2:
        LogLevel = logging.DEBUG

    logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s', datefmt='%F %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))
    logger.setLevel(LogLevel)

    main()
