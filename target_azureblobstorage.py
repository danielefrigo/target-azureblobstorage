#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

from azure.storage.blob import BlockBlobService, AppendBlobService

logger = singer.get_logger()
USER_HOME = os.path.expanduser('~')


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def persist_lines(block_blob_service, append_blob_service, blob_container_name, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # blobs = block_blob_service.list_blobs(blob_container_name)
    # blob_names = [blob.name for blob in list(blobs)]
    parent_dir = os.path.join(USER_HOME, blob_container_name)

    # reads first line to get filename
    for line in lines:
        o = json.loads(line)
        filename = o['stream'] + '.json'
        logger.info(f"Writing temp stream file in {parent_dir}")
        logger.info(f"Processing stream on file {filename}")
        stream_path = os.path.join(parent_dir, filename)
        file_obj = open(stream_path, "w+")
        pass

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            logger.debug('schema for this records stream {}'.format(schema))
            logger.debug('Validate record {}'.format(o))
            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])
            file_obj.write(json.dumps(o['record']) + ',')

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']

            # if currently_syncing == NONE upload file
            if not state['currently_syncing'] and os.path.exists(parent_dir):
                for _file in os.listdir(parent_dir):

                    file_path = os.path.join(parent_dir, _file)

                    block_blob_service.create_blob_from_path(
                        blob_container_name,
                        filename,
                        file_path,
                        content_settings=ContentSettings(
                            content_type='application/JSON')
                    )
                    os.remove(file_path)

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        elif t == 'ACTIVATE_VERSION':
            logger.debug("Type {} in message {}"
                         .format(o['type'], o))
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    file_obj.close()

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-azureblobstorage').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-azureblobstorage',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    block_blob_service = BlockBlobService(config.get('account_name', None), config.get('account_key', None))

    append_blob_service = AppendBlobService(config.get('account_name', None), config.get('account_key', None))

    blob_container_name = config.get('container_name', None)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(block_blob_service, append_blob_service, blob_container_name, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
