#!/usr/bin/env python3

import argparse
import io
import os
import shutil
import gzip
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

from azure.storage.blob import BlockBlobService, AppendBlobService, ContentSettings

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


def persist_lines(block_blob_service, blob_container_name, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    parent_dir = os.path.join(USER_HOME, blob_container_name)
    # clean temp folder for local file creation
    shutil.rmtree(parent_dir, ignore_errors=True)
    os.mkdir(parent_dir)

    # Loop over lines from stdin
    for line in lines:
        try:
            line_json = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in line_json:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = line_json['type']

        if t == 'RECORD':
            if 'stream' not in line_json:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if line_json['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(line_json['stream']))

            # Get schema for this record's stream
            schema = schemas[line_json['stream']]

            logger.debug('schema for this records stream {}'.format(schema))
            logger.debug('Validate record {}'.format(line_json))
            # Validate record
            validators[line_json['stream']].validate(line_json['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])
            filename = line_json['stream'] + '.json'
            stream_path = os.path.join(parent_dir, filename)
            with open(stream_path, "a") as file_obj:
                # todo usare json.dump al posto di dumps
                #file_obj.write(json.dumps(line_json['record']) + ',\n')
                #json.dump(line_json, file_obj)
                file_obj.write(f'{line},\n')

            state = None

        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(line_json['value']))
            state = line_json['value']

            if not state['currently_syncing'] and os.path.exists(parent_dir):
                for _file in os.listdir(parent_dir):
                    output_file_name = now + ".gz"
                    file_path_in = os.path.join(parent_dir, _file)
                    file_path_out = os.path.join(parent_dir, output_file_name)
                    with open(file_path_in, 'rb') as f_in:
                        with gzip.open(file_path_out, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out, length=1024*1024)
                    block_blob_service.create_blob_from_path(
                        blob_container_name,
                        _file.replace(".json", "") + "/" + output_file_name,
                        file_path_out,
                        content_settings=ContentSettings(
                            content_type='application/JSON')
                    )
                    os.remove(file_path_in)
                    os.remove(file_path_out)

        elif t == 'SCHEMA':
            if 'stream' not in line_json:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = line_json['stream']
            schemas[stream] = line_json['schema']
            validators[stream] = Draft4Validator(line_json['schema'])
            if 'key_properties' not in line_json:
                raise Exception("key_properties field is required")
            key_properties[stream] = line_json['key_properties']

        elif t == 'ACTIVATE_VERSION':
            logger.debug("Type {} in message {}".format(line_json['type'], line_json))

        else:
            raise Exception("Unknown message type {} in message {}".format(line_json['type'], line_json))

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

    blob_container_name = config.get('container_name', None)
    buffer = config.get('buffer', 1)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(block_blob_service, blob_container_name, input, buffer)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
