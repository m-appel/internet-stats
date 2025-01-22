import bz2
import json.decoder
import logging
import sys
from datetime import datetime, timezone

import msgpack
import requests

sys.path.append('../')
from file_handlers.common import make_symlink

API_BASE = 'https://atlas.ripe.net/api/v2/'
OUTPUT_DIR = '../raw/atlas/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_SUFFIX = '-probes.msgpack.bz2'


def process_response(response: requests.Response) -> (str, list):
    if response.status_code != requests.codes.ok:
        logging.error(f'Request to {response.url} failed with status: '
                      f'{response.status_code}')
        return str(), list()
    try:
        data = response.json()
    except json.decoder.JSONDecodeError as e:
        logging.error(f'Decoding JSON reply from {response.url} failed with '
                      f'exception: {e}')
        return str(), list()
    if 'next' not in data or 'results' not in data:
        logging.error('"next" or "results" key missing from response data.')
        return str(), list()
    next_url = data['next']
    if not next_url:
        logging.info('Reached end of list')
        next_url = str()
    ret = list()
    for res in data['results']:
        if 'tags' in res:
            # Not interested in this and uses comparatively lots of
            # space.
            res.pop('tags')
        ret.append(res)
    return next_url, ret


def execute_query(url: str) -> (str, list):
    logging.info(f'Querying {url}')
    r = requests.get(url)
    return process_response(r)


def write_data(data: list) -> None:
    if not data:
        return
    output_name = datetime.now(tz=timezone.utc).strftime(OUTPUT_FMT) \
                  + OUTPUT_SUFFIX
    output_file = OUTPUT_DIR + output_name
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    logging.info(f'Writing {len(data)} probes to {output_file}')
    with bz2.open(output_file, 'wb') as f:
        msgpack.dump(data, f)
    make_symlink(output_name, latest_symlink)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        filename='../logs/get_probe_snapshot.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    probe_endpoint = API_BASE + 'probes/'
    logging.info(f'Querying Atlas API {probe_endpoint}')
    params = {'format': 'json', 'status': 1, 'page_size': 500}
    r = requests.get(probe_endpoint, params)
    next_url, data = process_response(r)
    while next_url:
        next_url, next_data = execute_query(next_url)
        data += next_data
        logging.info(f'Added {len(next_data)} probes. Total: {len(data)}')
    write_data(data)


if __name__ == '__main__':
    main()
