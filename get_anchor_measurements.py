import json.decoder
import logging
import sys
from datetime import datetime, timezone
from typing import Tuple

import requests


API_BASE = 'https://atlas.ripe.net/api/v2/'
OUTPUT_DIR = 'parsed/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_DELIMITER = ','
OUTPUT_SUFFIX = '.csv'
PAGE_SIZE = 500
# Even though a page size of 500 should contain 500 entries,
# requesting 500 results generates a "next" link that leads to an
# empty page, so we only request 499 results.
CHUNK_SIZE = 499


def process_response(response: requests.Response) -> Tuple[str, list]:
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
    return next_url, data['results']


def execute_query(url: str) -> Tuple[str, list]:
    logging.info(f'Querying {url}')
    r = requests.get(url)
    return process_response(r)


def write_data(data: list, name: str) -> None:
    if not data:
        return
    data.sort()
    output_name = f'{datetime.now(tz=timezone.utc).strftime(OUTPUT_FMT)}-' \
                  f'{name}{OUTPUT_SUFFIX}'
    output_file = OUTPUT_DIR + output_name
    logging.info(f'Writing {len(data)} measurements to {output_file}')
    output_lines = [str(entry) + '\n' for entry in data]
    with open(output_file, 'w') as f:
        f.writelines(output_lines)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    # Get anchor measurements. These only include the type and if the
    # measurement is a mesh or probe measurement.
    endpoint = API_BASE + 'anchor-measurements/'
    logging.info(f'Querying Atlas API {endpoint}')
    params = {'format': 'json', 'page_size': PAGE_SIZE}
    r = requests.get(endpoint, params)
    next_url, anchor_msm_data = process_response(r)
    while next_url:
        next_url, next_data = execute_query(next_url)
        anchor_msm_data += next_data
        logging.info(f'Added {len(next_data)} measurements. Total: {len(anchor_msm_data)}')
    # Keep only traceroute measurements.
    tr_measurements = [entry
                       for entry in anchor_msm_data if entry['type'] == 'traceroute']
    logging.info(f'Got {len(tr_measurements)} traceroute measurements.')
    # Get all measurement ids and subsets for mesh and non-mesh (probes)
    # measurements.
    msm_ids = [entry['measurement'].split('/')[6] for entry in tr_measurements]
    mesh_msm_ids = {int(entry['measurement'].split('/')[6])
                    for entry in tr_measurements if entry['is_mesh']}
    probes_msm_ids = {int(entry['measurement'].split('/')[6])
                      for entry in tr_measurements if not entry['is_mesh']}

    # Get measurement details to distinguish between IPv4 and IPv6 measurements.
    params = {'format': 'json', 'page_size': PAGE_SIZE}
    endpoint = API_BASE + 'measurements/'
    logging.info(f'Querying Atlas API {endpoint}')
    msm_data = list()
    chunks = len(msm_ids) // CHUNK_SIZE
    if len(msm_ids) % CHUNK_SIZE:
        chunks += 1
    for chunk_no in range(chunks):
        msm_id_chunk = msm_ids[chunk_no * CHUNK_SIZE: (chunk_no + 1) * CHUNK_SIZE]
        params['id__in'] = ','.join(msm_id_chunk)
        r = requests.get(endpoint, params)
        next_url, next_data = process_response(r)
        if next_url:
            logging.warning('Result did not fit on one page.')
        msm_data += next_data
        logging.info(f'Added {len(next_data)} measurements. Total: {len(msm_data)}')
    ipv4_msm_ids = {entry['id'] for entry in msm_data if entry['af'] == 4}
    ipv6_msm_ids = {entry['id'] for entry in msm_data if entry['af'] == 6}
    write_data(list(mesh_msm_ids.intersection(ipv4_msm_ids)), 'mesh-ipv4-msm')
    write_data(list(mesh_msm_ids.intersection(ipv6_msm_ids)), 'mesh-ipv6-msm')
    write_data(list(probes_msm_ids.intersection(ipv4_msm_ids)), 'probes-ipv4-msm')
    write_data(list(probes_msm_ids.intersection(ipv6_msm_ids)), 'probes-ipv6-msm')


if __name__ == '__main__':
    main()
    sys.exit(0)
