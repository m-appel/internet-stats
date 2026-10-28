import bz2
import logging
import pickle
import sys
from datetime import UTC, datetime

import requests

sys.path.append('../')
from file_handlers.common import make_symlink

API_BASE = 'https://peeringdb.com/api/'
OUTPUT_DIR = '../raw/peeringdb/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_SUFFIX = '-peeringdb-netixlan.pickle.bz2'


def query_pdb(endpoint: str, params=None) -> dict:
    """Query the PeeringDB API with the specified endpoint and
    parameters and return the response JSON data as a dictionary.
    """
    if params is None:
        params = dict()
    url = API_BASE + endpoint
    # Always query only 'ok' entries
    params['status'] = 'ok'
    logging.info(f'Querying PeeringDB {url} with params {params}')
    try:
        r = requests.get(url, params)
    except ConnectionError as e:
        logging.error(f'Failed to connect to PeeringDB: {e}')
        return dict()
    if r.status_code != 200:
        logging.error(f'PeeringDB replied with status code: {r.status_code}')
        return dict()
    try:
        json_data = r.json()
    except ValueError as e:
        logging.error(f'Failed to decode JSON reply: {e}')
        return dict()
    return json_data


def fetch_data() -> dict:
    """Fetch netxilan data from PeeringDB."""
    netixlan_data = query_pdb('netixlan')
    if 'data' not in netixlan_data:
        return dict()
    return netixlan_data['data']


def write_data(netixlan_data: dict) -> None:
    if not netixlan_data:
        return
    output_name = datetime.now(UTC).strftime(OUTPUT_FMT) + OUTPUT_SUFFIX
    output_file = OUTPUT_DIR + output_name
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    logging.info(f'Writing {len(netixlan_data)} entries to {output_file}')
    with bz2.open(output_file, 'wb') as f:
        pickle.dump(netixlan_data, f)
    make_symlink(output_name, latest_symlink)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    data = fetch_data()
    write_data(data)


if __name__ == '__main__':
    main()
    sys.exit(0)
