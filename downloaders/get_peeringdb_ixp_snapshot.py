import bz2
import logging
import pickle
import sys
from datetime import datetime, timezone
from typing import Tuple

import requests

sys.path.append('../')
from file_handlers.common import make_symlink


API_BASE ='https://peeringdb.com/api/'
OUTPUT_DIR = '../raw/peeringdb/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_SUFFIX = '-peeringdb-ixp.pickle.bz2'


def query_pdb(endpoint: str, params=None) -> dict:
    """Query the PeeringDB API with the specified endpoint and
    parameters and return the response JSON data as a dictionary.
    """
    if params is None:
        params = dict()
    url = API_BASE + endpoint
    # Always query only 'ok' entries
    params['status'] = 'ok'
    logging.info('Querying PeeringDB {} with params {}'.format(url, params))
    try:
        r = requests.get(url, params)
    except ConnectionError as e:
        logging.error('Failed to connect to PeeringDB: {}'.format(e))
        return dict()
    if r.status_code != 200:
        logging.error('PeeringDB replied with status code: {}'
                      .format(r.status_code))
        return dict()
    try:
        json_data = r.json()
    except ValueError as e:
        logging.error('Failed to decode JSON reply: {}'.format(e))
        return dict()
    return json_data


def fetch_data() -> Tuple[dict, dict, dict]:
    """Fetch ix/ixlan/ixpfx data from PeeringDB."""
    ix_data = query_pdb('ix')
    if 'data' in ix_data:
        ix_data = ix_data['data']
    ixlan_data = query_pdb('ixlan')
    if 'data' in ixlan_data:
        ixlan_data = ixlan_data['data']
    ixpfx_data = query_pdb('ixpfx')
    if 'data' in ixpfx_data:
        ixpfx_data = ixpfx_data['data']
    return ix_data, ixlan_data, ixpfx_data


def write_data(ix_data: dict, ixlan_data: dict, ixpfx_data: dict) -> None:
    if not any((ix_data, ixlan_data, ixpfx_data)):
        return
    out = {'ix': ix_data,
           'ixlan': ixlan_data,
           'ixpfx': ixpfx_data}
    output_name = datetime.now(tz=timezone.utc).strftime(OUTPUT_FMT) \
                  + OUTPUT_SUFFIX
    output_file = OUTPUT_DIR + output_name
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    logging.info(f'Writing {len(ix_data)} IXPs, {len(ixlan_data)} IX LANs, '
                 f'{len(ixpfx_data)} IXP prefixes to {output_file}')
    with bz2.open(output_file, 'wb') as f:
        pickle.dump(out, f)
    make_symlink(output_name, latest_symlink)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    data = fetch_data()
    write_data(*data)


if __name__ == '__main__':
    main()
    sys.exit(0)
