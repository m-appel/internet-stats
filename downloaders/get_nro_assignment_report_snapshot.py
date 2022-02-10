import bz2
import logging
import os
import sys
from datetime import datetime, timezone

import requests

sys.path.append('../')
from file_handlers.common import make_symlink

OUTPUT_DIR = '../raw/nro/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_SUFFIX = '-delegated-stats.bz2'
URL = 'https://www.nro.net/wp-content/uploads/delegated-stats/nro-extended-stats'


def check_output(file: str) -> bool:
    if os.path.exists(file):
        logging.info(f'Current dump already downloaded: {file}')
        return True
    return False


def download_snapshot() -> None:
    output_name = datetime.now(tz=timezone.utc).strftime(OUTPUT_FMT) \
                  + OUTPUT_SUFFIX
    output_file = OUTPUT_DIR + output_name
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    logging.info(f'Output: {output_file}')

    if check_output(output_file):
        sys.exit(0)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f'Downloading {URL}')
    r = requests.get(URL, stream=True)
    download_len = 0
    with bz2.open(output_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1048576):
            download_len += f.write(chunk)
    make_symlink(output_name, latest_symlink)
    logging.info(f'Size: {download_len / 1024 / 1024:2f} MiB')
    logging.info('Finished.')


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    download_snapshot()


if __name__ == '__main__':
    main()
    sys.exit(0)
