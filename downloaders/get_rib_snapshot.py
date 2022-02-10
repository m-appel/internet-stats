import argparse
import bz2
import logging
import os
import pickle
import subprocess
import sys
import tempfile
from datetime import datetime

import radix
import requests

sys.path.append('../')
from file_handlers.common import make_symlink

DATE_FMT = '%Y%m%d'
RIB_URL = 'http://archive.routeviews.org/route-views.wide/bgpdata/{year}.{month:02d}/RIBS/rib.{year}{month:02d}{day:02d}.0000.bz2'
RIB_FIELD_DELIMITER = '|'
RIB_FIELD_COUNT = 15
PFX_FIELD_IDX = 5
AS_PATH_FIELD_IDX = 6
OUTPUT_DIR = '../raw/routeviews/'
OUTPUT_SUFFIX = '-rib.pickle.bz2'


def process_rib(file: str) -> radix.Radix:
    logging.info('Processing RIB')
    rtree = radix.Radix()
    with subprocess.Popen(['bgpdump', '-m', '-v', '-t', 'change', file],
                          bufsize=1, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, encoding='utf-8') as cp:
        line_count = 0
        for line in cp.stdout:
            line_count += 1
            line_split = line.split(RIB_FIELD_DELIMITER)
            if len(line_split) != RIB_FIELD_COUNT:
                logging.error(f'Looks like bgpdump output has changed. '
                              f'Expected {RIB_FIELD_COUNT} fields, got '
                              f'{len(line_split)}.')
                logging.error(line)
                logging.error(line_count)
                return radix.Radix()
            pfx = line_split[PFX_FIELD_IDX]
            as_path = line_split[AS_PATH_FIELD_IDX]
            if pfx == '0.0.0.0/0' or pfx == '::/0':
                continue
            node = rtree.add(pfx)
            node.data['as'] = as_path.split(' ')[-1]
    logging.info(f'Processed {line_count} RIB entries')
    return rtree


def save_rtree(rtree: radix.Radix, output: str) -> None:
    logging.info(f'Saving rtree: {output}')
    with bz2.open(output, 'wb') as f:
        pickle.dump(rtree, f, pickle.HIGHEST_PROTOCOL)
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    output_name = os.path.basename(output)
    make_symlink(output_name, latest_symlink)


def download_and_process_rib(date: datetime) -> None:
    url = RIB_URL.format(year=date.year, month=date.month, day=date.day)
    logging.info(f'Downloading RIB: {url}')
    r = requests.get(url)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        logging.error(f'Request failed with error: {e}')
        return
    with tempfile.NamedTemporaryFile(delete=True, suffix='.bz2') as tmp:
        tmp.write(r.content)
        tmp.flush()
        rtree = process_rib(tmp.name)
    if rtree.nodes():
        output = OUTPUT_DIR + date.strftime(DATE_FMT) + OUTPUT_SUFFIX
        save_rtree(rtree, output)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help='Download RIB for this date '
                                             'specified as %Y%m%d')
    args = parser.parse_args()
    date = datetime.now()
    if args.date:
        try:
            date = datetime.strptime(args.date, DATE_FMT)
        except ValueError as e:
            logging.error(f'Invalid date specified: {date} {e}')
            sys.exit(1)
    download_and_process_rib(date)


if __name__ == '__main__':
    main()
