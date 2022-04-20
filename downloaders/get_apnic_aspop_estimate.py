import logging
import os
import sys
from datetime import datetime, timezone

import requests

sys.path.append('../')
from file_handlers.common import make_symlink

OUTPUT_DIR = '../raw/apnic/'
OUTPUT_FMT = '%Y%m%d'
OUTPUT_SUFFIX = '-aspop-estimate.csv'
URL = 'https://stats.labs.apnic.net/aspop/'

EXPECTED_FIELD_COUNT = 7
CC_FIELD_IDX = 2
LINK_CLOSE_TAG = '</a>'


def extract_cc(cc_field: str) -> str:
    cc_field = cc_field.strip('"')
    a_tag_end = cc_field.find('>')
    if a_tag_end == -1:
        logging.error(f'Failed to find <a> tag end in CC field: {cc_field}')
        return str()
    cc = cc_field[a_tag_end + 1: -len(LINK_CLOSE_TAG)]
    if len(cc) != 2:
        logging.error(f'Failed to extract country code from field: {cc_field}')
        return str()
    return cc


def parse_line(line: str) -> list:
    line = line.strip('[],')
    try:
        rank, asn, line = line.split(',', maxsplit=2)
        # This is necessary since AS names can contain commata.
        line = line.lstrip('"')
        as_name, line = line.split('"', maxsplit=1)
    except ValueError:
        logging.error(f'Unkown line format: {line}')
        return None
    rem = line.lstrip(',').split(',')
    parsed_line = [asn.strip('"AS'), as_name.replace(',', ' '), *rem]
    field_count = len(parsed_line)
    if field_count != EXPECTED_FIELD_COUNT:
        logging.error(f'Unkown line format. Expected {EXPECTED_FIELD_COUNT} '
                    f'fields, got {field_count}')
        logging.error(parsed_line)
        return None
    parsed_line[CC_FIELD_IDX] = extract_cc(parsed_line[CC_FIELD_IDX])
    return parsed_line

def check_output(file: str) -> bool:
    if os.path.exists(file):
        logging.info(f'Current dump already downloaded: {file}')
        return True
    return False


def download_table() -> None:
    output_name = datetime.now(tz=timezone.utc).strftime(OUTPUT_FMT) \
                  + OUTPUT_SUFFIX
    output_file = OUTPUT_DIR + output_name
    latest_symlink = OUTPUT_DIR + 'latest' + OUTPUT_SUFFIX
    logging.info(f'Output: {output_file}')

    if check_output(output_file):
        sys.exit(0)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f'Downloading {URL}')
    r = requests.get(URL)
    if not r.ok:
        logging.error(f'Request failed with status code: {r.status_code}')
        sys.exit(1)
    in_list = False
    out_lines = [['asn', 'name', 'cc', 'users', 'country_pct', 'internet_pct', 'samples']]
    for line in r.text.split('\n'):
        line_stripped = line.strip()
        if line_stripped.startswith("['Rank"):
            in_list = True
            continue
        if not in_list:
            continue
        parsed_line = parse_line(line_stripped)
        if not parsed_line:
            break
        out_lines.append(parsed_line)
    logging.info(f'Writing {len(out_lines)} lines to: {output_file}')
    with open(output_file, 'w') as f:
        for line in out_lines:
            f.write(','.join(map(str, line)) + '\n')
    make_symlink(output_name, latest_symlink)
    logging.info('Finished.')


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    download_table()


if __name__ == '__main__':
    main()
    sys.exit(0)
