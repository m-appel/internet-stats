import argparse
import logging
from collections import defaultdict

import radix

from file_handlers.pickle import PickleFileHandler

DEFAULT_INPUT = 'raw/routeviews/latest-rib.pickle.bz2'
OUTPUT_SUFFIX = '-as-prefixes'


def count_prefixes(data: radix.Radix) -> list:
    logging.info(f'Counting {len(data.nodes())} nodes...')
    as_map = defaultdict(int)
    for rnode in data:
        as_map[rnode.data['as']] += 1
    return [['as', 'pfx_count']] \
           + sorted(as_map.items(), key=lambda t: t[1], reverse=True)


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input',
                        help='Use this raw dump instead of latest',
                        default=DEFAULT_INPUT)
    parser.add_argument('-o', '--output',
                        help='Manually specify output file')
    args = parser.parse_args()
    file = PickleFileHandler(input_=args.input, output=args.output,
                             output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = count_prefixes(data)
    file.write(lines)


if __name__ == '__main__':
    main()
