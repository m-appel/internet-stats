import argparse
import logging
from collections import defaultdict

from file_handlers.msgpack import MsgpackFileHandler

DEFAULT_INPUT = 'raw/atlas/latest-probes.msgpack.bz2'
OUTPUT_SUFFIX = '-by-country'


def group_by_country(data: list) -> list:
    as_probe_map = defaultdict(int)
    for probe in data:
        if 'country_code' not in probe or not probe['country_code']:
            continue
        as_probe_map[probe['country_code']] += 1
    if not as_probe_map:
        return list()
    return [('country', 'probe_count')] + \
           sorted(as_probe_map.items(), key=lambda t: t[1], reverse=True)


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
    file = MsgpackFileHandler(input_=args.input, output=args.output,
                              output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = group_by_country(data)
    file.write(lines)


if __name__ == '__main__':
    main()
