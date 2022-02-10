import argparse
import logging
from collections import defaultdict

from file_handlers.msgpack import MsgpackFileHandler

DEFAULT_INPUT = 'raw/atlas/latest-probes.msgpack.bz2'
OUTPUT_SUFFIX = '-by-country'


def group_by_country(data: list, ipv6: bool) -> list:
    as_probe_map = defaultdict(int)
    for probe in data:
        if 'country_code' not in probe or not probe['country_code']:
            continue
        if ipv6:
            if 'asn_v6' not in probe or not probe['asn_v6']:
                continue
        else:
            if 'asn_v4' not in probe or not probe['asn_v4']:
                continue
        as_probe_map[probe['country_code']] += 1
    if not as_probe_map:
        return list()
    return [('country', 'probe_count')] + \
           sorted(as_probe_map.items(), key=lambda t: t[1], reverse=True)


def main() -> None:
    global OUTPUT_SUFFIX
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
    parser.add_argument('--ipv6', action='store_true', help='use IPv6')
    args = parser.parse_args()

    ipv6 = args.ipv6
    if ipv6:
        OUTPUT_SUFFIX += '-v6'

    file = MsgpackFileHandler(input_=args.input, output=args.output,
                              output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = group_by_country(data, ipv6)
    file.write(lines)


if __name__ == '__main__':
    main()
