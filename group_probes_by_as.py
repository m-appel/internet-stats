import argparse
import logging
from collections import defaultdict

from file_handlers.msgpack import MsgpackFileHandler

DEFAULT_INPUT = 'raw/atlas/latest-probes.msgpack.bz2'
OUTPUT_SUFFIX = '-by-as'


def group_by_as(data: list, ipv6: bool) -> list:
    if ipv6:
        key = 'asn_v6'
    else:
        key = 'asn_v4'
    as_probe_map = defaultdict(int)
    for probe in data:
        if key not in probe or not probe[key]:
            continue
        as_probe_map[probe[key]] += 1
    if not as_probe_map:
        return list()
    return [('as', 'probe_count')] + \
           sorted(as_probe_map.items(), key=lambda t: t[1], reverse=True)


def main() -> None:
    global OUTPUT_SUFFIX
    log_format = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input',
                        help='use this raw dump instead of latest',
                        default=DEFAULT_INPUT)
    parser.add_argument('-o', '--output',
                        help=',anually specify output file')
    parser.add_argument('--ipv6', action='store_true',
                         help='use IPv6')
    args = parser.parse_args()

    ipv6 = args.ipv6
    if ipv6:
        OUTPUT_SUFFIX += '-v6'
    file = MsgpackFileHandler(input_=args.input, output=args.output,
                              output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = group_by_as(data, ipv6)
    file.write(lines)


if __name__ == '__main__':
    main()
