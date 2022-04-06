import argparse
import logging
import os
import sys
from collections import defaultdict

from file_handlers.msgpack import MsgpackFileHandler


DEFAULT_INPUT = 'raw/atlas/latest-probes.msgpack.bz2'
ASN_FILE_DELIMITER = ','
OUTPUT_SUFFIX = '-by-rir'


def read_asn_mapping(input_file: str) -> dict:
    if not os.path.exists(input_file):
        logging.error(f'Failed to find AS map file: {input_file}')
        return dict()

    logging.info(f'Reading AS map file: {input_file}')
    ret = dict()
    with open(input_file, 'r') as f:
        f.readline()
        for line in f:
            line_split = line.strip().split(ASN_FILE_DELIMITER)
            if len(line_split) != 2 or not line_split[1].isdigit():
                logging.error(f'AS map file has invalid line format: '
                              f'{line.strip()}')
                return dict()
            rir = line_split[0]
            asn = int(line_split[1])
            ret[asn] = rir
    logging.info(f'Found mapping for {len(ret)} ASes')
    return ret


def group_by_rir(data: list, asn_map: dict, ipv6: bool) -> list:
    if ipv6:
        key = 'asn_v6'
    else:
        key = 'asn_v4'
    rir_probe_map = defaultdict(int)
    for probe in data:
        if key not in probe or not probe[key]:
            continue
        asn = probe[key]
        if asn not in asn_map:
            logging.warning(f'Failed to find assigned RIR for ASN {asn}. This '
                            f'should not happen.')
            continue
        rir_probe_map[asn_map[asn]] += 1
    if not rir_probe_map:
        return list()
    return [('rir', 'probe_count')] + \
           sorted(rir_probe_map.items(), key=lambda t: t[1], reverse=True)


def main() -> None:
    global OUTPUT_SUFFIX
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('asn_file',
                        help='CSV containing ASN -> RIR mapping as generated '
                             'by get_assigned_as_numbers.py')
    parser.add_argument('-i', '--input',
                        help='use this raw dump instead of latest',
                        default=DEFAULT_INPUT)
    parser.add_argument('-o', '--output',
                        help=',anually specify output file')
    parser.add_argument('--ipv6', action='store_true',
                         help='use IPv6')
    args = parser.parse_args()

    asn_map = read_asn_mapping(args.asn_file)
    if not asn_map:
        sys.exit(1)

    ipv6 = args.ipv6
    if ipv6:
        OUTPUT_SUFFIX += '-v6'
    file = MsgpackFileHandler(input_=args.input, output=args.output,
                              output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = group_by_rir(data, asn_map, ipv6)
    file.write(lines)


if __name__ == '__main__':
    main()
    sys.exit(0)
