import argparse
import logging
import sys

from file_handlers.pickle import PickleFileHandler


DEFAULT_INPUT = 'raw/peeringdb/latest-peeringdb-netixlan.pickle.bz2'
OUTPUT_SUFFIX = '-ixp-participants'


def make_data_lines(netixlan_data: dict) -> None:
    lines = list()
    if len(netixlan_data) == 0:
        return lines
    for entry in netixlan_data:
        ix_id = entry['ix_id']
        ixlan_id = entry['ixlan_id']
        asn = entry['asn']
        if not asn:
            continue
        if 'ipaddr4' in entry and entry['ipaddr4']:
            line = (ix_id,
                    ixlan_id,
                    asn,
                    entry['ipaddr4'])
            lines.append(line)
        if 'ipaddr6' in entry and entry['ipaddr6']:
            line = (ix_id,
                    ixlan_id,
                    asn,
                    entry['ipaddr6'])
            lines.append(line)
    lines.sort()
    headers = ('ix_id', 'ixlan_id', 'asn', 'ip')
    lines.insert(0, headers)
    return lines


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
    lines = make_data_lines(data)
    if len(lines) <= 1:
        logging.error(f'No data written.')
        return
    file.write(lines)


if __name__ == '__main__':
    main()
    sys.exit(0)
