import argparse
import logging
import sys

from file_handlers.pickle import PickleFileHandler

DEFAULT_INPUT = 'raw/peeringdb/latest-peeringdb-ixp.pickle.bz2'
OUTPUT_SUFFIX = '-ixp-prefixes'

def connect_pfx_data(raw_data: dict) -> None:
    """Fetch ix/ixlan/ixpfx data from PeeringDB and push the merged
    entries to the Kafka topic.

    Push one entry per ixpfx to the topic. The entry also contains
    information about the corresponding ix (id, name, name_long, and
    country) as well as the ixlan_id it belongs to.
    The ix_id is used as the key and the 'updated' field (of the ixpfx
    entry) as the timestamp.
    """
    lines = list()
    # Getting the ix_id from the ixpfx requires an additional hop over
    # the ixlan since there is no direct connection.
    # Get ix data.
    ix_data = raw_data['ix']
    if len(ix_data) == 0:
        return lines
    ix_data_dict = dict()
    for entry in ix_data:
        if entry['id'] in ix_data_dict:
            logging.warning('Duplicate ix id: {}. Ignoring entry {}'
                            .format(entry['id'], entry))
            continue
        ix_data_dict[entry['id']] = entry
    # Get ixlan data.
    ixlan_data = raw_data['ixlan']
    if len(ixlan_data) == 0:
        return lines
    # Construct a map ixlan_id -> ix_id.
    ixlan_ix_map = dict()
    for entry in ixlan_data:
        if entry['id'] in ixlan_ix_map:
            logging.warning('Duplicate ixlan id: {}. Ignoring entry {}.'
                            .format(entry['id'], entry))
            continue
        ixlan_ix_map[entry['id']] = entry['ix_id']
    # Get ixpfx data.
    ixpfx_data = raw_data['ixpfx']
    if len(ixpfx_data) == 0:
        return lines
    for entry in ixpfx_data:
        proto = entry['protocol']
        if not (proto == 'IPv4' or proto == 'IPv6'):
            logging.warning('Unknown protocol specified for ixpfx {}: {}'
                            .format(entry['id'], proto))
            continue
        ixlan_id = entry['ixlan_id']
        if ixlan_id not in ixlan_ix_map:
            logging.warning('Failed to find ixlan {} for ixpfx {}.'
                            .format(ixlan_id, entry['id']))
            continue
        ix_id = ixlan_ix_map[ixlan_id]
        if ix_id not in ix_data_dict:
            logging.warning('Failed to find ix {} for ixlan {} / ixpfx {}.'
                            .format(ix_id, ixlan_id, entry['id']))
            continue
        ix_info = ix_data_dict[ix_id]
        line = (ix_id,
                ix_info['name'].replace(',', ' '),
                ix_info['name_long'].replace(',', ' '),
                ix_info['country'],
                ixlan_id,
                proto,
                entry['prefix'])
        lines.append(line)
    lines.sort()
    headers = ('ix_id', 'name', 'name_long', 'country', 'ixlan_id', 'proto',
               'prefix')
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
    lines = connect_pfx_data(data)
    if len(lines) <= 1:
        logging.error(f'No data written.')
        return
    file.write(lines)


if __name__ == '__main__':
    main()
    sys.exit(0)
