import argparse
import logging
import sys

from file_handlers.pickle import PickleFileHandler


DEFAULT_INPUT = 'raw/peeringdb/latest-peeringdb-ixp.pickle.bz2'
OUTPUT_SUFFIX = '-info'


def make_data_lines(ix_data: dict) -> None:
    lines = list()
    if len(ix_data) == 0:
        return lines
    ix_data_dict = dict()
    for entry in ix_data:
        if entry['id'] in ix_data_dict:
            logging.warning('Duplicate ix id: {}. Ignoring entry {}'
                            .format(entry['id'], entry))
            continue
        ix_data_dict[entry['id']] = entry
    for ix_id, ix_data in sorted(ix_data_dict.items()):
        line = (ix_id,
                ix_data['name'].replace(',', ' '),
                ix_data['name_long'].replace(',', ' '),
                ix_data['country'])
        lines.append(line)
    headers = ('ix_id', 'name', 'name_long', 'country')
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
    lines = make_data_lines(data['ix'])
    if len(lines) <= 1:
        logging.error(f'No data written.')
        return
    file.write(lines)


if __name__ == '__main__':
    main()
    sys.exit(0)
