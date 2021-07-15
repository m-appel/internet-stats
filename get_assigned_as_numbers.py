import argparse
import logging
import sys
from collections import defaultdict, namedtuple
from itertools import zip_longest

from file_handlers.bz2 import Bz2FileHandler

DEFAULT_INPUT = 'raw/nro/latest-delegated-stats.bz2'
OUTPUT_SUFFIX = '-assigned-asns'
INPUT_DELIMITER = '|'
VERSION_LINE_FIELD_COUNT = 7
SUMMARY_LINE_FIELD_COUNT = 6
RECORD_LINE_MIN_FIELD_COUNT = 7

# For an explanation of the fields see
# https://www.nro.net/wp-content/uploads/nro-extended-stats-readme5.txt
VersionLine = namedtuple('VersionLine',
                         'version registry serial records startdate enddate UTCoffset')
SummaryLine = namedtuple('SummaryLine', 'registry type count summary')
RecordLine = namedtuple('RecordLine',
                        'registry cc type start value date status')


def parse_summary_line(fields: list) -> SummaryLine:
    return SummaryLine(fields[0], fields[2], int(fields[4]), fields[5])


def get_assigned_asns(data: list) -> list:
    line_count = 0
    expected_record_counts = dict()
    record_counts = defaultdict(int)
    ret = list()
    for line in data:
        line_count += 1
        if line.startswith('#'):
            # Comment line
            continue
        line_split = line.strip().split('|')
        if line_count == 1:
            if len(line_split) != VERSION_LINE_FIELD_COUNT:
                logging.error(f'Malformed version line: {line.strip()}')
                continue
            parsed_line = VersionLine(*line_split)
            logging.info(f'  version: {parsed_line.version}')
            logging.info(f' registry: {parsed_line.registry}')
            logging.info(f'   serial: {parsed_line.serial}')
            logging.info(f'  records: {parsed_line.records}')
            logging.info(f'startdate: {parsed_line.startdate}')
            logging.info(f'  enddate: {parsed_line.enddate}')
            logging.info(f'UTCoffset: {parsed_line.UTCoffset}')
            logging.info('')
        elif len(line_split) == SUMMARY_LINE_FIELD_COUNT:
            parsed_line = parse_summary_line(line_split)
            expected_record_counts[parsed_line.type] = parsed_line.count
        elif len(line_split) >= RECORD_LINE_MIN_FIELD_COUNT:
            parsed_line = RecordLine(*line_split[:RECORD_LINE_MIN_FIELD_COUNT])
            record_counts[parsed_line.type] += 1
            if parsed_line.type != 'asn' or parsed_line.status != 'assigned':
                continue
            start_asn = int(parsed_line.start)
            count = int(parsed_line.value)
            ret += zip_longest([parsed_line.registry],
                               range(start_asn, start_asn + count),
                               fillvalue=parsed_line.registry)
    for record_type in expected_record_counts:
        if record_type not in record_counts \
                or expected_record_counts[record_type] != record_counts[
            record_type]:
            if record_type in record_counts:
                logging.warning(
                    f'Missing records for {record_type} class. Expected '
                    f'{expected_record_counts[record_type]} got '
                    f'{record_counts[record_type]}')
            else:
                logging.error(
                    f'Records for {record_type} class are missing entirely.')
    ret.sort(key=lambda t: t[1])
    last_asn = ret[0][1]
    for registry, asn in ret[1:]:
        if asn == last_asn:
            logging.error(f'AS {asn} was assigned twice?!')
        last_asn = asn
    ret.insert(0, ('registry', 'asn'))
    return ret


def main() -> None:
    log_format = '%(asctime)s %(processName)s %(message)s'
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

    file = Bz2FileHandler(input_=args.input, output=args.output,
                          output_name_suffix=OUTPUT_SUFFIX)
    data = file.read()
    lines = get_assigned_asns(data)
    file.write(lines)


if __name__ == '__main__':
    main()
    sys.exit(0)
