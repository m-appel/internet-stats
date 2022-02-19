import argparse
import logging
import sys
from typing import Tuple
from file_handlers.common import get_file_name


DEFAULT_INPUT_PATH = 'raw/thyme/'
DEFAULT_DATE = 'latest'
INPUT_FILE_FORMAT = '{date}-bgp-analysis-{location}.txt'
OUTPUT_DIR = 'parsed/'
OUTPUT_SUFFIX = '.csv'
OUTPUT_DELIMITER = ','

LOCATIONS = ('au', 'current', 'hk', 'london', 'singapore')
TOTAL_NUM_AS_LINE = 'Total ASes present in the Internet Routing Table:'
REGION_NUM_AS_PREFIX = 'origin ASes present in the Internet Routing Table:'
AS_PATH_LEN_PREFIX = 'Average'


def get_region_num_as(line: str) -> Tuple[str, str]:
    line_split = line.split()
    region = line_split[0].lower()
    num_as = int(line_split[-1])
    return region, num_as


def get_as_path_len(line: str) -> Tuple[str, str]:
    line_split = line.split()
    region = line_split[1].lower()
    if line_split[1] == 'AS':
        region = 'total'
    as_path_len = float(line_split[-1])
    return region, as_path_len


def get_stats(input_file: str) -> dict:
    ret = dict()
    for rir in ('total', 'afrinic', 'apnic', 'arin', 'lacnic', 'ripe'):
        ret[rir] = {'as-path-len': -1,
                    'num-as': -1}
    with open(input_file, 'r') as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped.startswith(TOTAL_NUM_AS_LINE):
                ret['total']['num-as'] = int(line_stripped.split()[-1])
            elif REGION_NUM_AS_PREFIX in line_stripped:
                region, num_as = get_region_num_as(line)
                ret[region]['num-as'] = num_as
            elif line_stripped.startswith(AS_PATH_LEN_PREFIX):
                region, as_path_len = get_as_path_len(line)
                ret[region]['as-path-len'] = as_path_len
    return ret


def get_date_from_file(input_path: str) -> str:
    sym_file = input_path + INPUT_FILE_FORMAT.format(date=DEFAULT_DATE,
                                                     location=LOCATIONS[0])
    base_file = get_file_name(sym_file, '.txt')
    return base_file.split('-')[0]


def main() -> None:
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_format,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-path',
                        help='input path containing analysis files',
                        default=DEFAULT_INPUT_PATH)
    parser.add_argument('-d', '--input-date',
                        help='use this date (%Y%m%d) instead of latest',
                        default=DEFAULT_DATE)
    parser.add_argument('-o', '--output',
                        help='manually specify output file')
    args = parser.parse_args()

    input_path = args.input_path
    if not input_path.endswith('/'):
        input_path += '/'

    input_date = args.input_date

    stats = dict()

    for location in LOCATIONS:
        stats[location] = \
          get_stats(input_path + INPUT_FILE_FORMAT.format(date=input_date,
                                                          location=location))
    rir_accs = dict()
    header_line = ['location']
    for rir in ('afrinic', 'apnic', 'arin', 'lacnic', 'ripe', 'total'):
        rir_accs[rir] = {'as-path-len': 0,
                         'num-as': 0}
        header_line.append(f'{rir}_as_path_len')
        header_line.append(f'{rir}_num_as')
    lines = [header_line]
    for location, rir_stats in stats.items():
        line = [location]
        for rir, stat_values in sorted(rir_stats.items()):
            for key, value in sorted(stat_values.items()):
                rir_accs[rir][key] += value
                line.append(value)
        lines.append(line)
    acc_line = ['avg']
    for rir, accs in rir_accs.items():
        for key, value in accs.items():
            acc_line.append(value / len(stats))
    lines.append(acc_line)

    output_arg = args.output
    if output_arg:
        output_file = output_arg
    else:
        if input_date == DEFAULT_DATE:
            output_date = get_date_from_file(input_path)
        else:
            output_date = input_date
        output_file = f'{OUTPUT_DIR}{output_date}-as-stats.csv'

    with open(output_file, 'w') as f:
        for line in lines:
            f.write(OUTPUT_DELIMITER.join(map(str, line)) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
