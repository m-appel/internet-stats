import argparse
import bz2
import logging
import pickle
import sys
from collections import defaultdict
from datetime import datetime

import msgpack


AS_MAP = 'raw/atlas/latest-asn-names.txt'
PROBE_FILE = 'raw/atlas/latest-probes.msgpack.bz2'
DATE_FMT = '%Y%m%d'
OUTPUT_FILE_TEMPLATE = \
    'parsed/{date}-top-{top}-probe-as-by-country.{type}.pickle.bz2'


class Country:
    def __init__(self, cc: str) -> None:
        self.cc = cc
        self.as_map = defaultdict(set)
        self.probe_count = 0
    
    def add_probe(self, asn: int, probe_id: int) -> None:
        self.as_map[asn].add(probe_id)
        self.probe_count += 1
    
    def get_top(self, top: int) -> dict:
        as_probes = [(len(probe_set), asn) for asn, probe_set in self.as_map.items()]
        as_probes.sort(reverse=True)
        if len(as_probes) < top:
            logging.warning(f'More ASes requested ({top}) than '
                            f'available ({len(as_probes)}) for country '
                            f'{self.cc}')
        ret = {str(asn): list(self.as_map[asn]) for _, asn in as_probes[:top]}
        return ret


def read_as_map() -> dict:
    ret = dict()
    with open(AS_MAP, 'r') as f:
        for line in f:
            line_split = line.split()
            asn = int(line_split[0])
            cc = line_split[-1]
            ret[asn] = cc
    return ret


def read_probe_file(ipv6: bool, as_map: dict) -> dict:
    ret = dict()
    with bz2.open(PROBE_FILE, 'rb') as f:
        probe_data = msgpack.load(f)
    for probe in probe_data:
        if ipv6:
            asn = probe['asn_v6']
        else:
            asn = probe['asn_v4']
        probe_id = probe['id']
        if asn is None or probe_id is None:
            continue
        cc = probe['country_code']
        if cc is None:
            # Try inferring cc from AS map.
            if asn not in as_map:
                logging.warning(f'Skipping probe due to missing and '
                                f'unmappable CC: {probe}')
                continue
            cc = as_map[asn]
        if asn in as_map and cc != as_map[asn]:
            logging.debug(f'Error: Probe CC "{cc}"" does not match map CC '
                          f'"{as_map[asn]}"')
        if cc not in ret:
            ret[cc] = Country(cc)
        ret[cc].add_probe(asn, probe_id)
    return ret


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--top', type=int, default=10,
                        help='output TOP ASes per country')
    parser.add_argument('--ipv6', action='store_true', help='use IPv6')
    parser.add_argument('--set', action='store_true',
                        help='write a flat set of probe IDs instead of a dict')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
    )

    top = args.top

    as_map = read_as_map()
    parsed_probe_file = read_probe_file(args.ipv6, as_map)
    if args.set:
        output = {prb_id for country in parsed_probe_file.values()
                  for asn in country.get_top(top).values()
                  for prb_id in asn}
        output_file = \
            OUTPUT_FILE_TEMPLATE.format(date=datetime.utcnow()
                                                     .strftime(DATE_FMT),
                                        top=top,
                                        type='set')
    else:
        output = {cc: country.get_top(top)
                for cc, country in parsed_probe_file.items()}
        output_file = \
            OUTPUT_FILE_TEMPLATE.format(date=datetime.utcnow()
                                                     .strftime(DATE_FMT),
                                        top=top,
                                        type='dict')
    logging.info(f'Writing {output_file}')
    with bz2.open(output_file, 'wb') as f:
        pickle.dump(output, f)


if __name__ == '__main__':
    main()
    sys.exit(0)
