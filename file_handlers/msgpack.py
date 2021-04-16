import bz2
import logging
import os

import msgpack

from file_handlers.common import get_file_name

INPUT_SUFFIX = '.msgpack.bz2'
OUTPUT_DIR = 'parsed/'
OUTPUT_SUFFIX = '.csv'
OUTPUT_DELIMITER = ','


class MsgpackFileHandler:
    def __init__(self,
                 input_: str,
                 output: str = None,
                 output_name_suffix: str = None):
        self.input = input_
        if output:
            self.output = output
        else:
            output_name = get_file_name(self.input, INPUT_SUFFIX)
            if output_name_suffix:
                output_name += output_name_suffix
            self.output = OUTPUT_DIR + output_name + OUTPUT_SUFFIX

    def read(self):
        logging.info(f'Reading file: {self.input}')
        return msgpack.load(bz2.BZ2File(self.input, 'rb'))

    def write(self, lines: list) -> None:
        logging.info(f'Writing {len(lines)} lines to file: {self.output}')
        os.makedirs(os.path.dirname(self.output), exist_ok=True)
        with open(self.output, 'w') as f:
            for line in lines:
                f.write(OUTPUT_DELIMITER.join(map(str, line)) + '\n')
