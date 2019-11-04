import sys
import argparse
import logging
import psycopg2
from dateutil.parser import parse
from datetime import datetime


#
# ########## CLI Arguments ##########
#
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
group = parser.add_mutually_exclusive_group(required=False)
group.add_argument("-f", "--file", help="Input file.", default=False)
group.add_argument("-t", "--tail", help="Listen to stdout.", action='store_true')
parser.add_argument("-v", "--verbose", help="Debug level output", action='store_true')
args = parser.parse_args()

#
# ########## Logging config ##########
#
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.DEBUG if args.verbose else logging.INFO)

#
# ########## Global Vars ##########
#
last_line = None
stack_lock = False


# Parse single line from input into list:
# list[0] - datetime obj or None.
# list[1] - Prefix.
# list[2] - context.
def parse_line(src_line):
    src_list = src_line.split(' ', 3)
    try:
        t_stamp = ' '.join([src_list[0], src_list[1]])
        del src_list[:2]
        src_list = [None if t_stamp == ' ' else parse(t_stamp)] + src_list
        return src_list
    except IndexError:
        logging.debug('Index error.')
    except Exception as e:
        logging.debug('Got this: %s.' % e)


# merge stacks into single line with timestamp
# [HTTP11ClientProtocol] Unhandled errors
def merge_stack(output):
    global last_line
    global stack_lock
    if not output[0]:
        last_line[2] += output[2]
        stack_lock = True
    else:
        workload = []
        if stack_lock:
            stack_lock = False
            workload.append(last_line)
        last_line = output
        workload.append(last_line)
        return workload


def postg_insert(src_line):
    output = parse_line(src_line)
    if output:
        workload = merge_stack(output)
        if workload:
            for record in workload:
                # print(record)
                pass


#
# Need bulk insert for whole file
# Need single record insert for tail output
# Need stat calculations


# Infinite loop for stdout listen
# Aborts on ctrl+c
def listen_stdout():
    logging.debug('Initializing listener for stdout.')
    try:
        line = ''
        while True:
            line += sys.stdin.read(1)
            if line.endswith('\n'):
                postg_insert(line)
                line = ''
    except KeyboardInterrupt:
        logging.debug('Termination signal received.')
        sys.stdout.flush()


def read_file_lines(filename):
    logging.debug('Reading log lines from file: "%s"' % filename)
    with open(filename, 'r') as src_file:
        log_lines = src_file.readlines()
        logging.debug('File len (line count): %i' % len(log_lines))
        for log_line in log_lines:
            postg_insert(log_line)


def main():
    if args.tail:
        listen_stdout()
    elif args.file:
        read_file_lines(args.file)
    else:
        print('something else')


if __name__ == '__main__':
    main()
