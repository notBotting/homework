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
workload = []
prefix_stats = {}

# Connection settings for postgres
table_name = 'log_lines'
try:
    connection = psycopg2.connect(user="postgres",
                                  password="123",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")

    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    logging.debug('Connection established. Server: "%s"' % record)
except (Exception, psycopg2.Error) as error:
    logging.debug('Error while establishing connection to Postgres server: "%s"' % error)
    sys.exit(0)

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
        build_stats(src_list)
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
    global workload
    if not output[0]:
        last_line[2] += output[2]
        stack_lock = True
    else:
        if stack_lock:
            stack_lock = False
            workload.append(list(last_line))
        last_line = output
        workload.append(list(last_line))


def build_workload(src_line):
    output = parse_line(src_line)
    if output:
        merge_stack(output)


def pginsert(table_name, workload):
    pgquery = "INSERT INTO %s VALUES " % table_name
    value_pattern = ",".join(["%s" for i in xrange(len(workload[0]))])
    args_str = ','.join(cursor.mogrify("(" + value_pattern  + ")", tuple(x)) for x in workload)
    cursor.execute(pgquery + args_str)
    connection.commit()

# Need stat calculations
def build_stats(src_list):
    global prefix_stats
    if src_list[1]:
        if src_list[1] not in prefix_stats.keys():
            prefix_stats[src_list[1]] = 0
        prefix_stats[src_list[1]] += 1


def upload_stats():
    table = 'prefix_stats'
    payload = [(datetime.now(), str(prefix_stats))]
    try:
        pginsert(table, payload)
    except Exception as e:
        logging.debug('Exception on stat upload: %s' % e)


def print_stats():
    for k, v in prefix_stats.iteritems():
        print("Prefix: %s, count: %i" % (k, v))

# Infinite loop for stdout listen
# Aborts on ctrl+c
def listen_stdout():
    global workload
    logging.debug('Initializing listener for stdout.')
    try:
        line = ''
        while True:
            line += sys.stdin.read(1)
            if line.endswith('\n'):
                build_workload(line)
                line = ''
                if not stack_lock:
                    pginsert(table_name, workload)
                    workload = []
    except KeyboardInterrupt:
        logging.debug('Termination signal received.')
        sys.stdout.flush()
        print_stats()
        upload_stats()


def read_file_lines(filename):
    logging.debug('Reading log lines from file: "%s"' % filename)
    global workload
    with open(filename, 'r') as src_file:
        log_lines = src_file.readlines()
        logging.debug('File len (line count): %i' % len(log_lines))
        for log_line in log_lines:
            build_workload(log_line)
    pginsert(table_name, workload)
    workload = []
    print_stats()
    upload_stats()


def main():
    if args.tail:
        listen_stdout()
    elif args.file:
        read_file_lines(args.file)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
