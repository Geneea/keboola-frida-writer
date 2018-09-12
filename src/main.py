# coding=utf-8
# Python 3

import argparse
import sys
import traceback

from writer_app import WriterApp

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--data', dest='data_dir', required=True)
        args = parser.parse_args()

        app = WriterApp(data_dir=args.data_dir)
        app.run()

        sys.exit(0)
    except (ValueError, LookupError, AttributeError, IOError) as e:
        print('{type}: {e}'.format(type=type(e).__name__, e=e), file=sys.stderr)
        sys.stderr.flush()
        sys.exit(1)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        print('{type}: {e}'.format(type=type(e).__name__, e=e), file=sys.stderr)
        print('please contact our support at support@geneea.com', file=sys.stderr)
        sys.stderr.flush()
        sys.exit(2)

if __name__ == '__main__':
    main()
