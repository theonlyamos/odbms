from odbms import DBMS

import argparse
import sys

Database = None

VERSION = "0.2.9"

def main(args):
    global parser
    global Database

    if not args.dbms:
        parser.print_help()
        sys.exit(1)
    if args.dbms in ['mysql','mongodb','sqlite', 'postgresql']:
        Database = DBMS.initialize_with_defaults(args.dbms, args.database)
        # from odbms import Database
        
    while True:
        code = input('> ')
        exec(code)
    
def get_arguments():
    global parser
    global VERSION
        
    parser.add_argument('dbms', type=str, choices=['mysql','mongodb','sqlite', 'postgresql'], help="Type of dbms [mysql|mongodb|sqlite|postgresql]")
    parser.add_argument('-db', '--database', default='runit', type=str, help="Name of database")
    parser.add_argument('-v','--version', action='version', version=f'%(prog)s {VERSION}')
    parser.set_defaults(func=main)
    return parser.parse_args()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A terminal client for runit")
    args = get_arguments()
    args.func(args)
