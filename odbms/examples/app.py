from odbms import DBMS

import argparse
import sys

Database = None

VERSION = "0.0.1"

def main(args):
    global parser
    global Database

    if not args.dbms:
        parser.print_help()
        sys.exit(1)
    if 'mysql' in args.dbms or 'mongodb' in args.dbms or 'sqlite' in args.dbms:
        DBMS.initialize_with_defaults(args.dbms, args.database)
        Database = DBMS.Database
        
    while True:
        code = input('> ')
        exec(code)
    
def get_arguments():
    global parser
    global VERSION
        
    parser.add_argument('dbms', type=str, choices=['mysql','mongodb','sqlite'], help="Type of dbms [mysql|mongodb]")
    parser.add_argument('-db', '--database', default='runit', type=str, help="Name of database")
    parser.add_argument('-v','--version', action='version', version=f'%(prog)s {VERSION}')
    parser.set_defaults(func=main)
    return parser.parse_args()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A terminal client for runit")
    args = get_arguments()
    args.func(args)
