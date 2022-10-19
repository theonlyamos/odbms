#! python
from dotenv import load_dotenv, dotenv_values, find_dotenv

from database import Database

import argparse
import sys

VERSION = "0.0.1"

def main(args):
    global parser

    if not args.dbms:
        parser.print_help()
    Database.load_defaults()
    while True:
        code = input('> ')
        exec(code)
    
def get_arguments():
    global parser
    global VERSION
        
    parser.add_argument('-d', '--dbms', type=str, help="Type of dbms [mysql|mongodb]")
    parser.add_argument('-v','--version', action='version', version=f'%(prog)s {VERSION}')
    parser.set_defaults(func=main)
    return parser.parse_args()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A terminal client for runit")
    args = get_arguments()
    args.func(args)