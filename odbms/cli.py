from odbms import DBMS
import argparse

Database = None

VERSION = "0.4.2"

def start(args):
    import code
    import pyreadline3
    
    
    DBMS.initialize(args.dbms, host=args.host, port=args.port, username=args.username, password=args.password, database=args.database)
    
    print(f"ODBMS Interactive Console v{VERSION}")
    print(f"Connected to {args.dbms} database: {args.database}")
    print("Type 'exit()' or 'quit()' to exit.")
    
    console = code.InteractiveConsole()

    # Start the interactive loop
    while True:
        try:
            # Use the console's raw_input to handle indentation
            line = console.raw_input(">>> ") 
            # The console will handle multiline statements and indentation
            more = console.push(line)
        except EOFError:
            print("\nExiting...")
            break
        except Exception as e:
            print(e)
    
def get_arguments(parser):
    global VERSION
        
    parser.add_argument('dbms', choices=['mongodb', 'mysql', 'sqlite', 'postgresql'], default='sqlite', help='Database management system to use')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, help='Database port')
    parser.add_argument('--username', help='Database username')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('-v','--version', action='version', version=f'%(prog)s {VERSION}')
    parser.set_defaults(func=start)
    return parser.parse_args()

def main():
    parser = argparse.ArgumentParser(description="A terminal client for odbms")
    args = get_arguments(parser)
    args.func(args)
