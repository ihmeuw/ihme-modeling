#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import mysql.connector
import sys
from dalynator.columnstore.ihmedb import IhmeDb


iniFile = 'DIRECTORY'

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


if not os.path.exists(iniFile):i
    eprint("ERROR: {} not found.".format(iniFile))
    exit(1)

def main():
    parser = argparse.ArgumentParser(description='Refresh GBD metadata in ColumnStore')
    parser.add_argument('-gv', nargs=1,       help="gbd_process_version_id",                                          type=int)
    parser.add_argument('-refreshDB',         help="comma separated list of DBs to be refreshed",                     type=str)
    parser.add_argument('-refreshTables',     help="comma separated list of tables to be refreshed",                  type=str)
    parser.add_argument('-skipDB',            help="comma separated list of DBs to be excluded from being refreshed", type=str)
    parser.add_argument('-remote_load',       help="load prod data to staging from remote server",    action="store_true")
    args = parser.parse_args()

    # start ETL process
    try:
        create_mycnf = not args.remote_load
        dbObj = IhmeDb(iniFile, create_mycnf)
    except Exception as err:
        eprint("ERROR: Cannot connect to either source or destination database")
        eprint(err)
        exit(1)

    # get databases to be refreshed
    if args.refreshDB is not None:
        metadataDBs = [db.strip() for db in args.refreshDB.split(',')]
    else:
        metadataDBs = dbObj.getDBsNamesToExport()

    if args.skipDB is not None:
        for db in args.skipDB.split(','):
            metadataDBs.remove(db.strip())

    if args.refreshTables is not None and len(metadataDBs) > 1:
        eprint("ERROR: Cannot refresh more than one DB when refreshTables flag is provided.")
        exit(1)

    for db in metadataDBs:
        # get all metadata tables of database
        if args.refreshTables is not None:
            tables = [table.strip() for table in args.refreshTables.split(',')]
        else:
            tables = dbObj.getMetadataTables(db)

        # refresh prod data
        try:
            dbObj.refreshTables(db, tables, args.remote_load)
        except Exception as err:
            eprint("ERROR: Cannot refresh production data")
            eprint(err)
            exit(1)

        # call sproc to create split tables
        if args.gv is not None and db == 'DATABASE':
            try:
                dbObj.createGBDSplitTables(args.gv)
            except mysql.connector.Error as err:
                eprint("ERROR: Cannot create split tables")
                eprint(err)
                exit(1)

if __name__ == '__main__':
    main()
