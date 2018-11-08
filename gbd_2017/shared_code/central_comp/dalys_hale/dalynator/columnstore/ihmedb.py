import configparser
import mysql.connector
import os
import textwrap
from subprocess import Popen, PIPE

class IhmeDb:
    _srcCnx      = None
    _srcConnDict = {}
    _srcCursor   = None

    _tgtCnx      = None
    _tgtConnDict = {}
    _tgtCursor   = None

    _dbs         = []
    _etldb       = None
    _batchSize   = 1000
    _myCnf       = "{}/{}".format(os.getcwd(), ".my.cnf")

    def parseConfig(self, iniFile):
        iniConfig = configparser.ConfigParser()
        iniConfig.read(iniFile)

        srcDbConnParam = iniConfig['mysql_client']
        for k,v in srcDbConnParam.items():
            self._srcConnDict[k] = v

        tgtDbConnParam = iniConfig['cs_client']
        for k,v in tgtDbConnParam.items():
            self._tgtConnDict[k] = v

        dbsParam = iniConfig['databases']
        for db in dbsParam.get('database').split(','):
            if db != 'staging': self._dbs.append(db)

        self._etldb = dbsParam.get('etldatabase')

    def createMyCnf(self):
        mycnfFh=open(self._myCnf, 'w')
        mycnfFh.write("[client]\n")
        mycnfFh.write("user={}\n".format(self._srcConnDict['user']))
        mycnfFh.write("password={}\n".format(self._srcConnDict['password']))
        mycnfFh.write("default-character-set=utf8")
        mycnfFh.close()

    def overrideDBsNamesToExport(self, userDefDbs):
        self._dbs = userDefDbs  # type: list

    def getDBsNamesToExport(self):
        return self._dbs

    def getMetadataTables(self, db):
        tables = []

        query = ("SELECT table_name FROM information_schema.tables "
                 "WHERE table_schema = '{}'".format(db))

        if db == 'gbd':
            query = "{} {}".format(query, "AND table_name NOT REGEXP '^output.*_v[0-9]+'")

        self._srcCursor.execute(query)

        for table in self._srcCursor:
            tables.append(table[0])

        return tables

    def getColumns(self, db, table):
        # construct query
        query = ("SELECT column_name, column_key FROM information_schema.columns "
                 "WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ordinal_position".format(db, table))
        self._srcCursor.execute(query)

        pkColumns = []
        columns = []
        for (column_name, column_key) in self._srcCursor:
            if column_key == 'PRI':
                pkColumns.append(column_name)

            columns.append(column_name)

        return (columns, pkColumns)

    def createStagingTable(self, etldb, db, table):
        query = ("SELECT COUNT(*) AS cnt "
                 "FROM information_schema.tables "
                 "WHERE table_schema='{etldb}' AND table_name='{db}_{table}'".format(etldb=etldb, db=db, table=table))
        self._tgtCursor.execute(query)

        cnt = self._tgtCursor.fetchall()[0][0]

        if cnt == 0:
            ddl=[]
            query="""
            SELECT
                CONCAT(
                    CASE
                        WHEN ordinal_position = 1
                            THEN concat('CREATE TABLE IF NOT EXISTS {etldb}.{db}_{table} (')
                            ELSE ' '
                    END,
                    column_name, ' ',
                    CASE
                        WHEN (   column_type = 'text'
                              OR (data_type   = 'varchar' AND character_maximum_length > 8000)
                             )
                            THEN 'varchar(8000)'
                        WHEN data_type = 'mediumint'
                            THEN REPLACE(column_type, 'mediumint', 'int')
                        WHEN data_type = 'timestamp'
                            THEN 'datetime'
                        ELSE column_type
                    END,
                    IF( is_nullable = 'NO',
                        CASE
                            WHEN table_schema = 'gbd'          AND table_name = 'gbd_process_version' AND column_name IN ('code_version')                          THEN ' '
                            WHEN table_schema = 'shared'       AND table_name = 'covariate'           AND column_name IN ('covariate_description','group_display') THEN ' '
                            WHEN table_schema = 'localization' AND table_name = 'measure_language'    AND column_name IN ('measure_name_short')                    THEN ' '
                                ELSE ' NOT NULL '
                        END,
                        ' '),
                    IF((is_nullable = 'NO' AND !isnull(column_default) AND column_type NOT IN ('datetime','timestamp')), CONCAT(' DEFAULT ', quote(column_default)), ' '),
                    CASE
                        WHEN ordinal_position = last_col
                            THEN ' ) engine=columnstore;'
                            ELSE ','
                    END
                ) AS ddl_stmt
            FROM
                information_schema.columns
            JOIN
                ( SELECT
                        table_name AS tname, MAX(ordinal_position) AS last_col
                   FROM
                        information_schema.columns
                   WHERE
                        table_schema = '{db}'
                   GROUP BY table_name
                ) AS lastcol
                ON lastcol.tname = table_name
            WHERE
                    table_schema = '{db}'
                AND table_name   = '{table}'
                AND table_name NOT LIKE '$vtable%'
            ORDER BY
                table_name, ordinal_position
            """.format(etldb=etldb, db=db, table=table)

            self._srcCursor.execute(query)

            for ddl_stmt in self._srcCursor:
                ddl.append(ddl_stmt[0])

            self._tgtCursor.execute(''.join(ddl))
            self._tgtCnx.commit()

    def columnCompareClause(self, colums, joinOps, colOps):
        joinStmt = []
        for i in range(len(colums)):
            lf   = "\n" if i != len(colums) - 1 else ""
            jOps = joinOps if i != 0 else ""
            tab=" " * 16   if i != 0 else ""
            joinStmt.append("{tab}{joinOps} tgt.{pkCol} {colOps} src.{pkCol}{lf}".format(tab=tab, joinOps=jOps, pkCol=colums[i], colOps=colOps, lf=lf))

        return ''.join(joinStmt)

    def tableExists(self, cursor, db, table):
        query = "SELECT COUNT(*) as CNT FROM information_schema.tables WHERE table_schema = '{db}' AND table_name = '{table}'".format(db=db, table=table)

        cursor.execute(query)
        if cursor.fetchall()[0][0] == 1:
            return True
        else:
            return False

    def refreshTables(self, db, tables, loadFromRemote):
        # note: etldb for this approach is the same as the target db
        for table in tables:
            tempProdTable = "etl_{db}_{table}".format(db=db, table=table)
            stagingTable  = "{db}_{table}".format(db=db, table=table)

            # check if staging table've been created. If not create it
            if not self.tableExists(self._tgtCursor, db, stagingTable):
                self.createStagingTable(db, db, table)  # First argument is where the staging table is created, required for renaming tables
            else:
                # truncate staging
                self._tgtCursor.execute("truncate table {etldb}.{stagingTable}".format(etldb=db, stagingTable=stagingTable))

            # create query
            columns = self.getColumns(db, table)[0]
            columnStr = ','.join(columns)

            # query to pull data from production
            selectQuery = ("SELECT {} FROM {}.{}".format(columnStr, db, table))

            # convert year 0000, and 0001-mm-dd to 1000-01-01 and 1001-01-01
            selectQuery=selectQuery.replace(",date_inserted", ",case when year(cast(date_inserted as datetime)) in (0,1) then cast( concat(1000 + year(cast(date_inserted as datetime)), '-01-01') as datetime) else date_inserted end")
            selectQuery=selectQuery.replace(",start_date", "   ,case when year(cast(start_date as datetime))    in (0,1) then cast( concat(1000 + year(cast(start_date as datetime)),    '-01-01') as datetime) else start_date end")

            if loadFromRemote:
                # user driver to load
                insertStmt = "INSERT INTO {etldb}.{stagingTable} ({columns}) VALUES ({data})".format(etldb=db, stagingTable=stagingTable, columns=columnStr,
                                                                                      data=','.join(['%s'] * len(columns)))

                self._srcCursor.execute(selectQuery)
                dataRows = self._srcCursor.fetchmany(size=self._batchSize)

                while len(dataRows) > 0:
                    try:
                        self._tgtCursor.executemany(insertStmt, dataRows)
                        self._tgtCnx.commit()
                        dataRows = self._srcCursor.fetchmany(size=self._batchSize)
                    except mysql.connector.Error as err:
                        print("ERROR: Cannot load to {etldb}.{stagingTable}".format(etldb=db, stagingTable=stagingTable))
                        raise Exception(err)
            else:
                # use cpimport
                srcMysql = []
                srcMysql.append("DIRECTORY")
                srcMysql.append("--defaults-file={}".format(self._myCnf))
                srcMysql.extend("-q -h {srcHost} -P {srcPort}".format(srcHost=self._srcConnDict['host'], srcPort=self._srcConnDict['port']).split())
                srcMysql.extend(["-e", selectQuery, "-N", db])

                cpimport = []
                cpimport.extend(["sudo", "DIRECTORY"])
                cpimport.extend(["-s", "\t", "-n", "1", db, stagingTable])

                pMysqlData = Popen(srcMysql, stdout=PIPE)
                pCpimport  = Popen(cpimport, stdin=pMysqlData.stdout, stdout=PIPE)

                (pCpimportOut, pCpimportErr) = pCpimport.communicate()
                if 'Error'.lower() in pCpimportOut.decode('utf-8').lower():
                    raise Exception(pCpimportOut.decode('utf-8'))

            try:
                self._tgtCursor.execute("ALTER TABLE {db}.{table}           RENAME TO {etldb}.{tempProdTable}".format(db=db, table=table, etldb=db, tempProdTable=tempProdTable))
                self._tgtCursor.execute("ALTER TABLE {etldb}.{stagingTable} RENAME TO {db}.{table}".format(etldb=db, stagingTable=stagingTable, db=db, table=table))
                self._tgtCursor.execute("DROP TABLE {etldb}.{tempProdTable}".format(etldb=db, tempProdTable=tempProdTable))
            except Exception as err:
                if self.tableExists(self._tgtCursor, db, tempProdTable) and not self.tableExists(self._tgtCursor, db, table):
                    self._tgtCursor.execute("ALTER TABLE {etldb}.{tempProdTable} RENAME TO {db}.{table}".format(etldb=db, tempProdTable=tempProdTable, db=db, table=table))
                raise Exception(err)

    def createGBDSplitTables(self, gbd_process_version_id):
        self._tgtCursor.callproc('gbd.cs_new_gbd_process_version_create_split_tables', gbd_process_version_id)

    def connDBs(self):
        self._srcCnx = mysql.connector.connect(**self._srcConnDict)
        self._tgtCnx = mysql.connector.connect(**self._tgtConnDict)

    def __init__(self, iniFile, create_mycnf):
        self.parseConfig(iniFile)
        if create_mycnf:
            self.createMyCnf()
        self.connDBs()
        self._srcCursor = self._srcCnx.cursor()
        self._tgtCursor = self._tgtCnx.cursor()

    def __exit__(self):
        self._srcCursor.close()
        self._srcCnx.close()
        self._tgtCursor.close()
        self._tgtCnx.close()
