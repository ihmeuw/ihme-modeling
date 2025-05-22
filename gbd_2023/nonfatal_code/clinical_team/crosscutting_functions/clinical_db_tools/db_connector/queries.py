import glob
import os


class Queries:
    """
    Queries is an object that dynamically keeps track of queries saved as txt
    files inside a folder. This allows common SQL queries to be kept in a
    central place. Furthermore, the queries object saves these queries to
    its own namespace, allowing tab completion to be used when searching for
    the right query.

    Example (Using Database):
    query_folder = FILEPATH
    queries = Queries(query_folder)

    DBopen = DBManager('some_odbc_profile')
    with DBopen(queries.my_favorite_query) as table:
        foo(table)
    """

    def __init__(self, path_to_queries):
        query_paths = glob.glob(path_to_queries + "*.txt")
        # For each query file, make a new variable with the files
        # basename and store the files content.
        self._path = path_to_queries
        for path in query_paths:
            filename = os.path.basename(path).split(".")[0]
            with open(path, "r") as file:
                self.__dict__[filename] = file.read()