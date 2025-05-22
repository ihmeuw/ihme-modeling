from db_tools_core.lib.db_api_utils import enable_cleartext_plugin

try:
    enable_cleartext_plugin()
    import MySQLdb  # noqa
except ModuleNotFoundError:
    raise ImportError(
        "You are trying to connect to a MySQL database, which requires that "
        "mysqlclient be installed, but is not present in the current "
        "environment. Install the MySQL C Connector via 'conda install "
        "-c conda-forge mysqlclient'"
    )
