
#############################################################################
# CUSTOM EXCEPTIONS
#############################################################################


class UnknownEnvironment(Exception):
    pass


#############################################################################
# CONFIG CLASS
#############################################################################


class Environment(object):

    env_name = "dev"

    @classmethod
    def set_environment(cls, env_name):
        cls.env_name = env_name

    @classmethod
    def get_cascade_root(cls):
        if cls.env_name == "prod":
            return "filepath"
        elif cls.env_name == "dev":
            return "filepath"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))

    @classmethod
    def get_code_root(cls):
        if cls.env_name == "prod":
            return "filepath"
        elif cls.env_name == "dev":
            return "filepath"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))

    @classmethod
    def get_odbc_key(cls):
        if cls.env_name == "prod":
            return "DB_KEY"
        elif cls.env_name == "dev":
            return "DB_KEY"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))
