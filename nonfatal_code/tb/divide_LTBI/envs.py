
#############################################################################
# CUSTOM EXCEPTIONS
#############################################################################


class UnknownEnvironment(Exception):
    pass


#############################################################################
# CONFIG CLASS
#############################################################################


class Environment(object):

    env_name = "prod"

    @classmethod
    def set_environment(cls, env_name):
        cls.env_name = env_name

    @classmethod
    def get_cascade_root(cls):
        if cls.env_name == "prod":
            return "ADDRESS"
        elif cls.env_name == "test":
            return "ADDRESS"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))

    @classmethod
    def get_code_root(cls):
        if cls.env_name == "prod":
            return "ADDRESS"
        elif cls.env_name == "test":
            return "ADDRESS"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))

    @classmethod
    def get_odbc_key(cls):
        if cls.env_name == "prod":
            return "epi-cascade"
        elif cls.env_name == "test":
            return "epi-cascade"
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))
