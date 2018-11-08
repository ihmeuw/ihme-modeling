import os

from cascade_ode.settings import load as load_settings

settings = load_settings()

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
        return settings['cascade_ode_out_dir']

    @classmethod
    def get_code_root(cls):
        this_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.abspath(os.path.join("../../", this_path))

    @classmethod
    def get_odbc_key(cls):
        if cls.env_name in ["prod", "dev"]:
            return "cascade-{}".format(cls.env_name)
        else:
            raise UnknownEnvironment("cannot find cascade root for {}".format(
                cls.env_name))
