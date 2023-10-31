from collections import defaultdict
import os


class InvalidPathException(Exception):
    pass


class ServerException(Exception):
    pass


class DuplicateServerException(Exception):
    pass


class ODBC:
    def __init__(self):
        pass

    def load(self, path):
        if not isinstance(path, str):
            raise InvalidPathException("Path can't be of type {}".format(type(path)))
        path = os.path.expanduser(path)

        if not os.path.exists(path):
            raise InvalidPathException("Path does not exist: {}".format(path))

        servers = {}
        with open(path) as file:
            server_name = ""
            server_params = {}
            for line in file:
                if line[0] == "[":
                    # Store the last server
                    if server_name in servers:
                        raise DuplicateServerException(
                            "Multiple server settings for {} found in {}. Please delete one".format(
                                server_name, path
                            )
                        )
                    servers[server_name] = server_params

                    # Reset server stuff
                    server_name = ""
                    server_params = {}

                    # Get new server stuff
                    for letter in line[1:]:
                        if letter == "]":
                            break
                        server_name += letter
                elif line == "\n":
                    continue
                else:
                    chunks = line.split("=")
                    if len(chunks) == 2:
                        param_key, param_value = [s.strip().lower() for s in chunks]
                        server_params[param_key] = param_value
        # Store the last parsed profile
        servers[server_name] = server_params
        self._servers = servers

    def __getattr__(self, key):
        if key in self._servers:
            return self._servers[key]
        raise ServerException("ODBC doesn't have a server named " + key)

    def __getitem__(self, key):
        return self.__getattr__(key)


odbc = ODBC()
default_odbc_path = "FILEPATH"
odbc.load(default_odbc_path)
