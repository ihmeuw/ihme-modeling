import traceback
import datetime
import sys
from os import path

class _rlog:
    def __init__(self):
        self.log_stream = None

    def open(self, file):
        self.log_stream = open(file, 'a')

        try:
            mainname = sys.modules['__main__'].__file__
        except:
            mainname = "(undefined file)"

        header = "\n/*\n"
        header = header + "New run of " + mainname + "\n"
        header = header + "Started at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "\n"
        header = header + "time\t\tlevel\tmessage\t\t\t[traceback]\n"
        header = header + "*/\n"

        self.log_stream.write(header)
        self.log_stream.flush()

    def get_stack(self, ourcount):
        stack = traceback.extract_stack()
        # Remove last elements of stack
        stack = stack[0:len(stack)-ourcount]
        return stack

    @staticmethod
    def format_stack(stack):
        return "[" + " <- ".join([":".join([path.split(x[0])[1], str(x[3]), str(x[1])]) for x in reversed(stack)]) + "]"

    def debug(self, text):
        self.write("dbg", str(text))

    def log(self, text):
        self.write("log", str(text))

    def warn(self, text):
        self.write("wrn", str(text))

    def error(self, text):
        self.write("err", str(text))

    def write(self, level, text):
        time = datetime.datetime.now().strftime("%H:%M:%S")
        line = time + "\t" + level + "\t" + text + "\t\t" + _rlog.format_stack(self.get_stack(4))
        self.log_stream.write(line + "\n")
        self.log_stream.flush()

    # Close the file
    def __del__(self):
        if self.log_stream is not None and not self.log_stream.closed:
            self.log_stream.flush()
            self.log_stream.close()

class _rlog_metaclass(type):
    _instance = None
    # Refer all static calls to the real class
    def __getattr__(self, name):
        # If the logger obj hasn't been created yet, create it
        if self.__class__._instance is None:
            self.__class__._instance = _rlog()

        if hasattr(self.__class__._instance, name):
            return getattr(self.__class__._instance, name)

class rlog:
    __metaclass__ = _rlog_metaclass
