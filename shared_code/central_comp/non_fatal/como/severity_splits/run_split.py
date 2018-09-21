import sys
import os
root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.append(root)

from lib import epi_splits

meid = sys.argv[1]
env = sys.argv[2]
mvids = epi_splits.split_me(meid, env)
