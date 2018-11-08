import sys
from elmo import run
import db_queries

step, bundle, out_dir = sys.argv[1:4]
print step
print bundle
print out_dir

df = run.upload_epi_data(bundle, '%s/step_%s_input_%s.xlsx'% (out_dir, step, bundle))

