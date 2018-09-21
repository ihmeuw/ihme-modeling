import sys
from elmo import run

chronic_csmr_bd,dismod_dir, out_dir = sys.argv[1:4]

df = run.upload_epi_data(chronic_csmr_bd, '%s/epi_input_%s.xlsx'% (out_dir, chronic_csmr_bd))

