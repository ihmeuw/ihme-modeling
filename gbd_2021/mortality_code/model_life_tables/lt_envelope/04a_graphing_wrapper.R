#### Wrapper for MLT Graphing ####
# This allows the MLT graphs, which take 3+ hours to generate, to not hold up the rest of the mortality runs.
# Wrapping in a qsub like this allows Jobmon to continue on with FLT and finalizer rather than wait for graphs.



library(mortcore)
library(argparse)

parser <- argparse::ArgumentParser()

parser <- ArgumentParser()
parser$add_argument('--mlt_version', type='integer', required=TRUE,
                    help='The MLT life table estimate version of this run')
parser$add_argument('--mlt_env_version', type='integer', required=TRUE,
                    help='The MLT envelope estimate version of this run')
parser$add_argument('--vr_version', type='integer', required=TRUE,
                    help='The empirical deaths version to compare to')
parser$add_argument('--gbd_year', type='integer', required=TRUE,
                    help = "GBD year of analysis")
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help = "Directory where MLT code is cloned")


args <- parser$parse_args()
username <- Sys.getenv("USER")
code_dir <- args$code_dir
args$code_dir <- NULL

execR_path <- "FILEPATH"
image_path <- "FILEPATH"

# Submit qsub
mortcore::qsub(paste0("plot_mlt_envelope", args$mlt_version),
               code = paste0(code_dir, '/04_graph_env_vr.R'),
               mem = 30,
               cores = 2,
               wallclock = "06:00:00",
               pass_argparse = args,
               shell = execR_path,
               pass_shell = list(i = image_path),
               submit=T)

quit(status=0)
