source('FILEPATH/agesex_split.R')
args <- commandArgs(trailingOnly = TRUE)
bv_id <- args[1]
measure_name <- args[2]
type <- args[3]
release_id <- args[4]
cascade <- args[5]
lit_cascade <- args[6]
outdir <- args[7]
old_release_id<- args[8]



age_sex_split(bv_id,
              measure_name,
              type,
              release_id,
              cascade,
              lit_cascade,
              outdir=outdir,
              old_release_id=old_release_id,
              debug=FALSE)


