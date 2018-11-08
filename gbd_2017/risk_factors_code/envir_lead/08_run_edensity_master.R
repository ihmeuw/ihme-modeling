# Project: RF: Lead Exposure
# Purpose: Launch edensity to calculate IQ shifts

rm(list=ls())

cores.provided <- 4
model.version <- 1
model.id <- 43253 #########
input_dir <- file.path("FILEPATH")
output_dir <- file.path("FILEPATH")
code.dir <- file.path("FILEPATH")

dir.create(output_dir, recursive = T, showWarnings = FALSE)
calc.script <- file.path("FILEPATH")
r.shell <- file.path("FILEPATH")

files <- list.files(input_dir)

for (file in files){
  # Launch jobs
  jname <- paste0("calc_dist_model_",model.version,"_",model.id,"_loc_",file)
  sys.sub <- paste0("qsub ",project, sge.output.dir, " -N ", jname, " -pe multi_slot ", cores.provided*4, " -l mem_free=", cores.provided*36, "G")
  args <- paste(file,
                model.version,
                model.id,
                cores.provided)
  system(paste(sys.sub, r.shell, calc.script, args))
}
