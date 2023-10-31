
# Launch the iron deficiency PAF, or scatter it.
library(data.table)
source('FILEPATH/launch_paf.R')
source('FILEPATH/paf_scatter.R')


#------------------------------
resume_arg <- FALSE
run_paf <- T
gbd_round_id <- 7
decomp_step <- "iterative"
#------------------------------


if(F){
  # Launch this as a job
  log_dir <- paste0("FILEPATH",Sys.info()['user'])
  system(paste0("qsub -N launch_iron_PAF",
                " -P proj_diet -l m_mem_free=10G -l fthread=5 -q long.q",
                " -o ", log_dir, "/output -e ", log_dir, "/errors ",
                "FILEPATH/execRscript.sh ",
                "-s FILEPATH/launch_run_iron_PAF.R"))
  
}

if(run_paf){
  # iron is 95!
  launch_paf(rei_id=95, decomp_step = decomp_step, cluster_proj="proj_diet", gbd_round_id = gbd_round_id, resume = resume_arg)
  
  
  }else{
  print("Scattering the paf!")
  filepath <- "FILEPATH/iron_diff_vs_norm"
  diff_version <- 579074
  norm_version <- 564257
  shifted_version <- 570296
  shifted_version <- 579032
  paf_scatter(rei_id=95, file_path=paste0(filepath, ".PDF"), decomp_step='iterative', gbd_round_id = 7, version_id = diff_version, measure=4, year_id = 2019,
                                         prev_gbd_round_id=7, prev_decomp_step = 'step4', prev_version_id = norm_version)

  }