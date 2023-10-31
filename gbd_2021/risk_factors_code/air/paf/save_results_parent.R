# clear memory
rm(list=ls())

user <- 'USERNAME'

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}


pacman::p_load(data.table, magrittr)

project <- "-P PROJECT "
sge.output.dir <- paste0(" -o FILEPATH", user, "FILEPATH -e FILEPATH", user, "FILEPATH ")


save.script <- "-s FILEPATH/save_results.R"
r.shell <- "FILEPATH/execRscript.sh"

paf.version <- VERSION
decomp <- "iterative"

#create datatable of unique jobs
risks <- data.table(risk=c("air_pmhap","air_pm","air_hap"), me_id=c(20260,8746,8747), rei_id=c(380,86,87))

save <- function(i){

  args <- paste(risks[i,risk],
                risks[i,me_id],
                risks[i,rei_id],
                paf.version,
                decomp)
  mem <- "-l m_mem_free=75G"
  fthread <- "-l fthread=10"
  runtime <- "-l h_rt=24:00:00"
  archive <- "-l archive=TRUE" 
  jname <- paste0("-N ","save_results_",risks[i,risk])
  
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,r.shell,save.script,args))
  
  
  
}

complete <- lapply(1:nrow(risks),save)


