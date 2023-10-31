## QSUB
slot_number <- 4
next_script <- paste0("-s ", h, "FILEPATH")

mes_to_upload <- c("MODELABLE ENTITY IDS")

print("Starting save")

for (me in mes_to_upload){
  job_name <- paste0("save_dental_", me)
  
  system(paste( "qsub -P ADDRESS -l m_mem_free=60G -l fthread=8 -l h_rt=05:00:00 -q long.q -N", job_name, "-e FILEPATH", "-o FILEPATH", 'FILEPATH', next_script, me))
}
