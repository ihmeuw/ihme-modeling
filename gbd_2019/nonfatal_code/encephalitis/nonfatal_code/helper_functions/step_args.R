step = "09"
# date <- gsub("-", "_", Sys.Date())
date <- "2019_11_10"
ds <- "step4"

library(data.table)

step_args <- fread(paste0(h,"step_args.csv"))

step_name <- step_args[step_num == step, step_name]
step_num <- step

code_dir <- paste0(h, "encephalitis_gbd2019/nonfatal_code/")
in_dir <- "filepath"
enceph_dir <- paste0("filepath", date)
root_tmp_dir = paste0(enceph_dir,"/draws")
root_j_dir <- paste0(enceph_dir,"/logs")

out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

locations <- 44735

# for step 09
if (step == "09") {
  dim.dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "encephalitis_dimension.csv"))
  dim.dt <- dim.dt[healthstate != "_parent" & !(grouping %in% c("_epilepsy", "cases", "_vision"))]
  
  dim.dt[grouping %in% c("long_mild", "long_modsev"), num:= "05b"]
  dim.dt[grouping %in% c("long_mild", "long_modsev"), name:= "outcome_prev_womort"]
  
  cause <- dim.dt[1, acause]
  group <- dim.dt[1, grouping]
  state <- dim.dt[1, healthstate]
}
