step = "04b"

library(data.table)

step_args <- fread("filepath/step_args.csv")

step_name <- step_args[step_num == step, step_name]
step_num <- step

code_dir <- "filepath"
in_dir <- "filepath"
enceph_dir <- "filepath"
root_tmp_dir <- "filepath"
root_j_dir <- "filepath"

out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

# locations <- 44735

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
