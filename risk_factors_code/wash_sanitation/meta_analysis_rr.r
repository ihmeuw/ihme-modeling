#load library
library(data.table)
library(netmeta)

#Sanitation
  dat.root <- ""
  dt <- fread(paste0(dat.root, "sanitation_rr3.csv"))
  #dt <- dt[reference !="Capun2"]
  dt <- dt[reference !="Capun3"]
  #dt <- dt[reference !="Capun4"]
  
  dt_san <- dt[, c("effectsize", "standard_error", "intervention_clean", "control_edit",
                  "reference")]
  dt_san <- dt[intervention_clean != "open_def" & control_edit != "."]
  #dt_san <- dt_san[reference !="Baker11"]
  #dt_san <- dt_san[reference !="Baker6"]
  #dt_san <- dt_san[reference !="Baker4"]
  #dt_san <- dt_san[reference !="Baker3"]
  dt_san <- dt_san[, log_rr := log(effectsize)]
  dt_san <- dt_san[, log_se := sqrt((standard_error^2) * (1/effectsize)^2)]
  
  net1 <- netmeta(log_rr, log_se, intervention_clean, control_edit,
                  reference, data = dt_san, sm = "RR", comb.random = T,
                  reference.group = "unimproved")
  net1
  forest(net1, ref="unimproved")