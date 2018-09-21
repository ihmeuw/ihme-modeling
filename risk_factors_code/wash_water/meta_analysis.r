#load library
library(data.table)
library(netmeta)

#Water
  #network meta-analysis
  dat.root <- ""
  dt <- fread(paste0(dat.root, "water_rr.csv"))
  dt <- dt[, log_rr := log(effectsize)]
  dt <- dt[, log_se := sqrt((standard_error^2) * (1/effectsize)^2)]
  #dt <- dt[added_2016 == 0,]
  dt <- dt[reference !="Capuno J3"]
  dt <- dt[reference !="Capuno J4"]
  dt_source <- dt[intervention_clean == "improved" | intervention_clean == "piped" |
                  intervention_clean == "hq_piped" |intervention_clean == "solar" |
                  intervention_clean == "filter", c("log_rr", 
                                                      "log_se",
                                                      "standard_error", 
                                                      "intervention_clean", 
                                                      "control_clean",
                                                      "reference")]
  
  net1 <- netmeta(log_rr, log_se, intervention_clean, control_clean,
                  reference, data = dt_source, sm = "RR", comb.random = T, 
                  reference.group = "unimproved")
  net1
  forest(net1, ref="unimproved")
  
  netconnection(control_group, intervention_clean, reference, data = dt)