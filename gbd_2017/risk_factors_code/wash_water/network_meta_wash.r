#load library
library(data.table)
library(netmeta)
library(forestplot)

#Sanitation
  dat.root <- "FILEPATH"
  dt <- fread(paste0(dat.root, "sanitation_rr.csv"))
  

  dt <- dt[reference !="Capun3"]
  dt[reference == "Capun4", reference := "Capun3"]
  
  #outlier open defecation studies
  dt_san <- dt[intervention_clean != "open_def" & control_edit != "."]
  dt_san[, se := (upper95confidenceinterval - effectsize)/1.96]
  dt_san <- dt_san[exclude == 0]
  
  #Transform to log
  dt_san <- dt_san[, log_rr := log(effectsize)]
  dt_san <- dt_san[, log_se := sqrt((se^2) * (1/effectsize)^2)]
  
  #Clean variable values in prep for meta-analysis
  dt_san[intervention_clean == "improved", intervention_clean := "Improved"]
  dt_san[intervention_clean == "unimproved", intervention_clean := "Unimproved"]
  dt_san[intervention_clean == "sewer", intervention_clean := "Sewer_connection"]
  dt_san[control_edit == "unimproved", control_edit := "Unimproved"]
  dt_san[control_edit == "improved", control_edit := "Improved"]
  
  net1 <- netmeta(log_rr, log_se, intervention_clean, control_edit,
                  reference, data = dt_san, sm = "RR", comb.random = T,
                  reference.group = "Unimproved")
  net1
  forest(net1, ref="Unimproved")
  dt_san <- dt_san[reference!="Baker6"]
  san_temp <- dt_san[intervention_clean=="Improved"]
  forestplot(san_temp$reference, san_temp$effectsize, san_temp$lower95confidenceinterval, san_temp$upper95confidenceinterval, zero = 1)

#Water
  #network meta-analysis
  dat.root <- "FILEPATH"
  dt <- fread(paste0(dat.root, "water_rr.csv"))
  dt[, se := (upper95confidenceinterval - effectsize)/1.96]
  dt <- dt[, log_rr := log(effectsize)]
  dt <- dt[, log_se := sqrt((se^2) * (1/effectsize)^2)]
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
  
  #Clean up variable values for prettier forest plot
  dt_source[intervention_clean == "filter", intervention_clean := "Filter/boil"]
  dt_source[intervention_clean == "solar", intervention_clean := "Solar/chlorine"]
  dt_source[intervention_clean == "improved", intervention_clean := "Improved"]
  dt_source[intervention_clean == "hq_piped", intervention_clean := "HQ_piped"]
  dt_source[intervention_clean == "piped", intervention_clean := "Bas_piped"]
  dt_source[control_clean == "unimproved", control_clean := "Unimproved"]
  dt_source[control_clean == "piped", control_clean := "Bas_piped"]
  dt_source[control_clean == "improved", control_clean := "Improved"]
  
  net1 <- netmeta(log_rr, log_se, intervention_clean, control_clean,
                  reference, data = dt_source, sm = "RR", comb.random = T, 
                  reference.group = "Unimproved")
  net1
  
  forest(net1, ref="Unimproved", xlab ="Relative Risk")
  
  netconnection(control_clean, intervention_clean, reference, data = dt)
 