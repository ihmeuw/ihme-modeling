# Calculate the combined RR value (RR value the takes into account both mediated and direct RRs)

rm(list=ls())

#libraries ################################
library(readr)

model_type<-c("cov","outlier")

# begin for loop ###############################
for (model in model_type) {
  print(model)
  
  in.dir<-ifelse(model=="outlier",
  "FILEPATH/outer_draws.csv", # model where I remove the non-SBP adj study
  "FILEPATH/outer_draws.csv" #model where we used a covariate to identify the non_sbp study
)
  
  print(in.dir) 
  filename<-ifelse(model=="outlier", "rr_total_outlier_draws.csv", "rr_total_cov_draws.csv")
  print(filename)

# Load in data ##########################
# Load in the RR draws from the BoP model
bop<-fread(in.dir)

# load in mediation factor
mf<-fread("FILEPATH/mediation_factor.csv")[,c(mean_mf)]

# Convert to Long format ####################
#convert the BoP data into the long format
bop<-melt(bop,id.vars = "risk", 
          measure.vars = patterns("^draw_"), 
          variable.name = "draw", 
          value.name = "val")

# exp values ####################
#currently the values are in log space and we need to convert them to linear space
bop[,val:=exp(val)]

# Calc RRtotal #################################
# RRunmediated = (RRtotal - 1)(1 - MF)+1
#when you rearrange this equation we can calculate RR total
# RRtotal = ((RRunmediated - 1) / (1 - MF)) + 1
# RRtotal = the RR for the combined effect of direct and SBP mediation
# RRunmediated = the RR from our SBP BoP model
# MF = mediation factor


bop[,rr_total:=log(((val-1)/(1-mf))+1)]
#taking the log so it will match the original MRBRT draws, so it will run in the PAF pipeline correctly

# clean up ##################################
#remove the val column, as its no longer needed
bop<-bop[,-c("val")]

#convert back to wide format
bop<-dcast(bop,risk ~ draw, value.var = "rr_total")

# export ##############################
write_excel_csv(bop,paste0("FILEPATH/",filename))


}
