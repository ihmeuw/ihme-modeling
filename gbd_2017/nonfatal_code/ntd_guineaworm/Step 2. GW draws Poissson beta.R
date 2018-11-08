#pre-process step for GW model ME 10523 GW incidence. Generate draws from beta distribution for years for which we have observed data


library(data.table)
setwd("FILEPATH/inputs")
gw_b_dt <- fread("gw_pop_corr.csv")

#generate from beta distribution
draws.required <- 1000
draw.cols <- paste0("draw_", 0:999)

gw_b_dt[, beta:=population-cases]
gw_b_dt[, id := .I]
gw_b_dt[, (draw.cols) := as.list(rbeta(draws.required, cases, beta)), by=id]


#output dataset
write.csv(gw_b_dt, "FILEPATH/Beta_draws.csv", na="")



