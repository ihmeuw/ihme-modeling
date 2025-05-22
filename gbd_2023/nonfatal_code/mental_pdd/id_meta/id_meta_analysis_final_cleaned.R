library(metafor)
library(data.table)
library(openxlsx)
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}
rma_results <- function(x){data.table(mean=predict(x, digits=6, transf=transf.ilogit)$pred, se=(predict(x, digits=6, transf=transf.ilogit)$ci.ub - predict(x, digits=6, transf=transf.ilogit)$ci.lb) / (qnorm(0.975,0, 1)*2), low = predict(x, digits=6, transf=transf.ilogit)$ci.lb, high = predict(x, digits=6, transf=transf.ilogit)$ci.ub)}

set.seed(4536231)

draw_cols <- paste0("draw_",0:999)

##########################################
####### Estimate proportions of ID #######
##########################################

meta <- as.data.table(read.xlsx("/FILEPATH/asd_id_dataset.xlsx"))
meta <- meta[grep("ASD", Disorder),]
meta <- meta[cv_comprehensive==1,]

### calculate total cases of ASD by study ###
meta[, asd_total := sum(n), by="nid"]

### Meta-analysis: Proportion of all ASD with any ID (IQ < 70) ###
meta_anyID <- meta[IQ_max<70, asd_anyID := sum(n), by="nid"]
meta_anyID_copy <- copy(meta_anyID) # To be used for later when dividing sub-ID groups
meta_anyID <- unique(meta_anyID[!is.na(asd_anyID) & nid %in% meta[IQ_max == 69, nid], .(nid, Author, asd_total, asd_anyID)], by=c("nid"))
res_anyID <- rma(measure="PLO", slab = Author, xi=asd_anyID, ni=asd_total, data=meta_anyID, method="DL")
results_anyID <- rma_results(res_anyID)

### Meta-analysis: Proportion of ASD with no ID that have borderline ID (IQ 70-84) ###
meta_70to85 <- meta[IQ_min>69, asd_above70 := sum(n), by="nid"]
meta_70to85[IQ_max==84, asd_borderID := sum(n), by="nid"]
meta_70to85 <- unique(meta_70to85[IQ_max==84, .(nid, Author, asd_above70, asd_borderID)], by=c("nid")) # Only keep studies that can separate at 85 IQ
res_borderID <- rma(measure="PLO", slab = Author, xi=asd_borderID, ni=asd_above70, data=meta_70to85, method="DL")
results_borderID <- rma_results(res_borderID)

### Meta-analysis: Proportion of ASD with any ID that have mild ID (IQ 50-69) ###
meta_mildID <- meta_anyID_copy[IQ_min==50 & IQ_max==69, asd_mildID := sum(n), by="nid"]
meta_mildID <-meta_mildID[IQ_min==50 & IQ_max==69, .(nid, Author, asd_anyID, asd_mildID)]
res_mildID <- rma(measure="PLO", slab = Author, xi=asd_mildID, ni=asd_anyID, data=meta_mildID, method="DL")
results_mildID <- rma_results(res_mildID)

### Meta-analysis: Proportion of ASD with any ID that have moderate to profound ID (IQ < 50) ###
##### This step is more of a check than anything else. It also makes the dataset for later #####
meta_mpID <- meta_anyID_copy[IQ_max<50, asd_mpID := sum(n), by="nid"]
meta_mpID_copy <- copy(meta_mpID) # To be used for later when dividing sub-ID groups
meta_mpID <- unique(meta_mpID[!is.na(asd_mpID), .(nid, Author, asd_anyID, asd_mpID)], by=c("nid"))
meta_mpID <- meta_mpID[nid %in% meta[IQ_max==49, nid] & nid %in% meta[IQ_max==69, nid],] # Only keep studies used above to separate any ID that can also separate at 50 IQ
res_mpID <- rma(measure="PLO", xi=asd_mpID, ni=asd_anyID, data=meta_mpID, method="DL")
results_mpID <- rma_results(res_mpID)

### Meta-analysis: Proportion of ASD with moderate to profound ID that have moderate ID (IQ 35-49) ###
meta_modID <- meta_mpID_copy[IQ_min==35 & IQ_max==49, asd_modID := sum(n), by="nid"]
meta_modID <-meta_modID[IQ_min==35 & IQ_max==49, .(nid, Author, asd_mpID, asd_modID)]
res_modID <- rma(measure="PLO", slab = Author,xi=asd_modID, ni=asd_mpID, data=meta_modID, method="DL")
results_modID <- rma_results(res_modID)

##########################################
###### Split Severe and Profound ID ######
##########################################

## No ASD estimates for this level so use autism estimates

### Load autism registry data ###
meta_autism <- as.data.table(read.xlsx("/FILEPATH/asd_id_dataset.xlsx"))
meta_autism <- meta_autism[grepl("Autism", Disorder),]

### meta-analysis for profound ID (IQ <20) ###
meta_autism <- meta_autism[IQ_max<35,]
meta_autism[, `:=` (autism_total = sum(n)), by="nid"]
meta_autism[IQ_max==19, `:=` (autism_profID = sum(n)), by="nid"]
meta_autism <- meta_autism[IQ_max==19 & autism_total != 0,]
res_profID <- rma(measure="PLO", slab = Author, xi=autism_profID, ni=autism_total, data=meta_autism, method="DL")
results_autism_profID <- rma_results(res_profID)

##########################################
####### 1000 draws of proportions ########
##########################################

anyID_1000 <- rlogit(rnorm(1000, res_anyID$beta, res_anyID$se))
NoID_1000 <- (rep(1, 1000) - anyID_1000) * (rep(1, 1000) - rlogit(rnorm(1000, res_borderID$beta, res_borderID$se)))
BorderID_1000 <- (rep(1, 1000) - anyID_1000) - NoID_1000
mildID_1000 <- anyID_1000 * rlogit(rnorm(1000, res_mildID$beta, res_mildID$se))
modID_1000 <- (anyID_1000 - mildID_1000) * rlogit(rnorm(1000, res_modID$beta, res_modID$se))
severeID_1000 <- (anyID_1000 - mildID_1000 - modID_1000) * (rep(1,1000) - rlogit(rnorm(1000, res_profID$beta, res_profID$se)))
profID_1000 <- anyID_1000 - mildID_1000 - modID_1000 - severeID_1000

prob_draws <- rbind(data.table(sequelae="No ID", draw=draw_cols, prop=NoID_1000), data.table(sequelae="Borderline ID", draw=draw_cols, prop=BorderID_1000), data.table(sequelae="Mild ID", draw=draw_cols, prop=mildID_1000),
                    data.table(sequelae="Moderate ID", draw=draw_cols, prop=modID_1000), data.table(sequelae="Severe ID", draw=draw_cols, prop=severeID_1000), data.table(sequelae="Profound ID", draw=draw_cols, prop=profID_1000))
prob_draws[prop<0, prop:=0] 

##########################################
###### Estimate Disability Weights #######
##########################################

### Load disability weights (dw) ###
dw <- fread("/FILEPATH/dw_full.csv")[hhseqid %in% c(41:42, 286:290),]
names(dw) <- gsub("draw", "draw_", names(dw))
dw[healthstate=="id_bord", sequelae:="Borderline ID"]
dw[healthstate=="id_mild", sequelae:="Mild ID"]
dw[healthstate=="id_mod", sequelae:="Moderate ID"]
dw[healthstate=="id_sev", sequelae:="Severe ID"]
dw[healthstate=="id_prof", sequelae:="Profound ID"]

### Create 1000 draws of autism cv ###
autism_in_asd <- as.data.table(read.xlsx("/FILEPATH/autism_in_asd.xlsx"))
res_autism_in_asd <- rma(measure="PLO", slab = study, xi=cases_autism, ni=cases_asd, data=autism_in_asd, method="DL")

results_autism_in_asd <- rma_results(res_autism_in_asd)
autism_prop_1000 <- rlogit(rnorm(1000, res_autism_in_asd$beta, res_autism_in_asd$se))

### Isolate 1000 draws of autism dw and asperger's dw ###
asperger_dw <- sort(as.numeric(dw[healthstate=="aspergers", draw_cols, with=F]))
autism_dw <- sort(as.numeric(dw[healthstate=="autism", draw_cols, with=F]))

asd_dw <- data.table(hhseqid=NA, healthstate_id=NA, healthstate="asd", draw=draw_cols, dw_autism = autism_dw, dw_asperger = asperger_dw, prop_autism = autism_prop_1000)
asd_dw[, val := dw_autism * prop_autism + dw_asperger * (1-prop_autism)]
                   
# to inspect autism vs other ASD vs ASD DWs
data.table(cause = c("Autism", "Other ASD", "ASD"), mean=c(mean(autism_dw), mean(asperger_dw), mean(asd_dw$val)), lower=c(quantile(autism_dw, 0.025), quantile(asperger_dw, 0.025), quantile(asd_dw[,val], 0.025)), upper=c(quantile(autism_dw, 0.975), quantile(asperger_dw, 0.975), quantile(asd_dw[,val], 0.975)))

### Estimate 1000 dws for ASD and each level of ID ###
id_dws <- melt.data.table(dw[hhseqid %in% c(286:290),], id.vars = names(dw[hhseqid %in% c(286:290),])[!(names(dw) %like% "draw")], value.name="dw", variable.name="draw")

id_dws <- merge(id_dws, prob_draws, by = c("sequelae", "draw"))
id_dws[, `:=` (p_x_dw = dw * prop, p_x_resid_dw = (1-dw)*prop)]
id_dws[, `:=` (sum_p_x_dw = sum(p_x_dw), sum_p_x_resid_dw = sum(p_x_resid_dw)), by = 'draw']

id_dws <- merge(id_dws, asd_dw[,.(draw, dw_asd = val)], by = 'draw')
id_dws <- merge(id_dws, prob_draws[sequelae == "No ID",.(draw, prop_no_id = prop)], by = 'draw')
id_dws[, dw_asd_no_id := (dw_asd - sum_p_x_dw)/(prop_no_id + sum_p_x_resid_dw)]
id_dws[, dw_asd_id := 1 - (1-dw_asd_no_id)*(1-dw)]
id_dws[, dw_asd_id_additive := 1 - (1-dw_asd)*(1-dw)]

id_dws <- rbind(unique(id_dws[,.(draw, sequelae = 'No ID', prop = prop_no_id, dw = dw_asd)]), id_dws[,.(draw, sequelae, prop, dw = dw_asd_id_additive)])

dw_backup <- copy(id_dws) 

id_dws[, `:=` (prop_mean = mean(prop), prop_lower = quantile(prop, 0.025), prop_upper = quantile(prop, 0.975), dw_mean = mean(dw), dw_lower = quantile(dw, 0.025), dw_upper = quantile(dw, 0.975)), by = 'sequelae']

summary_prop <- unique(id_dws[,.(prop = prop_mean, lower = prop_lower, upper = prop_upper), by = 'sequelae'])
summary_dw <- unique(id_dws[,.(dw = dw_mean, lower = dw_lower, upper = dw_upper), by = 'sequelae'])

summary_prop[sequelae == "No ID", target_meid := 19781]
summary_prop[sequelae == "Borderline ID", target_meid := 19782]
summary_prop[sequelae == "Mild ID", target_meid := 19783]
summary_prop[sequelae == "Moderate ID", target_meid := 19784]
summary_prop[sequelae == "Severe ID", target_meid := 19785]
summary_prop[sequelae == "Profound ID", target_meid := 19786]

summary_prop[, `:=` (source_meid = 18668, location_id = 1, year_start = 1990, year_end = 2022, age_start = 0, age_end = 100, sex_id = 3)]

severitysplit_form_asd <- rbind(summary_prop[order(target_meid),.(source_meid, target_meid, location_id, year_start, year_end, age_start, age_end, sex_id, measure_id = 5, mean = prop, lower, upper)],
                                summary_prop[order(target_meid),.(source_meid, target_meid, location_id, year_start, year_end, age_start, age_end, sex_id, measure_id = 6, mean = prop, lower, upper)])

asd_healthstate_request <- copy(id_dws)
asd_healthstate_request[sequelae == "No ID", `:=` (healthstate_id = 2612, healthstate = "asd_no_id", healthstate_name	= "Autism spectrum disorder without intellectual disability")]
asd_healthstate_request[sequelae == "Borderline ID", `:=` (healthstate_id = 2618, healthstate = "asd_id_bord", healthstate_name	= "Autism spectrum disorder with borderline intellectual disability")]
asd_healthstate_request[sequelae == "Mild ID", `:=` (healthstate_id = 2621, healthstate = "asd_id_mild", healthstate_name	= "Autism spectrum disorder with mild intellectual disability")]
asd_healthstate_request[sequelae == "Moderate ID", `:=` (healthstate_id = 2624, healthstate = "asd_id_mod", healthstate_name	= "Autism spectrum disorder with moderate intellectual disabilit")]
asd_healthstate_request[sequelae == "Severe ID", `:=` (healthstate_id = 2627, healthstate = "asd_id_sev", healthstate_name	= "Autism spectrum disorder with severe intellectual disability")]
asd_healthstate_request[sequelae == "Profound ID", `:=` (healthstate_id = 2630, healthstate = "asd_id_prof", healthstate_name	= "Autism spectrum disorder with profound intellectual disability")]
asd_healthstate_request[, healthstate_description := "(combined DW)"]

asd_healthstate_request <- dcast(asd_healthstate_request[,.(draw, healthstate_id, healthstate, healthstate_name, healthstate_description, dw)], ...~draw, value.var="dw")

write.csv(severitysplit_form_asd, "/FILEPATH/severitysplit_form_asd.csv", row.names=F)
write.csv(asd_healthstate_request, "/FILEPATH/asd_healthstate_request.csv", row.names=F)
write.csv(summary_dw, "/FILEPATH/summary_dw.csv", row.names=F)

### For Average ASD DW ###

dw_backup[, sum_dw_x_prop := sum(prop*dw), by = 'draw']
unique(dw_backup[,.(dw = mean(sum_dw_x_prop), lower = quantile(sum_dw_x_prop, 0.025), upper = quantile(sum_dw_x_prop, 0.975))])

