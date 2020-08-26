library(metafor)
library(data.table)
library(openxlsx)
rlogit <- function(x){exp(x)/(1+exp(x))}
rma_results <- function(x){data.table(mean=predict(res, digits=6, transf=transf.ilogit)$pred, se=(predict(res, digits=6, transf=transf.ilogit)$ci.ub - predict(res, digits=6, transf=transf.ilogit)$ci.lb) / (qnorm(0.975,0, 1)*2))}

coolbeta <- function(x, se){
  a <- x*(x-x^2-se^2)/se^2
  b <- a*(1-x)/x
  rbeta(1000, a, b)
}

autism_cv <- data.table(val = 0.4337648, lower = 0.3642287, upper = 0.5060104)
draw_cols <- paste0("draw_",0:999)

##########################################
####### Estimate proportions of ID #######
##########################################

meta <- as.data.table(read.xlsx("FILEPATH"), sheet=5))
meta <- meta[nid!="excluded",]
meta <- meta[grep("ASD", Disorder),]
meta <- meta[cv_comprehensive==1,]

### calculate total cases of ASD by study ###
meta[, asd_total := sum(n), by="nid"]

### Meta-analysis: Proportion of all ASD with any ID (IQ < 70) ###
meta_anyID <- meta[IQ_max<70, asd_anyID := sum(n), by="nid"]
meta_anyID[nid=="283136", asd_anyID := 0] # Study only reports IQ 100+
meta_anyID_copy <- copy(meta_anyID) # To be used for later when dividing sub-ID groups
meta_anyID <- unique(meta_anyID[!is.na(asd_anyID) & nid %in% meta[IQ_max==69, nid], .(nid, Author, asd_total, asd_anyID)], by=c("nid"))
res <- rma(measure="PLO", slab = nid, xi=asd_anyID, ni=asd_total, data=meta_anyID, method="DL")
results_anyID <- rma_results(res)

### Meta-analysis: Proportion of ASD with no ID that have borderline ID (IQ 70-84) ###
meta_70to85 <- meta[IQ_min>69, asd_above70 := sum(n), by="nid"]
meta_70to85[IQ_max==84, asd_borderID := sum(n), by="nid"]
meta_70to85 <- unique(meta_70to85[IQ_max==84, .(nid, Author, asd_above70, asd_borderID)], by=c("nid")) # Only keep studies that can separate at 85 IQ
res <- rma(measure="PLO", xi=asd_borderID, ni=asd_above70, data=meta_70to85, method="DL")
results_borderID <- rma_results(res)

### Meta-analysis: Proportion of ASD with any ID that have mild ID (IQ 50-69) ###
meta_mildID <- meta_anyID_copy[IQ_min==50 & IQ_max==69, asd_mildID := sum(n), by="nid"]
meta_mildID <-meta_mildID[IQ_min==50 & IQ_max==69, .(nid, Author, asd_anyID, asd_mildID)]
res <- rma(measure="PLO", xi=asd_mildID, ni=asd_anyID, data=meta_mildID, method="DL")
results_mildID <- rma_results(res)

### Meta-analysis: Proportion of ASD with any ID that have moderate to profound ID (IQ < 50) ###
##### This step is more of a check than anything else. It also makes the dataset for later #####
meta_mpID <- meta_anyID_copy[IQ_max<50, asd_mpID := sum(n), by="nid"]
meta_mpID_copy <- copy(meta_mpID) # To be used later when dividing sub-ID groups
meta_mpID <- unique(meta_mpID[!is.na(asd_mpID), .(nid, Author, asd_anyID, asd_mpID)], by=c("nid"))
meta_mpID <- meta_mpID[nid %in% meta[IQ_max==49, nid] & nid %in% meta[IQ_max==69, nid],] # Only keep studies used above to separate any ID that can also separate at 50 IQ
res <- rma(measure="PLO", xi=asd_mpID, ni=asd_anyID, data=meta_mpID, method="DL")
results_mpID <- rma_results(res)
if(results_mildID$mean+results_mpID$mean!=1){
  error <- "ID mild and ID moderate-profound proportions do not equal 100%"
  stop("ID mild and ID moderate-profound proportions do not equal 100%")
}

### Meta-analysis: Proportion of ASD with moderate to profound ID that have moderate ID (IQ 35-49) ###
meta_modID <- meta_mpID_copy[IQ_min==35 & IQ_max==49, asd_modID := sum(n), by="nid"]
meta_modID <-meta_modID[IQ_min==35 & IQ_max==49, .(nid, Author, asd_mpID, asd_modID)]
res <- rma(measure="PLO", xi=asd_modID, ni=asd_mpID, data=meta_modID, method="DL")
results_modID <- rma_results(res)

##########################################
###### Split Severe and Profound ID ######
##########################################

###   There are no estimates that separate severe ID from profound ID for ASD.
###   On the 5th of December it was decided that we could assume cases of severe 
###   or profound ID would be picked up by services and therefore captured in 
###   clinical / registry data. We would search for the clinical / registry 
###   data for studies that report the proportion of cases with severe ID and 
###   profound ID. However this search found no studies reporting this data for 
###   ASD. The search did however reveal 4 clinical / registry studies that do
###   report the proportion of cases with severe ID and profound ID among cases
###   of autism. There are 6 additional studies that look at IQ by ASD sub-type,
###   And together they seem to suggest that the majority of cases with ASD should
###   have autism (Chakrabarti & Fombonne, 2001; 2005; Kadesjö et al., 1999; 
###   Ellefsen et al., 2007; Kocovská et al., 2012; Mattila et al., 2011). On the 
###   19th of December the decision was made to the use of autism clinical / registry
###   data to inform this split given the absense of other data to inform this split.

### Load autism registry data ###
meta_autism <- as.data.table(read.xlsx(paste0("FILEPATH"), sheet=5))
meta_autism <- meta_autism[nid!="excluded",]
meta_autism <- meta_autism[grep("Autism", Disorder),]

### meta-analysis for profound ID (IQ <20) ###
meta_autism <- meta_autism[IQ_max<35,]
meta_autism[, `:=` (autism_total = sum(n)), by="nid"]
meta_autism[IQ_max==19, `:=` (autism_profID = sum(n)), by="nid"]
meta_autism <- meta_autism[IQ_max==19 & autism_total != 0,]
res <- rma(measure="PLO", xi=autism_profID, ni=autism_total, data=meta_autism, method="DL", slab=nid)
results_autism_profID <- rma_results(res)

##########################################
####### 1000 draws of proportions ########
##########################################
anyID_1000 <- coolbeta(results_anyID$mean, results_anyID$se)
NoID_1000 <- (rep(1, 1000) - anyID_1000) * (rep(1, 1000) - coolbeta(results_borderID$mean, results_borderID$se))
BorderID_1000 <- (rep(1, 1000) - anyID_1000) - NoID_1000
mildID_1000 <- anyID_1000 * coolbeta(results_mildID$mean, results_mildID$se)
modID_1000 <- (anyID_1000 - mildID_1000) * coolbeta(results_modID$mean, results_modID$se)
severeID_1000 <- (anyID_1000 - mildID_1000 - modID_1000) * (rep(1,1000) - coolbeta(results_autism_profID$mean, results_autism_profID$se))
profID_1000 <- anyID_1000 - mildID_1000 - modID_1000 - severeID_1000

prob_draws <- rbind(data.table(sequelae="No ID", draw=draw_cols, val=NoID_1000), data.table(sequelae="Borderline ID", draw=draw_cols, val=BorderID_1000), data.table(sequelae="Mild ID", draw=draw_cols, val=mildID_1000),
                    data.table(sequelae="Moderate ID", draw=draw_cols, val=modID_1000), data.table(sequelae="Severe ID", draw=draw_cols, val=severeID_1000), data.table(sequelae="Profound ID", draw=draw_cols, val=profID_1000))
prob_draws <- dcast(prob_draws, sequelae~draw, value.var="val")[c(4,1,2,3, 6, 5),] # adjust order of sequelae

##########################################
###### Estimate Disability Weights #######
##########################################

### Load disability weights (dw) ###
dw <- fread(paste0("FILEPATH"))[hhseqid %in% c(41:42, 286:290),]
names(dw) <- gsub("draw", "draw_", names(dw))
dw[healthstate=="id_bord", sequelae:="Borderline ID"]
dw[healthstate=="id_mild", sequelae:="Mild ID"]
dw[healthstate=="id_mod", sequelae:="Moderate ID"]
dw[healthstate=="id_sev", sequelae:="Severe ID"]
dw[healthstate=="id_prof", sequelae:="Profound ID"]

### Create 1000 draws of autism cv ###
autism_cv[, se:=(upper - lower)/(qnorm(0.975,0, 1)*2)]
autism_cv <- autism_cv[,.(draw=draw_cols, val=coolbeta(val, se))]$val

### Isolate 1000 draws of autism dw and asperger's dw ###
asperger_dw <- as.numeric(dw[healthstate=="aspergers", draw_cols, with=F])
autism_dw <- as.numeric(dw[healthstate=="autism", draw_cols, with=F])
asd_dw <- data.table(hhseqid=NA, healthstate_id=NA, healthstate="asd", draw=draw_cols, val=autism_dw*autism_cv + asperger_dw*(1-autism_cv))

### Estimate 1000 dws for ASD and each level of ID ###
final_dw <- prob_draws[,1]
for(d in draw_cols){
  final_dw[sequelae=="No ID", paste(d):=(asd_dw[draw==d, val] - crossprod(dw[hhseqid %in% c(286:290), get(d)], prob_draws[sequelae!="No ID", get(d)])) / 
             (prob_draws[sequelae=="No ID", get(d)] + prob_draws[sequelae=="Borderline ID", get(d)] * (1 - dw[healthstate=="id_bord", get(d)]) + 
                prob_draws[sequelae=="Mild ID", get(d)] * (1 - dw[healthstate=="id_mild", get(d)]) + prob_draws[sequelae=="Moderate ID", get(d)] * (1 - dw[healthstate=="id_mod", get(d)]) + 
                prob_draws[sequelae=="Severe ID", get(d)] * (1 - dw[healthstate=="id_sev", get(d)]) + prob_draws[sequelae=="Profound ID", get(d)] * (1 - dw[healthstate=="id_prof", get(d)]))]
  final_dw[sequelae=="Borderline ID", paste(d):= 1 - ((1 - final_dw[sequelae=="No ID", get(d)]) * (1 - dw[healthstate=="id_bord", get(d)]))]
  final_dw[sequelae=="Mild ID", paste(d):= 1 - ((1 - final_dw[sequelae=="No ID", get(d)]) * (1 - dw[healthstate=="id_mild", get(d)]))]
  final_dw[sequelae=="Moderate ID", paste(d):= 1 - ((1 - final_dw[sequelae=="No ID", get(d)]) * (1 - dw[healthstate=="id_mod", get(d)]))]
  final_dw[sequelae=="Severe ID", paste(d):= 1 - ((1 - final_dw[sequelae=="No ID", get(d)]) * (1 - dw[healthstate=="id_sev", get(d)]))]
  final_dw[sequelae=="Profound ID", paste(d):= 1 - ((1 - final_dw[sequelae=="No ID", get(d)]) * (1 - dw[healthstate=="id_prof", get(d)]))]
  print(paste0("Calculated dw for ASD and each ID, ", d, " out of draw_999"))
}

dw_backup <- copy(final_dw) 

final_dw[,`:=`(mean = apply(.SD, 1, mean, na.rm=T), lower = apply(.SD, 1, quantile, probs=0.025, na.rm=T), upper = apply(.SD, 1, quantile, probs=0.975, na.rm=T)), by=.(sequelae),.SDcols=draw_cols]

final_dw <- final_dw[,.(sequelae, mean, lower, upper)]
final_severity <- prob_draws[, `:=` (mean = apply(.SD, 1, mean, na.rm=T), lower = apply(.SD, 1, quantile, probs=0.025, na.rm=T), upper = apply(.SD, 1, quantile, probs=0.975, na.rm=T)), by=.(sequelae),.SDcols=draw_cols]
final_severity <- final_severity[, .(sequelae, mean, lower, upper)]

write.csv(dw_backup, paste0("FILEPATH"), row.names=F)
write.csv(prob_draws, paste0("FILEPATH"), row.names=F)

