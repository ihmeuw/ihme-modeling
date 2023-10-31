rm(list=ls())
library(readxl)
library(data.table)
library(magrittr)
library(actuar)
library(tidyverse)

# set up args ------------------------------------------------------------------
args <- commandArgs(trailingOnly=TRUE)
location_id <- as.numeric(args[1])
year_id <- as.numeric(args[2])
gbd_round_id <- as.numeric(args[3])
decomp_step <- args[4]
code_dir <- args[5]
out_dir <- args[6]
n_draws <- 1000

source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_covariate_estimates.R")
source(paste0(code_dir, "/ensemble.R"))

# Define ensemble prevalence function
XMAX = 350
ens_mv2prev <- function(q, mn, vr){
  x = q
  ##parameters
  params_gamma = gamma_mv2p(mn, vr)
  params_mgumbel = mgumbel_mv2p(mn, vr)
  
  ##weighting
  prev = sum(
    0.4 * pgamma(x, params_gamma$shape,params_gamma$rate),
    0.6 * pmgumbel(x,params_mgumbel$alpha,params_mgumbel$scale, lower.tail = T)
  )
  prev
  
}

demo <- get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)
demo_template <- expand.grid(location_id = location_id,
                             year_id = year_id,
                             sex_id = demo$sex_id,
                             age_group_id = demo$age_group_id) %>% data.table

demo_template$year_id <- as.integer(demo_template$year_id)


draw_cols <- paste0("draw_", 0:(n_draws-1))
id_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw")

make_long <- function(df, n_draws, new_col, id_cols) {
  df <- melt(copy(df), id.vars = id_cols,
             measure.vars = paste0("draw_", 0:(n_draws - 1)),
             variable.name = "draw", value.name = new_col)
  df[, draw := as.numeric(gsub("draw_", "", draw))]
  return(df)
}

# pull constants ---------------------------------------------------------------
if (gbd_round_id == 7){
  # proportions of remaining anemia to assign to residual causes
  residuals <- fread(paste0(code_dir, "/FILEPATH/residual_proportions_2020.csv"))
  
  # subtypes and modelable entity maps for things coming in and going out
  subin <- read_excel(paste0(code_dir, "/in_out_meid_map_2020.xlsx"), sheet = "in_meids") %>% data.table
  subout <- read_excel(paste0(code_dir, "/in_out_meid_map_2020.xlsx"), sheet = "out_meids") %>% 
    select(subtype, meid_mild, meid_moderate, meid_severe, iron_responsive) %>%
    data.table
  
  subout[, iron_responsive := NULL]
  subout <- melt(subout, id.vars=c("subtype"), value.name = "meid", variable.name = "severity")
  
  subout_prop <- read_excel(paste0(code_dir, "/in_out_meid_map_2020.xlsx"), sheet = "out_meids") %>% 
    select(subtype, prop_mild, prop_moderate, prop_severe, prop_without) %>%
    filter(!is.na(prop_mild))
  data.table
  
  subout_prop <- melt(subout_prop, id.vars=c("subtype"), value.name = "meid", variable.name = "severity")
  
  
  # shifts by subtype
  hb_shifts <- fread(paste0(code_dir, "/FILEPATH/hb_shifts_2020.csv"))
  
  # drop the residuals
  hb_shifts <- hb_shifts[!subtype %in% unique(residuals$subtype), ]
} else{
  residuals <- fread(paste0(code_dir, "/FILEPATH/residual_proportions.csv"))
  
  # subtypes and modelable entity maps for things coming in and going out
  subin <- read_excel(paste0(code_dir, "/in_out_meid_map.xlsx"), sheet = "in_meids") %>% data.table
  subout <- read_excel(paste0(code_dir, "/in_out_meid_map.xlsx"), sheet = "out_meids") %>% 
    select(subtype, meid_mild, meid_moderate, meid_severe, iron_responsive) %>%
    data.table
  
  subout[, iron_responsive := NULL]
  subout <- melt(subout, id.vars=c("subtype"), value.name = "meid", variable.name = "severity")
  
  # shifts by subtype
  hb_shifts <- fread(paste0(code_dir, "/FILEPATH/hb_shifts.csv"))
  
  # drop the residuals
  hb_shifts <- hb_shifts[!subtype %in% unique(residuals$subtype), ]
}

# read in data -----------------------------------------------------------------
# from map of incoming mes and measures
meid_measures <- subin[!is.na(modelable_entity_id), ][, .(modelable_entity_id, measure_id)]
# pull data
subtypes <- rbindlist(lapply(meid_measures$modelable_entity_id, function(me) {
  print(sprintf("reading subtype data for modelable_entity_id %s", me))
  subtype <- get_draws("modelable_entity_id", me, source="epi",
                       location_id=location_id, year_id=year_id,
                       sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                       measure_id=meid_measures[modelable_entity_id==me, ]$measure_id,
                       gbd_round_id=gbd_round_id, decomp_step=decomp_step)
  # keep demographically square
  subtype <- merge(demo_template, subtype, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x=T)
  subtype[, type := as.character(subin[modelable_entity_id==me, ]$subtype)]
  subtype <- subtype[, c("type", "location_id", "year_id", "age_group_id", "sex_id", draw_cols), with = F]
  return(subtype)
}), use.names=TRUE)
setnames(subtypes, "type", "subtype")
subtypes <- make_long(df = subtypes, n_draws = n_draws, new_col = "prevalence",
                      id_cols = c("subtype", "location_id", "year_id", "age_group_id", "sex_id"))
subtypes[is.na(prevalence), prevalence := 0]

# pull mean and SD hemoglobin levels
print("reading mean and sd hemoglobin")
thresholds <- fread("FILEPATH/anemia_thresholds.csv")
mean_hemo <- get_draws("modelable_entity_id", 10487, source="epi",
                       location_id=location_id, year_id=year_id,
                       sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                       gbd_round_id=gbd_round_id, decomp_step=decomp_step)
mean_hemo <- make_long(df = mean_hemo, n_draws = n_draws, new_col = "hemo",
                       id_cols = c("location_id", "year_id", "age_group_id", "sex_id"))

sd_hemo <- get_draws("modelable_entity_id", 10488, source="epi",
                       location_id=location_id, year_id=year_id,
                       sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                       gbd_round_id=gbd_round_id, decomp_step=decomp_step)
sd_hemo <- make_long(df = sd_hemo, n_draws = n_draws, new_col = "hemo_sd",
                     id_cols = c("location_id", "year_id", "age_group_id", "sex_id"))
sd_hemo[, hemo_vr := hemo_sd^2]

mean_hemo <- merge(mean_hemo, sd_hemo, by = id_cols)
mean_hemo[,pregnant:=0]

# Create rows for pregnant women
mean_hemo.p <- copy(mean_hemo[sex_id==2 & age_group_id %in% c(7:15)])

# Adjust mean Hgb for pregnant women
# Note: Need to figure out how to not hard code via pulling from MR-BRT output
mean_hemo.p[,`:=`(hemo=hemo*0.919325,
                  pregnant=1)]

# Bind pregnant df to full df
mean_hemo <- rbind(mean_hemo,mean_hemo.p)

# Merge on anemia thresholds by age/sex/pregnancy status
mean_hemo <- merge(
  mean_hemo,
  thresholds[, ][, .(age_group_id, sex_id, pregnant, hgb_lower_mild, hgb_upper_mild,
                                hgb_lower_moderate, hgb_upper_moderate, hgb_lower_severe,
                                hgb_upper_severe)],
  by=c("age_group_id", "sex_id", "pregnant"))

# Stop if errors in threshold merging
stopifnot(nrow(mean_hemo[is.na(hgb_upper_severe)])==0)

# ENN and LNN were previously a copy of PNN.
if (gbd_round_id == 5){
  mean_hemo <- rbindlist(list(
    copy(mean_hemo)[age_group_id == 4][, age_group_id := 2],
    copy(mean_hemo)[age_group_id == 4][, age_group_id := 3],
    mean_hemo[age_group_id >= 4, ]),
    use.names = T)
}

# Calculate anemia envelope prevalence 
print("Calculating anemia envelope - mild, moderate, severe, and total")
col_sums <- copy(mean_hemo)

col_sums[, prev_mild := ens_mv2prev(hgb_upper_mild, hemo, hemo_vr) - ens_mv2prev(hgb_lower_mild, hemo, hemo_vr)
         , by = 1:nrow(col_sums)]
col_sums[, prev_mod := ens_mv2prev(hgb_upper_moderate, hemo, hemo_vr) - ens_mv2prev(hgb_lower_moderate, hemo, hemo_vr)
         , by = 1:nrow(col_sums)]
col_sums[, prev_sev := ens_mv2prev(hgb_upper_severe, hemo, hemo_vr) - ens_mv2prev(hgb_lower_severe, hemo, hemo_vr)
         , by = 1:nrow(col_sums)]
col_sums[, prev_modsev := ens_mv2prev(hgb_upper_moderate, hemo, hemo_vr) - ens_mv2prev(hgb_lower_severe, hemo, hemo_vr)
         , by = 1:nrow(col_sums)]
col_sums[, prev_total := ens_mv2prev(hgb_upper_mild, hemo, hemo_vr)
         , by = 1:nrow(col_sums)]

# then start the math ----------------------------------------------------------

# merge cause specific prevalence on cause specific shifts
row_sums <- merge(subtypes, hb_shifts, by=c("subtype", "sex_id"))

# then merge again on hemo anemia data
df <- merge(col_sums, row_sums, by=id_cols, allow.cartesian=TRUE)
df[subtype == "nutrition_vitamina" & age_group_id > 7, mean_hb_shift := 0]

df[, prev_shift := prevalence * mean_hb_shift]

print("calculating envelope with counterfactual distribution")
df[, cf_sev := ens_mv2prev(hgb_upper_severe, hemo+prev_shift, hemo_vr) -
     ens_mv2prev(hgb_lower_severe, hemo+prev_shift, hemo_vr)
   , by = 1:nrow(df)]
df[, cf_modsev := ens_mv2prev(hgb_upper_moderate, hemo+prev_shift, hemo_vr) -
     ens_mv2prev(hgb_lower_severe, hemo+prev_shift, hemo_vr)
   , by = 1:nrow(df)]
df[, cf_total := ens_mv2prev(hgb_upper_mild, hemo+prev_shift, hemo_vr) 
   , by = 1:nrow(df)]

# make sure shifted prevalence isn't greater than true...
# write out df if shifted prevalence > envelope prevalence
if (nrow(df[round(cf_sev,4) > round(prev_sev,4) | round(cf_modsev,4) > round(cf_modsev,4) | 
                                round(cf_total,4) > round(prev_total,4)])>0) {
  df_err <- df[round(cf_sev,4) > round(prev_sev,4) | round(cf_modsev,4) > round(cf_modsev,4) | 
                 round(cf_total,4) > round(prev_total,4)]
  saveRDS(df_err,paste0("FILEPATH",location_id,"_",year_id,".rds"))
}

df[cf_sev > prev_sev, cf_sev := prev_sev]
df[cf_modsev > prev_modsev, cf_modsev := prev_modsev]
df[cf_total > prev_total, cf_total := prev_total]
# subtract to find inverse
df[, csp_sev := prev_sev - cf_sev]
df[, csp_modsev := prev_modsev - cf_modsev]
df[, csp_total := prev_total - cf_total]

## Re-aggregate females by pregnancy status (Note: need to remove if want to report by pregnancy status in the future)
# Pull age-specific fertility and stillbirths
asfr <- get_covariate_estimates(13,location_id=location_id,year_id=year_id,sex_id=2,age_group_id=7:15,
                                decomp_step=decomp_step,gbd_round_id=gbd_round_id)[,.(location_id,year_id,age_group_id,mean_value)]
setnames(asfr, "mean_value", "asfr")

# Stillbirths 
still <- get_covariate_estimates(2267,location_id=location_id,year_id=year_id,
                                 decomp_step=decomp_step,gbd_round_id=gbd_round_id)[,.(location_id,year_id,mean_value)]
setnames(still,"mean_value","sbr_mean")

# Merge - stillbirths are only location-year specific 
df_preg <- merge(asfr, still, by = c("location_id", "year_id"))
# Stillbirth_mean is still births per live birth
df_preg[, prev_pregnant := (asfr + (sbr_mean * asfr)) * 46/52  ]
df_preg[,`:=`(asfr=NULL,sbr_mean=NULL)]

# Merge on pregnancy prevalence
sevs <- c("prev_mild","prev_mod","prev_sev","prev_modsev","prev_total","csp_sev","csp_modsev","csp_total")
by_vars <- c("location_id","age_group_id","year_id","sex_id","subtype","draw")
df.p <- df[pregnant == 1]
df.p <- merge(df.p, df_preg, by =c("location_id","year_id","age_group_id") , all = TRUE)
df.p <- df.p[, c(by_vars, sevs, "prev_pregnant"), with = FALSE]
df.p <- unique(df.p)
lapply(sevs, function(s) setnames(df.p, s, paste(s, "preg", sep = "_")))

# Merge pregnant dataset onto non-pregnant
df <- df[pregnant == 0]
df <- merge(df, df.p, by = by_vars, all = TRUE)

# Take a weighted sum
lapply(sevs, function(sev) df[ !is.na(prev_pregnant) , c(sev) := get(sev) * (1 - prev_pregnant) + get(paste0(sev, "_preg")) * prev_pregnant ]) 
df[,c("pregnant","prev_pregnant",paste0(sevs,"_preg")):=NULL]

# Calculate mild and moderate cause-specific attributable prevalence
df[,`:=` (csp_mod = csp_modsev - csp_sev,
          csp_mild = csp_total - csp_modsev)]

# write out and adjust where attributable sev > modsev or attributable modsev > total
if (nrow(df[csp_mod < 0 | csp_mild < 0])>0) {
  df_err <- df[csp_mod < 0 | csp_mild < 0]
  saveRDS(df_err,paste0("FILEPATH/",location_id,"_",year_id,".rds"))
}
df[csp_mod < 0, csp_mod := 0]
df[csp_mild < 0, csp_mild := 0]

dir.create(showWarnings = FALSE, path = paste0(out_dir, "/diagnostics"), mode = "775")
#Writing out intermediate file for diagnostics
df_diag <- copy(df)

df_diag <- aggregate(df_diag[, c('csp_mild', "csp_mod", "csp_sev", "csp_total")], 
                     by = list(df_diag$age_group_id, df_diag$sex_id, df_diag$subtype),
                     FUN = mean) %>%
  rename(age_group_id = Group.1, sex_id = Group.2, subtype = Group.3)

df_diag$location_id <- location_id
df_diag$year_id <- year_id
df_diag$sub_total <- df_diag$csp_mild + df_diag$csp_mod + df_diag$csp_sev

write.csv(df_diag, paste0(out_dir, "/diagnostics/", location_id, "_", year_id, "_pre_squeeze.csv"), row.names=F)

print("Intermediate file written")
# squeeze and residuals --------------------------------------------------------
print("squeezing to severity envelopes")
non_resid_pct <- .9

# don't track super tiny numbers
round_cols <- names(df)[grepl("^sub_|^csp|^sum|^prev_|^prevalence$",names(df))]
df[ , (round_cols) := lapply(.SD, function(x) {round(x, digits = 40)}), .SDcols = round_cols]


## Rescale such that subtype severities sum to overall severity
# calculate mild/moderate/severe/total sums
df[,`:=` (mild_sum = sum(csp_mild),
          mod_sum = sum(csp_mod),
          sev_sum = sum(csp_sev),
          total_sum = sum(csp_total)), by=id_cols]

# squeeze severe and recalculate sum
df[, cspsqz_sev := ifelse(sev_sum <= prev_sev, csp_sev, csp_sev * (prev_sev/sev_sum))] # scale sum of severe to always be <= envelope
df[, cspsqzsev_sum := sum(cspsqz_sev), by=id_cols] # re-sum severe cases
df[cspsqzsev_sum > prev_sev, cspsqzsev_sum := prev_sev] # just a very small number issue in a few rows

# add squeezed severe cases into moderate bin and re-sum
df[, csp_modprime := ifelse(sev_sum <= prev_sev, csp_mod, csp_mod + (csp_sev - cspsqz_sev))] # where we squeezed sev, add cases to moderate bin
df[, modprime_sum := sum(csp_modprime), by=id_cols] # re-sum moderate cases

# squeeze moderate and recalculate sum
df[, cspsqz_mod := ifelse(modprime_sum <= prev_mod, csp_modprime, csp_modprime * (prev_mod/modprime_sum))] # scale sum of moderate to always be <= envelope
df[, cspsqzmod_sum := sum(cspsqz_mod), by=id_cols] # re-sum moderate cases
df[cspsqzmod_sum > prev_mod, cspsqzmod_sum := prev_mod] # adjust for very small number differences

# add squeezed moderate cases in mild bin and re-sum
df[, csp_mildprime := ifelse(modprime_sum <= prev_mod, csp_mild, csp_mild + (csp_modprime - cspsqz_mod))] # where we squeezed mod, add cases to mild bin
df[, mildprime_sum := sum(csp_mildprime), by=id_cols] # re-sum mild cases

# Calculate total and squeeze to residual
df[, csp_totalprime := cspsqz_sev + cspsqz_mod + csp_mildprime] # calculate interim total sum
df[, totalprime_sum := sum(csp_totalprime), by=id_cols] # sum interim total cases
df[, cspsqz_total := ifelse(totalprime_sum <= non_resid_pct * prev_total, csp_totalprime, csp_totalprime * (non_resid_pct * prev_total/totalprime_sum))] # scale sum of total cases to always be <= envelope

# Calculate cause-specific mild and re-sum
df[, cspsqz_mild := cspsqz_total - (cspsqz_mod + cspsqz_sev)] # subtract mod+sev from total to get mild
df[cspsqz_mild < 0, cspsqz_mild := 0] # small numbers issue again
df[, cspsqzmild_sum := sum(cspsqz_mild), by=id_cols] # re-sum mild cases

# Calculate residuals
df[, remaining_mild := prev_mild - cspsqzmild_sum, by=id_cols]
df[, remaining_mod := prev_mod - cspsqzmod_sum, by=id_cols]
df[, remaining_sev := prev_sev - cspsqzsev_sum, by=id_cols]

# Reformat columns for residual assignment
df[, `:=` (csp_mild = cspsqz_mild,
           csp_mod = cspsqz_mod,
           csp_sev = cspsqz_sev)]
df[,c("cspsqz_sev","cspsqzsev_sum","csp_modprime","modprime_sum","cspsqz_mod","cspsqzmod_sum","csp_mildprime","mildprime_sum",
      "csp_totalprime","totalprime_sum","cspsqz_total","cspsqz_mild","cspsqzmild_sum","prev_shift") := NULL]

print("assinging remaining to residuals")
residuals <- merge(residuals,
                   unique(df[, names(df)[!grepl("^sub|^csp|^cf|_raw$|_sum$|^mean_hb_shift$|^prevalence$",names(df))], with=F]),
                   by=c("age_group_id", "sex_id"), allow.cartesian = T)
# TODO: cancel out resid and rescale for ntd other if no ntd prev for schisto and hookworm
# Zero out the ntd_other proportion if schisto and hookworm are zero
residuals[, csp_mild := resid_prop * remaining_mild]
residuals[, csp_mod := resid_prop * remaining_mod]
residuals[, csp_sev := resid_prop * remaining_sev]

df <- rbind(df, residuals, fill = T)

# Zero out the ntd_other proportion if schisto and hookworm are zero
df[subtype %in% c("ntd_other", "ntd_schisto", "ntd_nema_hook"), check_col := sum(prevalence, na.rm = TRUE), by=id_cols]
df[subtype == "ntd_other" & check_col == 0, `:=` (csp_mild = 0, csp_mod = 0, csp_sev = 0)]
df[, check_col := NULL]

# then check to make sure severity sum isn't greater than cause prev
#Write in error check for 346-352
print("squeezing to cause prevalence")
df[, sub_anemic := (csp_mild + csp_mod + csp_sev)]
# if it is, scale to sum
df[sub_anemic > prevalence, csp_mild := csp_mild / sub_anemic * prevalence]
df[sub_anemic > prevalence, csp_mod := csp_mod / sub_anemic * prevalence]
df[sub_anemic > prevalence, csp_sev := csp_sev / sub_anemic * prevalence]
df[, sub_anemic := (csp_mild + csp_mod + csp_sev)]


print("again assigning remaining to residuals")
# then go back again and squeeze/expand resid so that severities sum
df[is.na(resid_prop), target_resid_sum := prev_mild-sum(csp_mild), by=id_cols]
df[target_resid_sum < 0, target_resid_sum:=0] # sometimes targert_resid_sum negative due to small numbers (i.e. 10^-18)
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), csp_mild := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

df[is.na(resid_prop), target_resid_sum := prev_mod-sum(csp_mod), by=id_cols]
df[target_resid_sum < 0, target_resid_sum:=0] # sometimes targert_resid_sum negative due to small numbers (i.e. 10^-18)
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), csp_mod := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

df[is.na(resid_prop), target_resid_sum := prev_sev-sum(csp_sev), by=id_cols]
df[target_resid_sum < 0, target_resid_sum:=0] # sometimes targert_resid_sum negative due to small numbers (i.e. 10^-18)
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), csp_sev := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

####Calculating proportions
df[, csp_other := prevalence - csp_mild - csp_mod - csp_sev]
df[csp_other < 0, csp_other := 0] # small numbers problem where some draws negative 10^-17 or less where should be 0
df[!prevalence == 0, `:=` (prop_mild = csp_mild / prevalence,
                           prop_mod = csp_mod / prevalence,
                           prop_sev = csp_sev / prevalence,
                           prop_without = csp_other / prevalence)]

df[prevalence == 0, `:=` (prop_mild = 0,
                               prop_mod = 0,
                               prop_sev = 0,
                               prop_without = 0)]

# save it out ------------------------------------------------------------------
##Writing out Cause-Specific Prevalence files 
df_prev <- df[, .(location_id, year_id, age_group_id, sex_id, subtype, draw, csp_mild, csp_mod, csp_sev)]

df_prev <- melt(df_prev,
                id.vars=c("location_id", "year_id", "age_group_id", "sex_id", "draw", "subtype"),
                value.name = "mean", variable.name = "severity")
df_prev[, severity := gsub("csp_","", severity)]
df_prev[, severity := ifelse(severity=="mod","meid_moderate", ifelse(severity=="sev", "meid_severe", "meid_mild"))]
df_prev <- merge(df_prev, subout, by=c("subtype", "severity"), all.x=T)
df_prev[, `:=` (measure_id = 5, metric_id = 3)]
df_prev <- df_prev[, .(meid, location_id, year_id, age_group_id, sex_id, draw, mean)]


df_prev <- dcast(df_prev, meid + location_id + year_id + age_group_id + sex_id ~ draw, value.var = "mean")
setnames(df_prev, paste(0:(n_draws-1)), draw_cols)

colnames(df_prev)[which(names(df_prev) == "meid")] <- "modelable_entity_id"


for(me in subout$meid) {
  dir.create(showWarnings = FALSE, recursive = TRUE, path = paste0(out_dir, "/", me, "/", year_id), mode = "775")
  print(sprintf("saving data for modelable_entity_id %s", me))
  write.csv(df_prev[modelable_entity_id == me, ],
            paste0(out_dir, "/", me, "/", year_id, "/", location_id, ".csv"),
            row.names = FALSE)
  Sys.chmod(paste0(out_dir, "/", me, "/", year_id, "/", location_id, ".csv"), mode = "0775", use_umask = FALSE)
}


##Writing out Cause-Specific Proportion files 
df_prop <- df[!subtype %in% c("nutrition_iron", "ntd_other", "infectious", "hemog_other"), 
              .(location_id, year_id, age_group_id, sex_id, subtype, draw, prop_mild, prop_mod, 
                prop_sev, prop_without)]


df_prop <- melt(df_prop,
                id.vars=c("location_id", "year_id", "age_group_id", "sex_id", "draw", "subtype"),
                value.name = "mean", variable.name = "severity")
df_prop[, severity := gsub("prop_","", severity)]
df_prop[, severity := ifelse(severity=="mod","prop_moderate", ifelse(severity=="sev", "prop_severe", ifelse(severity == "without", "prop_without", "prop_mild")))]
df_prop <- merge(df_prop, subout_prop, by=c("subtype", "severity"), all.x=T)
df_prop[, `:=` (measure_id = 18)]
df_prop <- df_prop[, .(meid, location_id, year_id, age_group_id, sex_id, measure_id, draw, mean)]


df_prop <- dcast(df_prop, meid + location_id + year_id + age_group_id + sex_id ~ draw, value.var = "mean")
setnames(df_prop, paste(0:(n_draws-1)), draw_cols)

colnames(df_prop)[which(names(df_prop) == "meid")] <- "modelable_entity_id"


for(me in subout_prop$meid) {
  dir.create(showWarnings = FALSE, recursive = TRUE, path = paste0(out_dir, "/", me, "/", year_id), mode = "775")
  print(sprintf("saving data for modelable_entity_id %s", me))
  write.csv(df_prop[modelable_entity_id == me, ],
            paste0(out_dir, "/", me, "/", year_id, "/", location_id, ".csv"),
            row.names = FALSE)
  Sys.chmod(paste0(out_dir, "/", me, "/", year_id, "/", location_id, ".csv"), mode = "0775", use_umask = FALSE)
}


print("saving summary file for agg cause vetting")
df_prev[, val := apply(.SD, 1, mean), .SDcols=draw_cols]
df_prev$modelable_entity_id <- as.numeric(df_prev$modelable_entity_id)
df_prev[, val := sum(val), by=c('location_id', 'year_id', 'age_group_id', 'sex_id', "modelable_entity_id")]
df_prev <- unique(df_prev[, .(modelable_entity_id, location_id, year_id, age_group_id, sex_id, mean=val)])
write.csv(df_prev, paste0(out_dir, "/diagnostics/", location_id, "_", year_id, "_post_squeeze.csv"), row.names=F)

print("All done!")
