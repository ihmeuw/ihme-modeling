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

source("FILEPATH")

demo <- get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)
demo_template <- expand.grid(location_id = location_id,
                             year_id = year_id,
                             sex_id = demo$sex_id,
                             age_group_id = demo$age_group_id) %>% data.table
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

# proportions of remaining anemia to assign to residual causes
residuals <- fread(paste0("FILEPATH"))

# subtypes and modelable entity maps for things coming in and going out
subin <- read_excel(paste0("FILEPATH"), sheet = "in_meids") %>% data.table
subout <- read_excel(paste0("FILEPATH"), sheet = "out_meids") %>% data.table %>%
  select(-full_name, -test_run_mvid_mild, -test_run_mvid_moderate, -test_run_mvid_severe)

subout[, iron_responsive := NULL]
subout <- melt(subout, id.vars=c("subtype"), value.name = "modelable_entity_id", variable.name = "severity")
subout[, severity := gsub("modelable_entity_id_","", severity)]

# shifts by subtype
hb_shifts <- fread(paste0("FILEPATH"))

# drop the residuals
hb_shifts <- hb_shifts[!subtype %in% unique(residuals$subtype), ]

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

subtypes[subtype=="hemog_g6pd_hemi", prevalence := prevalence * .005]

# pull mean and SD hemoglobin levels
print("reading mean and sd hemoglobin")
thresholds <- fread("FILEPATH")
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

mean_hemo <- merge(
  mean_hemo,
  thresholds[pregnant==0, ][, .(age_group_id, sex_id,hgb_lower_mild, hgb_upper_mild,
                                hgb_lower_moderate, hgb_upper_moderate, hgb_lower_severe,
                                hgb_upper_severe)],
  by=c("age_group_id", "sex_id"))

mean_hemo <- rbindlist(list(
  copy(mean_hemo)[age_group_id == 4][, age_group_id := 2],
  copy(mean_hemo)[age_group_id == 4][, age_group_id := 3],
  mean_hemo[age_group_id >= 4, ]),
  use.names = T)

# pull prev of mild mod severe anemia
print("reading anemia envelope -- mild, moderate, and severe")
mild <- get_draws("modelable_entity_id", 10489, source="epi",
                  location_id=location_id, year_id=year_id,
                  sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                  gbd_round_id=gbd_round_id, decomp_step=decomp_step)
mild <- make_long(df = mild, n_draws = n_draws, new_col = "prev_mild",
                  id_cols = c("location_id", "year_id", "age_group_id", "sex_id"))
mod <- get_draws("modelable_entity_id", 10490, source="epi",
                 location_id=location_id, year_id=year_id,
                 sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                 gbd_round_id=gbd_round_id, decomp_step=decomp_step)
mod <- make_long(df = mod, n_draws = n_draws, new_col = "prev_mod",
                 id_cols = c("location_id", "year_id", "age_group_id", "sex_id"))
sev <- get_draws("modelable_entity_id", 10491, source="epi",
                 location_id=location_id, year_id=year_id,
                 sex_id=demo$sex_id, age_group_id=demo$age_group_id,
                 gbd_round_id=gbd_round_id, decomp_step=decomp_step)
sev <- make_long(df = sev, n_draws = n_draws, new_col = "prev_sev",
                 id_cols = c("location_id", "year_id", "age_group_id", "sex_id"))

# then start the math ----------------------------------------------------------
# merge all anemia related data together
col_sums <- merge(mean_hemo, mild, by=id_cols)
col_sums <- merge(col_sums, mod, by=id_cols)
col_sums <- merge(col_sums, sev, by=id_cols)

# merge cause specific prevalence on cause specific shifts
row_sums <- merge(subtypes, hb_shifts, by=c("subtype", "sex_id"))

# then merge again on hemo anemia data
df <- merge(col_sums, row_sums, by=id_cols)
df[subtype == "nutrition_vitamina" & age_group_id > 7, mean_hb_shift := 0]

df[, prev_shift := prevalence * mean_hb_shift]

print("calculating envelope with counterfactual distribution")
XMAX = 220

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
df[, sub_mild := ens_mv2prev(hgb_upper_mild, hemo+prev_shift, hemo_vr)
   - ens_mv2prev(hgb_lower_mild, hemo+prev_shift, hemo_vr)
   , by = 1:nrow(df)]
df[, sub_mod := ens_mv2prev(hgb_upper_moderate, hemo+prev_shift, hemo_vr) -
     ens_mv2prev(hgb_lower_moderate, hemo+prev_shift, hemo_vr)
   , by = 1:nrow(df)]
df[, sub_sev := ens_mv2prev(hgb_upper_severe, hemo+prev_shift, hemo_vr) -
     ens_mv2prev(hgb_lower_severe, hemo+prev_shift, hemo_vr)
   , by = 1:nrow(df)]

# make sure shifted prevalence isn't greater than true...
df[prev_shift == 0, `:=` (sub_mild=prev_mild, sub_mod=prev_mod, sub_sev=prev_sev)]
df[sub_mild > prev_mild, sub_mild := prev_mild - (sub_mild-prev_mild)]
df[sub_mod > prev_mod, sub_mod := prev_mod - (sub_mod-prev_mod)]
df[sub_sev > prev_sev, sub_sev := prev_sev - (sub_sev-prev_sev)]
# subtract to find inverse
df[, sub_mild := prev_mild - sub_mild]
df[, sub_mod := prev_mod - sub_mod]
df[, sub_sev := prev_sev - sub_sev]

# squeeze and residuals --------------------------------------------------------
print("squeezing to severity envelopes")
resid_pct <- .9
# if cause prevelence is 0, set all sev to 0
df[prevalence == 0, sub_mod := 0]
df[prevalence == 0, sub_mild := 0]
df[prevalence == 0, sub_sev := 0]

round_cols <- names(df)[grepl("^sub_|^prev_|^prevalence$",names(df))]
df[ , (round_cols) := lapply(.SD, function(x) {round(x, digits = 40)}), .SDcols = round_cols]
# rescale such that subtype severities sum to overall severity
#df[, prev_shift := prevalence * mean_hb_shift]

df[, mild_sum := sum(sub_mild), by=id_cols]
df[mild_sum > prev_mild * resid_pct, sub_mild := ((sub_mild * prev_shift) / sum(sub_mild * prev_shift)) * (resid_pct * prev_mild), by = id_cols]
df[, remaining_mild := prev_mild - sum(sub_mild), by=id_cols]

df[, mod_sum := sum(sub_mod), by=id_cols]
df[mod_sum > prev_mod * resid_pct, sub_mod := ((sub_mod * prev_shift) / sum(sub_mod * prev_shift)) * (resid_pct * prev_mod), by = id_cols]
df[, remaining_mod := prev_mod - sum(sub_mod), by=id_cols]

df[, sev_sum := sum(sub_sev), by=id_cols]
df[sev_sum > prev_sev * resid_pct, sub_sev := ((sub_sev * prev_shift) / sum(sub_sev * prev_shift)) * (resid_pct * prev_sev), by = id_cols]
df[, remaining_sev := prev_sev - sum(sub_sev), by=id_cols]

df[, prev_shift := NULL]

print("assinging remaining to residuals")
residuals <- merge(residuals,
                   unique(df[, names(df)[!grepl("^sub|_sum$|^mean_hb_shift$|^prevalence$",names(df))], with=F]),
                   by=c("age_group_id", "sex_id"), allow.cartesian = T)
# Zero out the ntd_other proportion if schisto and hookworm are zero
residuals[, sub_mild := resid_prop * remaining_mild]
residuals[, sub_mod := resid_prop * remaining_mod]
residuals[, sub_sev := resid_prop * remaining_sev]

df <- rbind(df, residuals, fill = T)

# then check to make sure severity sum isn't greater than cause prev
print("squeezing to cause prevalence")
df[, sub_anemic := (sub_mild + sub_mod + sub_sev)]
# if it is, scale to sum
df[sub_anemic > prevalence, sub_mild := sub_mild / sub_anemic * prevalence]
df[sub_anemic > prevalence, sub_mod := sub_mod / sub_anemic * prevalence]
df[sub_anemic > prevalence, sub_sev := sub_sev / sub_anemic * prevalence]
df[, sub_anemic := (sub_mild + sub_mod + sub_sev)]

print("again assinging remaining to residuals")
df[, target_sum := sum(sub_sev)/prev_sev,by=id_cols]
# then go back again and squeeze/expand resid so that severities sum
df[is.na(resid_prop), target_resid_sum := prev_mild-sum(sub_mild), by=id_cols]
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), sub_mild := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

df[is.na(resid_prop), target_resid_sum := prev_mod-sum(sub_mod), by=id_cols]
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), sub_mod := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

df[is.na(resid_prop), target_resid_sum := prev_sev-sum(sub_sev), by=id_cols]
df[, target_resid_sum := mean(target_resid_sum, na.rm = T), by=id_cols]
df[!is.na(resid_prop), sub_sev := resid_prop * target_resid_sum]
df[, target_resid_sum := NULL]

# save it out ------------------------------------------------------------------
print("reformatting data, reshaping wide on draw")
df <- df[, .(location_id, year_id, age_group_id, sex_id, subtype, draw, sub_mild, sub_mod, sub_sev)]
df <- melt(df,
           id.vars=c("location_id", "year_id", "age_group_id", "sex_id", "draw", "subtype"),
           value.name = "mean", variable.name = "severity")
df[, severity := gsub("sub_","", severity)]
df[, severity := ifelse(severity=="mod","moderate", ifelse(severity=="sev", "severe", "mild"))]
df <- merge(df, subout, by=c("subtype", "severity"), all.x=T)
df[, `:=` (measure_id = 5, metric_id = 3)]
df <- df[, .(modelable_entity_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id, draw, mean)]
df <- dcast(df, modelable_entity_id + location_id + year_id + age_group_id + sex_id ~ draw, value.var = "mean")
setnames(df, paste(0:(n_draws-1)), draw_cols)

for(me in subout$modelable_entity_id) {
  dir.create(showWarnings = FALSE, recursive = TRUE, path = paste0("FILEPATH"), mode = "775")
  print(sprintf("saving data for modelable_entity_id %s", me))
  write.csv(df[modelable_entity_id == me, ],
            paste0("FILEPATH"),
            row.names = FALSE)
  Sys.chmod(paste0("FILEPATH"), mode = "0775", use_umask = FALSE)
}

print("saving summary file for agg cause vetting")
cause_link <- fread(paste0("FILEPATH"))
df[, val := apply(.SD, 1, mean), .SDcols=draw_cols]
df$modelable_entity_id <- as.numeric(df$modelable_entity_id)
df <- merge(df[, -c(draw_cols), with=F],
            cause_link[,. (modelable_entity_id, cause_id, rei_id, dw)],
            by="modelable_entity_id")
df[, val := sum(val), by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'rei_id', 'cause_id')]
df <- unique(df[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, prevalence=val, dw)])
df[, yld := prevalence * dw]
df <- rbind(df, copy(df)[, rei_id := 192])
df[, `:=` (prevalence=sum(prevalence), yld=sum(yld)), by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'rei_id', 'cause_id')]
df <- unique(df[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, metric_id=3, prevalence, yld)])
dir.create(showWarnings = FALSE, path = paste0("FILEPATH"), mode = "775")
write.csv(df, paste0("FILEPATH"), row.names=F)

print("All done!")
