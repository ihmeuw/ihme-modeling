###########################################################################################################
# PAD PROCESSING
###########################################################################################################

###########################################################################################################
# SET UP
###########################################################################################################

rm(list=ls())

j <- "FILEPATH"
central <- paste0(j, "FILEPATH/r/")
suppressMessages(source(paste0(central, "get_draws.R")))

# load libraries
library(data.table)

# set directory paths
out_path <- "FILEPATH/decomp_1/"

###########################################################################################################
# OBTAIN AND FORMAT DRAWS
###########################################################################################################

## extract current command args:
args <- commandArgs(trailingOnly = TRUE)
parameters_filepath <- args[1]
decomp_step <- args[2]

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)

parameters <- fread(parameters_filepath)
location <- parameters[task_id, location_id]
print(location)

# set locals
meid_pad <- 2532
meid_claudication <- 1861
save_type <- c("symp", "asymp")

# import and compile prevalence of PAD from DisMod estimates
df <- get_draws(gbd_id_type = "modelable_entity_id",
                gbd_id = meid_pad,
                location_id = location,
                measure_id = c(5, 6),
                status = "best",
                source = "epi",
                decomp_step = decomp_step)

# subset and output incidence
df_incid <- df[df$measure_id == 6,]
df_incid <- df_incid[,c("age_group_id", "sex_id", "year_id", paste0("draw_", 0:999)), with = F]
write.csv(df_incid, paste0(out_path, "asymp/6_", location, ".csv"))

df_prev <- df[df$measure_id == 5,]

# rename "draw_i" to "prev_i"
for (i in 0:999) {
  colnames(df_prev)[colnames(df_prev)==paste0("draw_", i)] <- paste0("prev_", i)
}

# import and compile claudication proportion (percent of people with PAD who show symptoms of claudication)
# from DisMod estimates
meid_claudication = 1861
df_prop = get_draws(gbd_id_type = "modelable_entity_id",
                    gbd_id = meid_claudication,
                    location_id = location,
                    measure_id = 18,
                    status = "best",
                    source = "epi",
                    decomp_step = decomp_step)

for (i in 0:999) {
  # rename "draw_i" to "claud_i"
  colnames(df_prop)[colnames(df_prop)==paste0("draw_", i)] <- paste0("claud_", i)
}

###########################################################################################################
# MERGE AND TRANSFORM DATA
###########################################################################################################

# PAD prevalence is the effective sample size (uncertainty measure),
# and claudication prevalence is # of people with claudication

master <- merge(df_prev, df_prop, by=c("location_id", "year_id", "sex_id", "age_group_id"))

# convert to a data.table
setDT(master)

# calculate values for symp and asymp
master[, paste0("symp_", c(0:999)) := lapply(c(0:999), function(x) get(paste0("prev_", x)) * get(paste0("claud_", x)))]
master[, paste0("asymp_", c(0:999)) := lapply(c(0:999), function(x) get(paste0("prev_", x)) * (1 - get(paste0("claud_", x))))]

# saving check file for summary statistics
type <- c("prev", "claud", "symp", "asymp")
for (kind in type) {
  master[, paste0(kind, "_mean") := rowMeans(.SD), .SDcols = paste0(kind, "_", 0:999)]
  master[, paste0(kind, "_upper") := apply(.SD, 1, quantile, 0.975), .SDcols = paste0(kind, "_", 0:999)]
  master[, paste0(kind, "_lower") := apply(.SD, 1, quantile, 0.025), .SDcols = paste0(kind, "_", 0:999)]
  master[, paste0(kind, "_sd") := apply(.SD, 1, sd), .SDcols = paste0(kind, "_", 0:999)]
}

mean_cols <- colnames(master)[grep("mean", colnames(master))]
upper_cols <- colnames(master)[grep("upper", colnames(master))]
lower_cols <- colnames(master)[grep("lower", colnames(master))]
sd_cols <- colnames(master)[grep("sd", colnames(master))]

dt_check <- master[, .SD, .SDcols = c(mean_cols, upper_cols, lower_cols, sd_cols, "age_group_id", "sex_id", "location_id", "year_id")]
write.csv(dt_check, paste0(out_path, "diagnostics/check_", location, ".csv"), row.names=F)

###########################################################################################################
# SAVE AND OUTPUT FILE FOR UPLOAD WITH SAVE RESULTS
###########################################################################################################

save_type <- c("symp", "asymp")
for (kind in save_type) {
  # subset dt by age_group_id, draws, year_id and sex_id
  pad_sub <- master[,c("age_group_id", "sex_id", "year_id", paste0(kind , "_", 0:999)), with = F]

  # rename kind_i to draw_i
  for (i in 0:999) {
    setnames(pad_sub, old=paste0(kind, "_", i), new=paste0("draw_", i))
  }

  for(i in 0:999) {
    pad_sub[age_group_id < 13, paste0("draw_", i) := 0]
  }
  write.csv(pad_sub, paste0(out_path, kind, "/5_", location, ".csv"), row.names=F)
}
