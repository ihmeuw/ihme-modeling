library(data.table)
library(gtools)
library(magrittr)
library(parallel)
library(zoo)
library(reticulate)

gbd_env_python <- "FILEPATH"
Sys.setenv("RETICULATE_PYTHON"=gbd_env_python)
reticulate::use_python(gbd_env_python, required=TRUE)
df_utils <- reticulate::import("ihme_dimensions.dfutils")

initial_args <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_dir <- dirname(sub(file_arg, "", initial_args[grep(file_arg, initial_args)]))
setwd(script_dir)

source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/db.R")
source("./utils/burden_functions.R")
source("./utils/common.R")
source("./utils/risk_prevalence.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
risk_id <- as.numeric(args[1])
loc_id <- as.numeric(args[2])
year_ids <- eval(parse(text = paste0("c(",args[3],")")))
n_draws <- as.numeric(args[4])
gbd_round_id <- as.numeric(args[5])
decomp_step <- args[6]
out_dir <- args[7]
paf_version_id <- as.numeric(args[8])
paf_dir <- paste0("FILEPATH/", paf_version_id)
codcorrect_version <- as.numeric(args[9])
como_version <- as.numeric(args[10])
by_cause <- as.logical(args[11])

message("starting SEV calc for rei_id ", risk_id, ", location_id ", loc_id)

ckd_parent_cause_id <- 589
lbwsg_outcomes_cause_id <- 1061
neonatal_cause_id <- 380

reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = gbd_round_id,
                         decomp_step = decomp_step)
rei <- reis[rei_id == risk_id, ]$rei

#--PULL RRMAX ------------------------------------------------------------------
rr_max_file <- paste0(out_dir, "/rrmax/", risk_id, ".csv")
message("reading RRmax draws from ", rr_max_file)
rr_max <- fread(rr_max_file)
rr_max <- melt(rr_max, id.vars = c("cause_id", "age_group_id", "sex_id"),
               measure.vars = paste0("draw_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
rr_max[, draw := as.numeric(gsub("draw_", "", draw))]

#--PULL PAFs AND MERGE ---------------------------------------------------------
message("reading PAF draws from PAF compile v", paf_version_id)
if (rei %in% c("metab_bmd", "occ_hearing")) {
    # RRs for these risks aren't at the cause level, so we save them to disk in PAF calc
    # before we convert the paf so we can merge them here
    files <- paste0("FILEPATH/", rei, "/sev/", loc_id, "_", c(1, 2), ".csv")
    paf <- rbindlist(lapply(files, fread), use.names = TRUE)
    paf <- paf[year_id %in% year_ids, ]

    # resample to n_draws if necessary
    paf_n_draws <- length(unique(paf$draw))
    if (n_draws < paf_n_draws) {
        paf <- dcast(paf, location_id + year_id + age_group_id + sex_id + cause_id ~ draw,
                     value.var = "paf")
        setnames(paf, paste0(0:(paf_n_draws - 1)), paste0("draw_", 0:(paf_n_draws - 1)))
        paf <- downsample(dt=paf, n_draws=n_draws, df_utils=df_utils)
        paf <- melt(paf, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                     "cause_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "paf")
        paf[, draw := as.numeric(gsub("draw_", "", draw))]
    }
} else {
    # use YLL PAFs unless cause is YLD only
    yld_only <- fread(paste0(paf_dir, "/existing_reis.csv.gz")) %>%
        .[rei_id == risk_id, .(cause_id, measure_id)] %>% unique %>%
        .[order(cause_id, -measure_id)] %>% unique(., by = "cause_id") %>%
        .[measure_id == 3, cause_id]
    files <- paste0(paf_dir, "/", loc_id, "_", year_ids, ".csv.gz")
    compiler <- function(f) {
        df <- fread(f)[rei_id == risk_id & year_id %in% year_ids, ]
        df <- df[(measure_id == 3 & cause_id %in% yld_only) | measure_id == 4, ]
        return(df)
    }
    paf <- rbindlist(mclapply(files, compiler, mc.cores = 2), use.names = TRUE)
    paf <- downsample(dt=paf, n_draws=n_draws, df_utils=df_utils)
    paf <- paf[, c("age_group_id", "sex_id", "location_id", "year_id",
                   "cause_id", paste0("draw_", 0:(n_draws - 1))), with=F]
    paf <- melt(paf, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "cause_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "paf")
    paf[, draw := as.numeric(gsub("draw_", "", draw))]
    if (neonatal_cause_id %in% rr_max$cause_id) {
        # If Neonatal Disorders is present in RRmax, aggregate cause PAFs
        # using current CoDCorrect YLLs
        neo_paf <- aggregate_paf_to_parent_cause(
            dt = paf, parent_cause_id = neonatal_cause_id, measure = "yll",
            location_id = loc_id, year_id = year_ids, gbd_round_id = gbd_round_id,
            n_draws = n_draws, codcorrect_version = codcorrect_version
        )
        paf <- rbind(paf, neo_paf)
    }
    if (lbwsg_outcomes_cause_id %in% rr_max$cause_id) {
        # If LBW/SG outcomes cause is present in RRmax, aggregate cause PAFs
        # using CoDCorrect v257 YLLs for year ID 2010.
        lbwsg_outcomes_paf <- aggregate_paf_to_parent_cause(
            dt = paf, parent_cause_id = lbwsg_outcomes_cause_id, measure = "yll",
            location_id = loc_id, year_id = 2010, gbd_round_id = gbd_round_id,
            n_draws = n_draws, codcorrect_version = 257
        )
        paf <- rbind(paf, lbwsg_outcomes_paf)
    }
    if (ckd_parent_cause_id %in% rr_max$cause_id) {
        # For CKD PAFs for FPG, SBP, and any distal risk where CKD is mediated through FPG or SBP,
        # if CKD is present in RRmax, aggregate cause PAFs using CoDCorrect v257 YLLs
        # and COMO v932 YLDs for year ID 2010.
        ckd_paf <- aggregate_paf_to_parent_cause(
            dt = paf, parent_cause_id = ckd_parent_cause_id, measure = "daly",
            location_id = loc_id, year_id = 2010, gbd_round_id = gbd_round_id,
            n_draws = n_draws, codcorrect_version = 257, como_version = 932
        )
        paf <- rbind(paf, ckd_paf)
    }
}

dt <- merge(paf, rr_max, by=c("age_group_id", "sex_id", "cause_id", "draw"))

#--CALC SEV --------------------------------------------------------------------
message("calculating SEV")
dt[paf < 0, paf := 0]
dt[, sev := (paf / (1 - paf)) / (rr - 1)]
dt[rr <= 1, sev := 0]
if (nrow(dt[is.infinite(sev) | round(paf, 12) == 1, ]) > 0) stop("SEVs are infinite, PAFs of 100%?")
dt[sev >= 1, sev := 1]
if (nrow(dt[is.na(sev), ]) > 0) warning("SEVs are NA")
dt[, mean_sev := mean(sev, na.rm=T),
   by=c("location_id", "year_id", "age_group_id", "sex_id", "cause_id")]
dt[is.na(sev), sev := mean_sev][, mean_sev := NULL]
if (nrow(dt[is.na(sev), ]) > 0) stop("SEVs are still NA")

# For certain continuous risks with PAFs of 1 for some risk - outcome pairs, set SEV value as:
#   * risk prevalence (FPG, SBP)
#   * moderate and severe anemia prevalence (iron deficiency)
if (rei %in% c("metab_fpg", "metab_sbp", "nutrition_iron")) {
    prevalence <- if (rei %in% c("metab_fpg", "metab_sbp")) {
        get_risk_prevalence(
            rei_id = risk_id, rei = rei, location_id = loc_id, year_id = year_ids,
            gbd_round_id = gbd_round_id, decomp_step = decomp_step, n_draws = n_draws
        )
    } else if (rei == "nutrition_iron") {
        get_iron_deficiency_prevalance(
            location_id = loc_id, year_id = year_ids, gbd_round_id = gbd_round_id,
            decomp_step = decomp_step, n_draws = n_draws, df_utils = df_utils
        )
    }
    
    setnames(prevalence, "prevalence", "sev")
    pafs_of_one_causes <- fread("FILEPATH/pafs_of_one.csv")[rei_id == risk_id]$cause_id
    dt <- rbind(dt, merge(paf[cause_id %in% pafs_of_one_causes, ], prevalence,
                          by = c("location_id", "year_id", "age_group_id", "sex_id", "draw")),
                fill = TRUE)
}

#--AVERAGE ACROSS CAUSE AND SAVE ---------------------------------------------------------------
save_draws <- function(dt, risk_id, n_draws, out_dir) {
    dt[, `:=` (rei_id=risk_id, measure_id=29, metric_id=3)]
    id_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id")
    if ("cause_id" %in% names(dt)) {
        id_cols <- c("rei_id", "cause_id", id_cols)
    } else {
        id_cols <- c("rei_id", id_cols)
    }
    dt <- dcast(dt, as.formula(paste(paste(id_cols, collapse = "+"), "~ draw")), value.var = "sev")
    setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorderv(dt, cols = id_cols)
    dir.create(paste0(out_dir,"/draws/", risk_id), recursive = TRUE, showWarnings = FALSE)
    file <- paste0("/draws/", risk_id, "/", loc_id, ".csv")
    message("saving to ", out_dir, file)
    write.csv(dt, paste0(out_dir, file), row.names = F)
}

# save risk_cause-specific sev if requested
if (by_cause) save_draws(dt, risk_id, n_draws, paste0(out_dir, "/risk_cause"))

# average across causes to get risk-specific sev
dt <- dt[, .(sev = mean(sev)), by=c("location_id", "year_id", "age_group_id", "sex_id", "draw")]
save_draws(dt, risk_id, n_draws, out_dir)
