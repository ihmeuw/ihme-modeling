library(data.table)
library(magrittr)
library(gtools)
library(parallel)
library(zoo)

source("./utils/data.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/interpolate.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_rei_metadata.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
params <- fread(args[1])[task_id, ]

risk_id <- unique(params$risk_id)
loc_id <- unique(params$location_id)
year_ids <- eval(parse(text = args[2]))
n_draws <- as.numeric(args[3])
gbd_round_id <- as.numeric(args[4])
decomp_step <- args[5]
out_dir <- args[6]
paf_version_id <- as.numeric(args[7])
paf_dir <- paste0("FILEPATH/pafs/", paf_version_id)

rei_meta <- get_rei_meta(risk_id)
rei <- rei_meta$rei
cont <- ifelse(rei_meta$calc_type == 2, TRUE, FALSE)
if (cont) {
    inv_exp <- rei_meta$inv_exp
    rr_scalar <- rei_meta$rr_scalar
    tmrel_lower <- rei_meta$tmrel_lower
    tmrel_upper <- rei_meta$tmrel_upper
}
# hearing and ergonomic and bullying and IPV exp we use YLD, all other YLL
yld_risks <- c(130, 132, 363, 167)
demo <- get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)

rei_hier <- get_rei_metadata(rei_set_id = 1, gbd_round_id = gbd_round_id)
reis <- copy(rei_hier)
# for lbw/sg and air we estimate the joint directly
reis <- reis[!rei_id %in% c(334, 335, 86, 87), ]
reis[rei_id %in% c(339, 380), most_detailed := 1]
parent_cols <- paste0("parent_",min(reis$level):max(reis$level))
reis <- reis[, .(rei_id, path_to_top_parent, most_detailed)]
reis[, (parent_cols) := tstrsplit(path_to_top_parent, ",")][, path_to_top_parent := NULL]
reis <- melt(reis, id.vars = c("rei_id", "most_detailed"), value.vars = parent_cols,
             value.name = "parent_id", variable.name = "level")
reis[, parent_id := as.integer(parent_id)]
reis <- reis[!is.na(parent_id) & (most_detailed == 1 | rei_id == parent_id), ]
reis[, level := NULL]

#--PULL RR AND COLLAPSE --------------------------------------------------------
if(rei %in% c("air_pm", "air_hap", "drugs_alcohol", "smoking_direct", "bullying",
              "activity", "air_pmhap", "nutrition_lbw", "nutrition_preterm",
              "nutrition_lbw_preterm", "drugs_illicit_suicide") | rei %like% "^diet_") {
    # modeler provides draws of RR(max) for these risks
    rr <- fread(paste0("FILEPATH/",rei,".csv"))
    rr[, rr := as.numeric(rr)]
    if (rei == "diet_salt") {
        # rrs are mediated through sbp RRs and saved to disk in PAF calc
        files <- paste0("FILEPATH/", rei, "/", loc_id, "_", demo$sex_id, ".csv")
        rr <- rbind(rr, rbindlist(lapply(files, fread), use.names=T), fill=T)
    }
    if (rei == "activity") rr[, rr := rr + .75]
    if (rei %in% c("nutrition_lbw", "nutrition_preterm", "nutrition_lbw_preterm")) {
        rr <- rr[cause_id == 302, ]
        rr[, cause_id := 380]
    }
} else if (rei == "metab_fpg") {
    files <-paste0(out_dir, "/rrmax/", c(141, 142), ".csv")
    rr <- rbindlist(lapply(files, fread), use.names=T)
    rr <- melt(rr, id.vars = c("age_group_id", "sex_id", "cause_id"),
               measure.vars = paste0("draw_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
    rr[, draw := as.numeric(gsub("draw_", "", draw))]
} else if (risk_id %in% reis[most_detailed==0,]$rei_id) {
    # joint risks
    child_risks <- reis[parent_id == risk_id & most_detailed == 1,]$rei_id
    files <- paste0(out_dir, "/rrmax/", child_risks, ".csv")
    files <- files[file.exists(files)]
    rr <- rbindlist(lapply(files, fread), use.names=T)
    rr <- melt(rr, id.vars = c("age_group_id", "sex_id", "cause_id"),
               measure.vars = paste0("draw_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
    rr[, draw := as.numeric(gsub("draw_", "", draw))]
    rr[, n_risks := .N, by=c("age_group_id","sex_id","cause_id","draw")]
    rr[, ratio := 1]
    rr[n_risks > 1, ratio := prod(rr_mean/rr)^(1/n_risks),
       by=c("age_group_id","sex_id","cause_id","draw")]
    rr <- rr[, list(rr = prod(rr)*ratio),
             by=c("age_group_id","sex_id","cause_id","draw")] %>% unique
} else {
    if(rei == "envir_lead_bone") {
        # use SBP RRs for Lead (bone) - .61 mmHg shift in SBP, convert SBP unit space to 1 SBP
        rr <- get_rr(107, loc_id, year_ids, demo$sex_id, gbd_round_id, decomp_step, n_draws)
        rr[, rr := rr^(.61/10)]
    } else {
        rr <- get_rr(risk_id, loc_id, year_ids, demo$sex_id, gbd_round_id, decomp_step, n_draws)
    }
    if(risk_id %in% yld_risks) {
        rr <- rr[morbidity == 1, ]
    } else {
        rr <- rr[mortality == 1, ]
    }
    # keep RR where 99th categ of exposure for categorical risks
    if(!cont) {
        rr[, mean_rr := mean(rr), by = c("age_group_id", "sex_id", "cause_id", "parameter")]
        min_categ <- rr[mean_rr==max(mean_rr, na.rm=T)]$parameter %>% unique
        rr <- rr[parameter == min_categ]
    }
}
# make sure it's constant across time and space (location, year)
rr <- rr[, .(rr = mean(rr)), by = c("age_group_id", "sex_id", "cause_id", "draw")]

#--PULL TMREL and EXP CAP (if continuous) --------------------------------------
if (rei %in% c('diet_salt', 'envir_lead_bone', 'envir_radon', 'metab_bmd',
               'metab_bmi_adult', 'metab_ldl', 'metab_fpg_cont', 'metab_sbp',
               'nutrition_iron')) {
    tmrel <- get_tmrel(risk_id, loc_id, year_ids, demo$sex_id, gbd_round_id,
                       decomp_step, n_draws, tmrel_lower, tmrel_upper, demo$age_group_id)
    # make sure it's constant across time and space (location, year)
    tmrel <- tmrel[, .(tmrel = mean(tmrel)), by = c("age_group_id", "sex_id", "draw")]
    # put in same space as rr
    tmrel[, tmrel := tmrel / rr_scalar]
    # exposure 99th or 1st percentile from PAF calc
    paf_cap <- fread(paste0("FILEPATH/", risk_id, ".csv"))
    if (inv_exp==1) {
        paf_cap <- paf_cap[pctile==0.01,]$val
    } else {
        paf_cap <- paf_cap[pctile==0.99,]$val
    }
    tmrel[, cap := paf_cap]
    # put in same space as rr
    tmrel[, cap := cap / rr_scalar]
    rr <- merge(rr, tmrel, by = c("age_group_id", "sex_id", "draw"))
    if(inv_exp==1) {
        rr[, diff := tmrel-cap]
    } else {
        rr[, diff := cap-tmrel]
    }
    if (rei == "diet_salt") {
        rr[cause_id != 414, rr := rr^((abs(diff)+diff)/2)]
    } else {
        rr[, rr := rr^((abs(diff)+diff)/2)]
    }
}
rr <- rr[, .(cause_id, age_group_id, sex_id, draw, rr)]

#--PULL PAFs AND MERGE ---------------------------------------------------------
if (rei %in% c("metab_bmd", "occ_hearing")) {
    # RRs for these risks aren't at the cause level, so we save them to disk in PAF calc
    # before we convert the paf so we can merge them here
    files <- paste0("FILEPATH/", loc_id, "_", demo$sex_id, ".csv")
    paf <- rbindlist(lapply(files, fread), use.names=T)
    paf <- paf[year_id %in% year_ids, ]
} else {
    if (rei %in% c("nutrition_lbw", "nutrition_preterm", "nutrition_lbw_preterm")) {
        paf <- interpolate(gbd_id_type = "rei_id", gbd_id = risk_id,
                           location_id = loc_id, measure_id = 4,
                           reporting_year_start=min(year_ids), reporting_year_end=max(year_ids),
                           gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                           source = "burdenator")
        paf <- paf[year_id %in% year_ids & metric_id == 2 & cause_id == 380, ]
    } else {
        files <- paste0(paf_dir, "/", loc_id, "_", year_ids, ".csv.gz")
        compiler <- function(f){
            df <- fread(cmd=paste0("zcat < ", f))
            if (risk_id %in% yld_risks) {
                df <- df[measure_id == 3, ]
            } else if (risk_id == 381) {
                df <- df[measure_id == 4 | (cause_id %in% c(568,571) & measure_id==3), ]
            } else {
                df <- df[measure_id == 4, ]
            }
            df <- df[rei_id == risk_id & year_id %in% year_ids, ][, rei_id := NULL]
            return(df)
        }
        paf <- rbindlist(lapply(files, compiler), use.names=T)
    }
    paf <- paf[, c("age_group_id", "sex_id", "location_id", "year_id",
                   "cause_id", paste0("draw_", 0:(n_draws - 1))), with=F]
    paf <- melt(paf, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "cause_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "paf")
    paf[, draw := as.numeric(gsub("draw_", "", draw))]
}

dt <- merge(paf, rr, by=c("age_group_id", "sex_id", "cause_id", "draw"))

#--CALC SEV ---------------------------------------------------------------------
dt[paf < 0, paf := 0]
dt[, sev := (paf/(1-paf))/(rr-1)]
dt[rr <= 1, sev := 0]
dt <- dt[is.finite(sev) & round(paf, 12) != 1, ]
dt[, rei_id := risk_id]

# write RRmax to disk
if (loc_id == 101) { # will be the same across locations, so just write it once
    rr_max <- dt[, .(rei_id, age_group_id, sex_id, cause_id, draw, rr)] %>% unique
    rr_max <- dcast(rr_max, rei_id + cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
    setnames(rr_max, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorder(rr_max, rei_id, cause_id, age_group_id, sex_id)
    write.csv(rr_max, paste0(out_dir, "/rrmax/", risk_id, ".csv"), row.names = F)
}

# average across causes to get risk-specific sev
dt <- dt[, .(sev = mean(sev)), by=c("rei_id", "location_id", "year_id", "age_group_id",
                                    "sex_id", "draw")]

#--SAVE ------------------------------------------------------------------------
dt[, measure_id := 29][, metric_id := 3]
dt <- dcast(dt, rei_id + location_id + year_id + age_group_id + sex_id + measure_id + metric_id ~ draw,
            value.var = "sev")
setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
setorder(dt, rei_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id)
dir.create(paste0(out_dir,"/draws/", risk_id), showWarnings = FALSE)
file <- paste0("/draws/", risk_id, "/", loc_id, ".csv")
write.csv(dt, paste0(out_dir, file), row.names = F)
