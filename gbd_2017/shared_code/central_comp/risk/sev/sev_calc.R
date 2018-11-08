library(data.table)
library(magrittr)
library(gtools)
library(parallel)
library(zoo)

source("./utils/data.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics.R")

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
out_dir <- args[5]
paf_version_id <- as.numeric(args[6])
paf_dir <- paste0("FILEPATH", paf_version_id)

rei_meta <- get_rei_meta(risk_id)
rei <- rei_meta$rei
cont <- ifelse(rei_meta$calc_type == 2, TRUE, FALSE)
if (cont) {
    inv_exp <- rei_meta$inv_exp
    rr_scalar <- rei_meta$rr_scalar
    tmrel_lower <- rei_meta$tmrel_lower
    tmrel_upper <- rei_meta$tmrel_upper
}
# hearing and ergonomic and bullying we use YLD, all other YLL
yld_risks <- c(130, 132, 363)
# handle reporting risks that we use a copy of the computation risk
comp_only <- c(140, 167, 243, 141, 244, 245, 370, 371)
hier_map <- data.table(comp = comp_only,
                       rep = c(103, 135, 91, 105, 134, 134, 108, 108))

demo <- get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)

#--PULL RR AND COLLAPSE --------------------------------------------------------

if(rei %in% c("air_pm", "air_hap", "drugs_alcohol", "smoking_direct", "bullying",
              "temperature_high","temperature_low", "activity")) {
    # modeler provides draws of RR(max) for these risks
    rr <- fread(paste0("FILEPATH/",rei,".csv"))
    rr[, rr := as.numeric(rr)]
} else {
    if(rei== "drugs_illicit_suicide") {
        # RRs just calculated on the fly
        rr <- expand.grid(location_id = loc_id,
                          year_id = year_ids,
                          age_group_id = demo$age_group_id,
                          sex_id = demo$sex_id,
                          cause_id = c(721, 723),
                          parameter = "cat1",
                          mortality = 1,
                          morbidity = 0) %>% data.table
        rr_mean <- mean(c(log(6.85), log(8.16), log(8.16)))
        rr_sd <- mean(c((log(16.94) - log(3.93))/(2 * qnorm(0.975)),
                        (log(10.53) - log(4.49))/(2 * qnorm(0.975)),
                        (log(10.53) - log(4.49))/(2 * qnorm(0.975))))
        rr[, paste0("rr_", 0:(n_draws-1)) := as.list(exp(rnorm(n=n_draws,
                                                               mean=rr_mean,
                                                               sd=rr_sd)))]
        rr <- melt(rr,
                   id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                               "cause_id", "mortality", "morbidity", "parameter"),
                   measure.vars = paste0("rr_", 0:(n_draws - 1)),
                   variable.name = "draw", value.name = "rr")
        rr[, draw := as.numeric(gsub("rr_", "", draw))]
    } else if(rei == "envir_lead_bone") {
        # use SBP RRs for Lead (bone) - .61 mmHg shift in SBP, convert SBP unit space to 1 SBP
        rr <- get_rr(107, loc_id, year_ids, demo$sex_id, gbd_round_id, n_draws)
        rr[, rr := rr^(.61/10)]
    } else if(rei == "diet_salt") {
        # rrs are mediated through sbp RRs and saved to disk in PAF calc
        files <- paste0("FILEPATH/", rei, "/", loc_id, "_", demo$sex_id, ".csv")
        rr <- rbindlist(lapply(files, fread), use.names=T)
    } else if(rei %in% c("nutrition_lbw", "nutrition_preterm")) {
        rr <- get_rr(339, loc_id, year_ids, demo$sex_id, gbd_round_id, n_draws)
        tmrel <- fread("FILEPATH/nutrition_lbw_preterm_tmrel.csv")
        tmrel[, c("modelable_entity_name", "modelable_entity_id", "ga", "bw") := NULL]
        rr <- merge(rr, tmrel, by = c("sex_id", "parameter", "age_group_id"))
        child_name <- ifelse(rei == "nutrition_preterm", "preterm", "lbw")
        rr <- rr[order(location_id, year_id, age_group_id, sex_id, cause_id,
                       mortality, morbidity, draw, get(child_name),
                       get(paste0("tmrel_", child_name)))]
        rr[get(paste0("tmrel_", child_name)) == 1, tmrel_rr := rr]
        rr[, tmrel_rr := na.locf(tmrel_rr), by = child_name]
        rr[, rr := rr/tmrel_rr]
    } else {
        rr <- get_rr(risk_id, loc_id, year_ids, demo$sex_id, gbd_round_id, n_draws)
        # for vaccines and interventions, rr represents the proportion covered, so flip it
        if ((rei %like% "vacc_") | (risk_id %in% c(324, 325, 326, 328, 329, 330))) {
            rr[, rr := 1/rr]
            rr[parameter == "cat1", vaccinated := 1][, parameter := "cat1"]
            rr[vaccinated == 1, parameter := "cat2"][, vaccinated := NULL]
        }
    }
    if(risk_id %in% yld_risks) {
        rr <- rr[morbidity == 1, ]
    } else {
        rr <- rr[mortality == 1, ]
    }
    # keep RR where 99th categ of exposure for categorical risks
    if(!cont) {
        rr <- rr[, mean_rr := mean(rr), by = c("age_group_id", "sex_id", "cause_id", "parameter")]
        min_categ <- rr[mean_rr==max(mean_rr)]$parameter %>% unique
        rr <- rr[parameter == min_categ]
    }
}
# make sure it's constant across time and space (location, year)
rr <- rr[, .(rr = mean(rr)), by = c("age_group_id", "sex_id", "cause_id", "draw")]
if (rei == "air_ozone") rr[, rr := 1.157]

#--PULL TMREL and EXP CAP (if continuous) --------------------------------------

if (cont) {
    tmrel <- get_tmrel(risk_id, loc_id, year_ids, demo$sex_id, gbd_round_id,
                       n_draws, tmrel_lower, tmrel_upper, demo$age_group_id)
    # make sure it's constant across time and space (location, year)
    tmrel <- tmrel[, .(tmrel = mean(tmrel)), by = c("age_group_id", "sex_id", "draw")]
    # put in same space as rr
    tmrel[, tmrel := tmrel / rr_scalar]
    # replace 99th percentile with given values for certain risks
    tmrel[, cap := tmrel]
    if (rei == "nutrition_iron") {
        tmrel[, cap := 81.222488]
    } else if (rei == "metab_fpg_cont") {
        tmrel[age_group_id == 9, cap := 6.993982]
        tmrel[age_group_id == 10, cap := 9.096063]
        tmrel[age_group_id == 11, cap := 11.24124]
        tmrel[age_group_id == 12, cap := 13.57947]
        tmrel[age_group_id == 13, cap := 15.72667]
        tmrel[age_group_id == 14, cap := 16.80556]
        tmrel[age_group_id == 15, cap := 17.71378]
        tmrel[age_group_id == 16, cap := 18.06121]
        tmrel[age_group_id == 17, cap := 17.53536]
        tmrel[age_group_id == 18, cap := 17.00651]
        tmrel[age_group_id == 19, cap := 16.22208]
        tmrel[age_group_id == 20, cap := 15.2465]
        tmrel[age_group_id %in% c(30,31,32,235), cap := 14.20409]
    } else if (rei == "metab_bmi_adult") {
        tmrel[age_group_id == 9, cap := 45]
        tmrel[age_group_id == 10, cap := 49.07616]
        tmrel[age_group_id == 11, cap := 49.66063]
        tmrel[age_group_id == 12, cap := 50.60482 ]
        tmrel[age_group_id == 13, cap := 51.43813]
        tmrel[age_group_id == 14, cap := 51.52379]
        tmrel[age_group_id == 15, cap := 51.48124]
        tmrel[age_group_id == 16, cap := 50.82619]
        tmrel[age_group_id == 17, cap := 49.17794]
        tmrel[age_group_id == 18, cap := 47.5203]
        tmrel[age_group_id == 19, cap := 45.28234]
        tmrel[age_group_id == 20, cap := 42.6084]
        tmrel[age_group_id %in% c(30,31,32,235), cap := 39.75129]
    } else {
        # exposure 99th or 1st percentile from PAF calc
        paf_cap <- fread(paste0("FILEPATH/", rei, "/exposure/exp_max_min.csv"))$cap %>% unique
        tmrel[, cap := paf_cap]
    }
    # put in same space as rr
    tmrel[, cap := cap / rr_scalar]
}

#--PULL PAFs AND MERGE ---------------------------------------------------------

if (rei %in% c("metab_bmd", "occ_hearing")) {
    # RRs for these risks aren't at the cause level, so we save them to disk in PAF calc
    # before we convert the paf so we can merge them here
    files <- paste0("FILEPATH/", rei, "/sev/", loc_id, "_", demo$sex_id, ".csv")
    paf <- rbindlist(lapply(files, fread), use.names=T)
    paf <- paf[year_id %in% year_ids, ]
} else {
    files <- paste0(paf_dir, "/", loc_id, "_", year_ids, ".csv.gz")
    compiler <- function(f){
        df <- fread(paste0("zcat < ", f))
        if (risk_id %in% yld_risks) {
            df <- df[measure_id == 3, ]
        } else {
            df <- df[measure_id == 4, ]
        }
        df <- df[rei_id == risk_id & year_id %in% year_ids, ][, rei_id := NULL]
        return(df)
    }
    paf <- rbindlist(mclapply(files, compiler, mc.cores=4), use.names=T)
    paf <- paf[, c("age_group_id", "sex_id", "location_id", "year_id",
                   "cause_id", paste0("draw_", 0:(n_draws - 1))), with=F]
    paf <- melt(paf, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "cause_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "paf")
    paf[, draw := as.numeric(gsub("draw_", "", draw))]
}

dt <- merge(paf, rr, by=c("age_group_id", "sex_id", "cause_id", "draw"))
if (cont) {
    dt <- merge(dt, tmrel, by = c("age_group_id", "sex_id", "draw"))
}

#--CALC SEV ---------------------------------------------------------------------

if (cont) {
    if(inv_exp==1) {
        dt[, diff := tmrel-cap]
    } else {
        dt[, diff := cap-tmrel]
    }
    dt[, rr := rr^((abs(diff)+diff)/2)]
}

dt[paf < 0, paf := 0]
dt[, sev := (paf/(1-paf))/(rr-1)]
dt[rr <= 1, sev := 0]
dt <- dt[is.finite(sev) & round(paf,12) != 1, ] # make sure no pafs of 1 come through
dt[sev >= 1, sev := 1]
dt[, mean_sev := mean(sev, na.rm=T), by=c("location_id", "year_id", "age_group_id",
                                          "sex_id", "cause_id")]
dt[is.na(sev), sev := mean_sev][, mean_sev := NULL]
dt <- dt[!is.na(sev), ]
dt[, rei_id := risk_id]

# write RRmax to disk
if (loc_id == 101) { # will be the same across locations, so just write it once
    rr_max <- dt[, .(rei_id, age_group_id, sex_id, cause_id, draw, rr)] %>% unique
    rr_max <- dcast(rr_max, rei_id + cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
    setnames(rr_max, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorder(rr_max, rei_id, cause_id, age_group_id, sex_id)
    write.csv(rr_max, paste0(out_dir, "/rrmax/", risk_id, ".csv"), row.names = F)
    if (risk_id %in% comp_only) {
        repid <- hier_map[comp == risk_id, rep]
        rr_max[, rei_id := repid]
        file <- paste0(out_dir, "/rrmax/", repid, ".csv")
        if (file.exists(file)) rr_max <- rbind(rr_max, fread(file))
        write.csv(rr_max, file, row.names = F)
    }
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

# handle reporting risks that we use a copy of the computation risk
if (risk_id %in% comp_only) {
    repid <- hier_map[comp == risk_id, rep]
    dt[, rei_id := repid]
    dir.create(paste0(out_dir,"/draws/", repid), showWarnings = FALSE)
    file <- paste0(out_dir, "/draws/", repid, "/", loc_id, ".csv")
    if (file.exists(file)) dt <- rbind(dt, fread(file))
    write.csv(dt, file, row.names = F)
}
