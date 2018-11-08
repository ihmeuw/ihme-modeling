rm(list=ls())
library(data.table)
library(magrittr)
library(argparse)
library(assertable)
library(parallel)
library(mortcore)

parser <- ArgumentParser()
parser$add_argument("--out_dir", help = "where are we saving things",
                    default = "FILEPATH", type = "character")
parser$add_argument("--compare_version", help = "compare version of observed yll sevs and pafs", type = "integer")
parser$add_argument("--rrmax", help = "version of rrmax that was used to calculate the sevs (yll) used in producing expected sevs", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
parser$add_argument("--measid", help = "measure id. should be 3 and/or 4.", nargs = "+", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

best_versions <- fread(paste0(out_dir,"/best_versions.csv"))
for(etmt in best_versions$etmtid) assign(paste0("t",etmt), best_versions[etmtid==etmt, ]$best_mvid)

currentDir <- function() {
    return("FILEPATH")
}
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_outputs.R")

# build job list
demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbdrid)
submitmap <- data.table(expand.grid(age_group_id = demo$age_group_id, sex_id = demo$sex_id, measure_id = measid))
submitmap[, task_id := seq(.N)]
saveRDS(submitmap, file = sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))

#  PULL OBSERVED PAFS WHERE WE DON'T HAVE SEVS ---------------------------------

# what risk-outcome pairs
sev_rc <- rbindlist(lapply(list.files(paste0("FILEPATH/",rrmax,"/rrmax/"),
                                      full.names = T),fread), use.names = T)[, .(rei_id, cause_id)] %>% unique
sev_rc[, sev := 1]
rab_rc <- get_outputs(topic="rei", rei_id= "most_detailed", cause_id= "most_detailed",
                      measure_id= 2, rei_set_id = 2, compare_version_id=compare_version)[!is.na(val),]
rab_rc <- rab_rc[, .(rei_name, cause_name, rei_id, cause_id)][, rab := 1]
paf_one <- fread("FILEPATH/pafs_of_one.csv")[, paf := 1]
rc <- merge(rab_rc, sev_rc[rei_id %in% unique(rab_rc$rei_id)], by=c("rei_id","cause_id"),all=T)
rc <- merge(rc, paf_one[, .(rei_id, cause_id, paf)], by = c("rei_id", "cause_id"), all.x = T)
rc <- rc[is.na(paf) & is.na(sev)]
dir.create(paste0(out_dir, "/t7/v", t7,"/PAFs/gbd_inputs"), showWarnings = F)
write.csv(rc[, .(rei_id, cause_id, rei_name, cause_name)], paste0(out_dir, "/t7/v", t7,"/PAFs/gbd_inputs/risk_cause.csv"), row.names=F)

# then run the jobs
query_args <- list(out_dir = out_dir,
                   code_dir = currentDir(),
                   t7 = t7,
                   compare_version = compare_version,
                   gbdrid = gbdrid)
array_qsub(
    jobname = sprintf("epi_trans_risk_query_inputs_etmvid_%d",t7),
    shell = sprintf("%s/shells/r_shell.sh", currentDir()),
    code = sprintf("%s/t7/query_inputs.R", currentDir()),
    pass_argparse = query_args,
    slots = 10,
    mem = 20,
    proj = 'proj_epitrans',
    num_tasks = nrow(submitmap),
    submit = T
)
gbdfiles <- data.table(expand.grid(process_dir = 'PAFs/gbd_inputs',
                                   age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                   measure_id = measid))
gbdfiles[, file := sprintf('%s/obs_agid_%d_sid_%d_measid_%d.RDS', process_dir, age_group_id, sex_id, measure_id)]
check_files(filenames = gbdfiles$file, folder = sprintf('%s/t7/v%d/', out_dir, t7),
            continual = T, sleep_time = 60, sleep_end = 60)

#  FIT PAFS WHERE WE DON'T HAVE SEVS -------------------------------------------

fit_args <- list(out_dir = out_dir,
                 code_dir = currentDir(),
                 t7 = t7,
                 gbdrid = gbdrid)
array_qsub(
    jobname = sprintf("epi_trans_risk_fit_etmtid_t7_etmvid_%d", t7),
    shell = sprintf("%s/shells/r_shell.sh", currentDir()),
    code = sprintf("%s/t7/fit.R", currentDir()),
    pass_argparse = fit_args,
    slots = 46,
    mem = 60,
    proj = 'proj_epitrans',
    num_tasks = nrow(submitmap),
    submit = T
)
fitfiles <- data.table(expand.grid(process_dir = 'PAFs/gbd_fits',
                                   age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                   measure_id = measid))
fitfiles[, file := sprintf("%s/MEAN_agid_%d_sid_%d_measid_%d.RDs", process_dir, age_group_id, sex_id, measure_id)]
check_files(filenames = fitfiles$file, folder = sprintf("%s/t7/v%d/", out_dir, t7),
            continual = T, sleep_time = 60, sleep_end = 60)

#  BACK CALCULATE BURDEN -------------------------------------------------------

burden_args <- list(out_dir = out_dir,
                    code_dir = currentDir(),
                    t7 = t7,
                    t4 = t4,
                    t5 = t5,
                    compare_version = compare_version,
                    rrmax = rrmax,
                    gbdrid = gbdrid)
array_qsub(jobname = sprintf('epi_trans_risk_paf_etmtid_t7_%d', t7),
           shell = sprintf('%s/shells/r_shell.sh', currentDir()),
           code = sprintf('%s/t7/calc_paf_burden.R', currentDir()),
           pass_argparse = burden_args,
           slots = 10,
           mem = 20,
           proj = 'proj_epitrans',
           num_tasks = nrow(submitmap),
           submit = T)
if(4 %in% measid) measid <- c(measid, 1)
burdenfiles <- data.table(expand.grid(process_dir = 'PAFs/fits',
                                      age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                      measure_id = measid))
burdenfiles[, file := sprintf('%s/MEAN_agid_%d_sid_%d_measid_%d.RDs', process_dir, age_group_id, sex_id, measure_id)]
check_files(filenames = burdenfiles$file, folder = sprintf('%s/t7/v%d/', out_dir, t7),
            continual = T, sleep_time = 60, sleep_end = 60)

# MAKE DALYS -------------------------------------------------------------------

if (all(c(3,4) %in% measid)) {

    # build job list
    submitmap <- submitmap[, measure_id := 2][, task_id := NULL] %>% unique %>% .[, task_id := seq(.N)]
    saveRDS(submitmap, file = sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))

    # submit jobs
    daly_args <- list(out_dir = out_dir,
                      code_dir = currentDir(),
                      t7 = t7,
                      t4 = t4,
                      t5 = t5,
                      gbdrid = gbdrid)
    array_qsub(jobname = sprintf('epi_trans_risk_daly_etmtid_t7_%d', t7),
               shell = sprintf('%s/shells/r_shell.sh', currentDir()),
               code = sprintf('%s/t7/make_dalys.R', currentDir()),
               pass_argparse = daly_args,
               slots = 10,
               mem = 20,
               proj = 'proj_epitrans',
               num_tasks = nrow(submitmap),
               submit = T)

    # check to make sure all files present
    burdenfiles <- data.table(expand.grid(process_dir = 'PAFs/fits',
                                          age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                          measure_id = 2))
    burdenfiles[, file := sprintf('%s/MEAN_agid_%d_sid_%d_measid_%d.RDs', process_dir, age_group_id, sex_id, measure_id)]
    check_files(filenames = burdenfiles$file, folder = sprintf('%s/t7/v%d/', out_dir, t7),
                continual = T, sleep_time = 60, sleep_end = 60)
    measid <- c(measid, 2)
}


#  SQUARE AND RESAVE BY REI INSTEAD OF MEASURE --------------------------------

df <- rbindlist(lapply(list.files(sprintf('%s/t7/v%d/PAFs/fits', out_dir, t7),pattern="measid",full.names = T), readRDS),
                use.names = T) %>% data.table
square <- rbindlist(lapply(unique(df$rei_id),
                           function(x) data.table(expand.grid(etmtid = 7, etmvid = t7,
                                                              sdi = seq(0,1,0.005), rei_id = x,
                                                              cause_id = unique(df[rei_id == x]$cause_id),
                                                              sex_id = demo$sex_id, age_group_id = demo$age_group_id,
                                                              measure_id = measid, metric_id = c(2,3)))), use.names = T)
df <- merge(df, square, by = c("etmtid", "etmvid", "sdi", "rei_id", "cause_id",
                               "sex_id", "age_group_id", "measure_id", "metric_id"), all = T)
df[is.na(pred), pred := 0]
dfs <- split(df, by = c('age_group_id', 'sex_id', 'rei_id'))
mclapply(dfs,
         function(df, out_dir, t7, gbdid) {
             saveRDS(df,
                     file = paste0(out_dir, "/t7/v", t7, "/PAFs/fits/MEAN_agid_", unique(df$age_group_id), "_sid_", unique(df$sex_id), "_gbdid_", unique(df$rei_id), ".RDs"))
         },
         out_dir, t7, mc.cores = 10)
unlink(list.files(sprintf('%s/t7/v%d/PAFs/fits', out_dir, t7), pattern = "measid",full.names = T))

#  AGE SEX AGG AND SUMMARIES ---------------------------------------------------

submitmap <- data.table(expand.grid(rei_id = unique(df$rei_id)))
submitmap[, task_id := seq(.N)]
saveRDS(submitmap, file = sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))
array_qsub(jobname = sprintf('epi_trans_agg_age_sex_risk_etmtid_t7_%d', t7),
           shell = sprintf('%s/shells/r_shell.sh', currentDir()),
           code = sprintf('%s/t7/age_sex_summ.R', currentDir()),
           pass_argparse = list(out_dir = out_dir,
                                code_dir = currentDir(),
                                t7 = t7,
                                t2 = t2,
                                t3 = t3,
                                gbdrid = gbdrid),
           slots = 10,
           mem = 20,
           proj = 'proj_epitrans',
           num_tasks = nrow(submitmap),
           submit = T)
summfiles <- data.table(expand.grid(process_dir = 'PAFs/summaries', gbdid = submitmap$rei_id))
summfiles[, file := sprintf('%s/gbdid_%d.csv', process_dir, gbdid)]
check_files(filenames = summfiles$file, folder = sprintf('%s/t7/v%d/', out_dir, t7),
            continual = T, sleep_time = 60, sleep_end = 60)
