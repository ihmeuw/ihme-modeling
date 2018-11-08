##########################################################
## Epi transition draw summarizer
##########################################################
## DRIVE MACROS
rm(list = ls())
if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin") {
    j <- "FILEPATH"
    h <- "FILEPATH"
}

##########################################################
## Load Dependencies
source(paste0(c, "/helpers/primer.R"))

# Parse arguments
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 4, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 15, type = "integer"
)
parser$add_argument("--summ_steps",
                    help = "Processes that need summarization", nargs = "+",
                    default = c("raked", "YLLs"), type = "character"
)
args <- parser$parse_args()
list2env(args, environment())
rm(args)
taskid <- ifelse(interactive(), 1, Sys.getenv("SGE_TASK_ID"))
print(taskid)
gbdid <- readRDS(sprintf("%s/t%d/v%d/submitmap.RDS", data_dir, etmtid, etmvid))[task_id == taskid, unique(gbd_id)]
agids <- c(1:28, 30:32, 235)
sids <- 1:3

# Load data and summarize
df <- et.getProduct(
    etmtid = etmtid, etmvids = etmvid,
    data_dir = data_dir,
    agids = agids,
    sids = sids,
    gbdids = gbdid,
    process_dirs = summ_steps,
    mean = T,
    scale = "normal"
)
if (etmtid == 4 & gbdid == 294) {
    df <- rbind(
        et.getProduct(
            etmtid = etmtid, etmvids = etmvid,
            data_dir = data_dir,
            agids = agids,
            sids = sids,
            gbdids = gbdid,
            process_dirs = "YLLs",
            mean = T,
            scale = "normal"
        ),
        et.getProduct(
            etmtid = 1, etmvids = 14,
            data_dir = data_dir,
            agids = agids,
            sids = sids,
            gbdids = gbdid,
            process_dirs = "fits",
            mean = T,
            scale = "normal"
        )
    )
}
df <- df[, .(mean = mean(pred, na.rm = TRUE), lower = quantile(pred, 0.025, na.rm = TRUE), upper = quantile(pred, 0.975, na.rm = TRUE)), by = eval(names(df)[!names(df) %in% c("draw", "sim", "pred", "ax_mx")])]
df <- df[, .(etmtid, etmvid, age_group_id, sex_id, measure_id, metric_id, sdi, mean, lower, upper)][order(sdi, age_group_id, sex_id)]
assert_ids(df, id_vars = list(age_group_id = agids, sex_id = sids, measure_id = unique(df$measure_id), metric_id = unique(df$metric_id), sdi = seq(0, 1, .005)))

# Save
write.csv(df,
          file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/summaries/gbdid_", gbdid, ".csv"),
          row.names = FALSE
)
