# aggregate RR max: given a parent rei_id and the most-detailed child rei_ids,
# read the RR max for all of the children and calculate the aggregate RR max.
# Save the RR max for the parent.

library(data.table)
library(magrittr)

initial_args <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_dir <- dirname(sub(file_arg, "", initial_args[grep(file_arg, initial_args)]))
setwd(script_dir)

source("FILEPATH/get_cause_metadata.R")
source("./utils/common.R")

args <- commandArgs(trailingOnly = TRUE)

agg_rei_id <- as.numeric(args[1])
child_rei_ids <- eval(parse(text = paste0("c(",args[2],")")))
gbd_round_id <- as.numeric(args[3])
n_draws <- as.numeric(args[4])
out_dir <- args[5]

message("Aggregating RR max for ", agg_rei_id)
message("Child rei_ids: ", paste(child_rei_ids, collapse=", "))

draw_cols <- paste0("draw_", 0:(n_draws - 1))
ratio_cols <- gsub("draw", "ratio", draw_cols)

#-- READ RR MAX FOR CHILD RISKS ------------------------------------------------
rr_max <- rbindlist(
    lapply(paste0(out_dir, "/rrmax/", child_rei_ids, ".csv"),
           function(x) {
           if (file.exists(x)) fread(x) else data.table()
           }),
    use.names = TRUE)
if (nrow(rr_max) == 0) stop("nothing to aggregate")

# drop any RRs that are for most-detailed 100% attributable outcomes
# or for aggregate causes if all children are 100% attributable to single risk
one_hundred <- fread("FILEPATH/pafs_of_one.csv")[rei_id %in% child_rei_ids, ]
cause_meta <- get_cause_metadata(cause_set_id = 2, gbd_round_id = gbd_round_id)
cause_meta <- cause_meta[, .(parent_id=as.numeric(tstrsplit(path_to_top_parent, ","))),
                         by=c("cause_id", "most_detailed")]
cause_meta <- cause_meta[most_detailed == 1 | cause_id == parent_id, ]
one_hundred <- merge(one_hundred, cause_meta, by="cause_id", allow.cartesian = TRUE, all.x = TRUE)
for (r in unique(one_hundred$rei_id)) {
  for(p in unique(one_hundred[rei_id == r, ]$parent_id)) {
    if (!all(cause_meta[parent_id == p & most_detailed == 1, ]$cause_id %in%
             one_hundred[rei_id == r, ]$cause_id)) {
      one_hundred <- one_hundred[!(rei_id == r & parent_id == p), ]
    }
  }
}
one_hundred <- dataframe_unique(one_hundred[, .(rei_id, cause_id = parent_id)])
rr_max <- rr_max[!cause_id %in% one_hundred$cause_id, ]

#-- AGGREGATE RR MAX AND SAVE --------------------------------------------------
rr_max[, n_risks := .N, by=c("cause_id", "age_group_id", "sex_id")]
rr_max[, (ratio_cols) := 1]
rr_max[n_risks > 1, (ratio_cols) := lapply(
    .SD, function(x) prod(((x - 1) * .25 + 1) / x) ^ (1 / n_risks)
), .SDcols=draw_cols, by=c("cause_id", "age_group_id", "sex_id")]
rr_max[, (draw_cols) := lapply(
    1:n_draws, function(x) prod(get(draw_cols[x])) * get(ratio_cols[x])
), .SDcols=draw_cols, by=c("cause_id", "age_group_id", "sex_id")]

rr_max[, rei_id := agg_rei_id]
rr_max <- rr_max[, c("rei_id", "cause_id", "age_group_id", "sex_id", ..draw_cols)] %>%
    dataframe_unique
setorder(rr_max, rei_id, cause_id, age_group_id, sex_id)
write.csv(rr_max, paste0(out_dir, "/rrmax/", agg_rei_id, ".csv"), row.names = FALSE)
