# Given a rei (or other entity) and microdata, fit ensemble weights to distribution

# required args:
# rei - (str) what entity (does not need to actually be an rei) are you fitting weights for? if filepath already exists, will create a new version within that folder, otherwise will make a new one
# microdata - (str or datatable/frame) can either be a file path to the data or the dataframe/table itself if you have it in memory. Code will then save to filepath for reproducibility. 
# optional args:
# fit_by_sex - (bool) do you want to fit weights by sex? Defaul to TRUE.
# version - (int) version of results to filepath, will default to autoincrement. if specified, will wipe version if it already exists.
# cluster_proj - (str) what project to run jobs under on the cluster. Default is proj_custom_models.
# slots - (int) number of slots for each job. Default is 30, but you can profile your jobs to see if you need more or less.

# returns: nothing. Will save all outputs to filepath

library(data.table)
library(magrittr)
library(ggplot2)
library(gtools)
source("FILEPATH/get_demographics_template.R")
source("FILEPATH")
source("FILEPATH/get_location_metadata.R")

# create the param_map
code_dir <- "FILEPATH"
proj <- "PROJECT"
by_sex_only <- F

rei <- "metab_bmi"
version <- 10
microdata <- "FILEPATH/fulldata.csv"

# make directory if doesn't exist and find version otherwise delete it if it exists
dir.create(paste0("FILEPATH", rei), showWarnings = F)
if (is.null(version)) {
  version <- list.files(paste0("FILEPATH", rei, "/")) %>% as.numeric 
  if(length(version) == 0) version <- 0
  version <- max(version, na.rm = T) + 1
} else {
  unlink(paste0("FILEPATH", rei, "/", version), recursive = T)
}
message("Launching ensemble weights for ", rei, ", version ", version)

# Make diagnostic output folder
dir.create(paste0("FILEPATH", rei, "/", version), showWarnings = F)
dir.create(paste0("FILEPATH", rei, "/", version, "/detailed_weights/"), showWarnings = F)
dir.create(paste0("FILEPATH", rei, "/", version, "/diagnostics/"), showWarnings = F)

# save the microdata to the bmi dir
model_data <- fread(microdata)

req_cols <- c("nid","location_id","year_id","sex_id","age_year","data", "age_cat")
missing_cols <- setdiff(req_cols, names(model_data))
if(length(missing_cols) != 0) stop("microdata is missing the following columns: ", paste(missing_cols, collapse = ", "))

if(by_sex_only){
  model_data[, age_cat := 1]
}

fwrite(model_data, paste0("FILEPATH", rei, "/", version, "/microdata.csv"), row.names = F)

task_map <- model_data[,.(nid, location_id, year_id, sex_id, age_cat)] %>% unique

task_map[, rei := rei]
task_map[, version := version]
task_map[, file := paste0(nid,"_",location_id, "_", year_id, "_", sex_id, "_", age_cat, ".csv")]

# save the param_map
fwrite(task_map, paste0("FILEPATH", rei, "/", version, "/task_map.csv"), row.names = F)

# submit array jobs for fitting ensemble weights
jid <- 
  SUBMIT_ARRAY_JOB(
    script = paste0(code_dir,"/fit_ensemble_function.R"),
    queue = "all.q",
    n_jobs = nrow(task_map),
    memory = "20G",
    threads = "10",
    time = "2:00:00",
    name = paste0("fit_", rei, "_", version),
    project = proj,
    archive = T,
    args = c(
      task_map_filepath = paste0("FILEPATH", rei, "/", version, "/task_map.csv")
    )
  )

# check the jobs are all done
finished_jobs <- list.files(paste0("FILEPATH",rei, "/", version, "/detailed_weights/"), full.names = F)
unfinishe_jobs <- task_map[, file][!task_map[, file] %in% finished_jobs]

task_map_unfinished <- task_map[file %in% unfinishe_jobs]
fwrite(task_map_unfinished, paste0("FILEPATH", rei, "/", version, "/unfinished_task_map.csv"), row.names = F)

# resubmit the unfinished jobs
# submit array jobs for fitting ensemble weights
jid <- 
  SUBMIT_ARRAY_JOB(
    script = paste0(code_dir,"fit_ensemble_function.R"),
    queue = "all.q",
    n_jobs = nrow(task_map_unfinished),
    memory = "40G",
    threads = "20",
    time = "4:00:00",
    name = paste0("fit_", version),
    project = proj,
    archive = T,
    args = c(
      task_map_filepath = paste0("FILEPATH", rei, "/", version, "/unfinished_task_map.csv")
    )
  )


# compile the results and create weights by super region, sex, and age category
## Aggregate all the individual weight files
files <- list.files(paste0("FILEPATH", rei, "/", version, "/detailed_weights"), full.names = T)
if(length(files) == 0) stop("No jobs finished sucessfully.")
message("Ensemble fits complete, now making aggregate weights and diagnostics!")
out <- lapply(files, fread) %>% rbindlist(., use.names = T, fill = T)

# merge with region and super region id
locs <- get_location_metadata(release_id = 9, location_set_id = 35)
out <- merge(out, locs[,.(location_id, location_name, super_region_id, super_region_name, region_id, region_name)], by = "location_id", all.x = T)

# save out
fwrite(out, paste0("FILEPATH", rei, "/", version, "/out.csv"), row.names = F)

# aggregates values
valid_aggregates <- c("location_id", "region_id", "super_region_id", "sex_id", "year_id", "age_cat")
if(by_sex_only){
  aggregate_id <- c("super_region_id", "sex_id")
} else {
  aggregate_id <- c("super_region_id", "sex_id", "age_cat")
}

if (!all(aggregate_id %in% valid_aggregates)){
  stop("Not all provided aggregate_id(s) are valid, (", 
       paste(setdiff(aggregate_id, valid_aggregates), collapse = ", "))
}
dlist <- c(classA, classM) 
dist_cols <-  names(dlist)

## Prep global weights
global <- copy(out)
global <- global[, ..dist_cols, with = F]
global <- global[, lapply(.SD,mean)]
# scale the wts to 1
global[, (dist_cols) := lapply(.SD, function(x) x / sum(unlist(.SD))), .SDcols = dist_cols]

sq_template <- get_demographics_template(gbd_team = "epi", release_id = 9)
global[, key := 1]
sq_template[, key := 1]
sq_template <- setkey(sq_template) %>% unique
global <- merge(sq_template, global, by="key", all.x = T)
global[, key := NULL]
write.csv(global, paste0("FILEPATH", rei, "/", version,
                         "/global_weights.csv"), na = "", row.names = F)

# prep weights by sex, age_cat and super region
wts_sr_sex_age <- copy(out)
wts_sr_sex_age <- wts_sr_sex_age[, lapply(.SD,mean), .SDcols = dist_cols, by=c("super_region_name",aggregate_id)]
# scale the wts to 1
wts_sr_sex_age[, (dist_cols) := .SD / rowSums(.SD), .SDcols = dist_cols]
# merge with the template
sq_template <- merge(sq_template, locs[,.(location_id, super_region_id)], by = "location_id", all.x = T)
sq_template[age_group_id %in% c(2,3,238,388,389,34,6:8), age_cat := 1]
sq_template[age_group_id %in% c(9:12), age_cat := 2]
sq_template[age_group_id %in% c(13:16), age_cat := 3]
sq_template[age_group_id %in% c(17:20,30:32,235), age_cat := 4]

wts_sr_sex_age <- merge(sq_template, wts_sr_sex_age, by = aggregate_id, all.x = T)
wts_sr_sex_age[, key := NULL]

# save the weights by super region, sex and age cat
fwrite(wts_sr_sex_age, paste0("FILEPATH", rei, "/", version,
                              "/weights.csv"), na = "", row.names = F)

## Prep data for heat mapping
out <- out[order(as.numeric(location_id))]
out_melted <- suppressWarnings(melt(out[,c("location_id", "year_id", "sex_id", "age_cat", "nid", dist_cols), with = F], 
                                    id.vars = c("location_id", "year_id", "sex_id", "age_cat", "nid")))
if(by_sex_only){
  out_melted[, id := .GRP, by = c("location_id", "year_id", "sex_id", "nid")]
} else {
  out_melted[, id := .GRP, by = c("location_id", "year_id", "sex_id", "age_cat", "nid")]
}


## Make heatmap
pdf(paste0("FILEPATH", rei, "/", version, "/dist_weight_heatmap.pdf"))
plot<-ggplot(out_melted, aes(x = variable, y = id)) +
  geom_tile(aes(fill=value), colour = "white") + 
  theme_bw() +
  scale_fill_gradient(low = "white", high = "steelblue") +
  theme(axis.text.x=element_text(angle=45, hjust = 1)) +
  labs(x = "Distribution", y = "Source") +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank())
print(plot)
dev.off()
