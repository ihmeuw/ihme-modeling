## Purpose: Fit GPR to obtain smoothed mobility metric
## ----------------------------------------------------

# Load libraries
library(dplyr)
library(zoo)
library(splines)
library('tseries')
library(msm)
library(ggplot2)
library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Define user
user <- Sys.info()["user"]

# Set permissions
Sys.umask('0002')

# Define args if interactive. Parse args otherwise.
if (interactive()){
  code_dir <- "FILEPATH"
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--code_dir", type = "character", help = "Root of repository.")

  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
  
}

# Define additional args
plot_bool <- FALSE # if you want to plot along the way

# Define dirs
output_dir <- "FILEPATH"
today <- Sys.Date() %>% gsub('-', '_', .)
run_index <- ihme.covid::get_latest_output_date_index(output_dir, today)
output_subdir <- file.path(output_dir, sprintf("%s.%02i", today, run_index))
metadata_path <- file.path(output_subdir, "03_gpr.metadata.yaml")
message(paste0("Saving outpus here: ", output_subdir))

# Load location sets
locs <- fread(file.path(output_subdir, "locs.csv"))
locs_for_pub <- fread(file.path(output_subdir, "locs_for_pub.csv"))

source(paste0(code_dir,"/mobility/gpr/custom_functions.R"))
source(paste0(code_dir,"/mobility/gpr/job_hold.R"))

python_shell <- paste0(code_dir,"/mobility/gpr/python_shell.sh")
gpr_script <- paste0(code_dir,"/mobility/gpr/gpr_without_function.py") # function to calculate GPR

# load the data
df_compiled <- fread(file.path(output_subdir, "/post_outliering_indicated_data.csv"))
df_compiled[, date:=as.Date(date)]

# exclude outliered data points
df_compiled[out_flag_google == 1, change_from_normal_GOOGLE := NA ]

# calculate val and variance
df_compiled_model <- df_compiled[,.(ihme_loc_id, date,
                                      change_from_normal_GOOGLE)]
# val
df_compiled_model <- df_compiled_model[, val := change_from_normal_GOOGLE]
df_compiled_model <- df_compiled_model[!is.na(val)] 
  

# variance
df_compiled_model[, variance := var(val, na.rm=T), by="ihme_loc_id"]


# add location id
df_compiled_model <- merge(df_compiled_model,
                           unique(locs[,.(ihme_loc_id,location_id)]),by="ihme_loc_id",all.x=T)
df_compiled_model <- df_compiled_model[!is.na(location_id)]

# rename val
df_compiled_model[, change_from_normal := val]


# calculate rolling mean again
df_compiled_model_to_roll <- unique(df_compiled_model[, .(ihme_loc_id, location_id, date, change_from_normal)])
df_compiled_model_to_roll <- rolling_fun(df_compiled_model_to_roll, 20)
df_compiled_model_to_roll[, date := as.Date(date)]
df_compiled_model[, date := as.Date(date)]
df_compiled_model <- merge(df_compiled_model, df_compiled_model_to_roll[,.(ihme_loc_id, date, change_from_normal_avg)],
                           by=c("ihme_loc_id", "date"))

# plot final moving average (after outliering)
if(plot_bool){
  new_moving_avg_path <- paste0(output_subdir,"/updated_moving_average_post_outliering.pdf")
  post_out_moving_avg(new_moving_avg_path,df_compiled_model, loc_list = unique(locs_for_pub$ihme_loc_id))
}

# output final moving average (after outliering)
updated_avg_post_outliering_path <- paste0(output_subdir,"/prepped_final_moving_avg.csv")
fwrite(df_compiled_model, updated_avg_post_outliering_path)

# add variables required for st-gpr
df_compiled_model[,sex_id := 3]
df_compiled_model[,age_group_id := 22]
df_compiled_model[,sample_size := NA]
df_compiled_model[,is_outlier := 0]
df_compiled_model[,nid := 9999]
df_compiled_model[,measure := "continuous"]

# map dates to years
df_compiled_model <- setorderv(df_compiled_model,c("location_id","date"))
extra_dates <- as.Date(c('2020-12-24', '2020-12-25', '2020-12-26',
                         '2020-12-31', '2021-01-01', '2021-01-02'))
ordered_dates <- setorderv(as.data.table(c(unique(df_compiled_model$date),extra_dates)),cols="V1") 
corr_years <- seq(2019-nrow(ordered_dates)+1,2019,1)

map_years_ref <- as.data.table(cbind(ordered_dates,corr_years)) 
setnames(map_years_ref,c("V1","corr_years"),c("date","year_id"))
df_compiled_model <- merge(df_compiled_model,map_years_ref,by="date")

write.csv(map_years_ref, file.path(output_subdir, "map_years_ref.csv"), row.names = F)


df_compiled_model <- unique(df_compiled_model[,.(location_id,year_id,sex_id,age_group_id,change_from_normal_avg,val,variance,sample_size,is_outlier,measure,nid,ihme_loc_id)])

# get rid of duplicates due to duplicate variance
df_compiled_model[,count := .N, by=c("ihme_loc_id","location_id","year_id")]
df_compiled_model[,count_2 := seq_len(.N), by=c("ihme_loc_id","location_id","year_id","count")]
df_compiled_model[,max_count := max(count_2), by=c("ihme_loc_id","location_id","year_id","count")]
no_dups <- df_compiled_model[count == 1]
dups <- df_compiled_model[count > 1 & count_2 == max_count]
# combine duplicates and non duplcicates
df_compiled_model <- rbind(no_dups,dups)
df_compiled_model[,c("count","count_2","max_count") := NULL]
setnames(df_compiled_model,"variance","variance_var")



all_data_permutations_model <- as.data.table(expand.grid(ihme_loc_id = unique(df_compiled_model$ihme_loc_id),
                                                         year_id = seq(min(df_compiled_model$year_id),max(df_compiled_model$year_id),by=1)))

df_compiled_model <- merge(df_compiled_model,all_data_permutations_model,by=c("ihme_loc_id",'year_id'),all=T)


post_final_outliering_path <- paste0(output_subdir,"/ready_for_gpr.csv")
fwrite(df_compiled_model, post_final_outliering_path)


#### run python script for GPR:
### set up arguments:
df <- fread(post_final_outliering_path)
results_folder <- paste0(output_subdir,"/gpr_results/")
ref_folder <- paste0(results_folder,"/ref")
gpr_folder <- paste0(results_folder,"/gpr")
dir.create(results_folder,showWarnings = F)
dir.create(ref_folder,showWarnings = F)
dir.create(gpr_folder,showWarnings = F)

# save locations we want to run with in the results folder:
write.csv(unique(locs[,.(ihme_loc_id,location_id)]),paste0(ref_folder,"/locs.csv"))

# create a very simple linear prior:
df_linear <- copy(df)
df_linear[,mad := mad(x=val,na.rm = T),by=c("ihme_loc_id")] # calculate median absolute deviation
df_linear[,linear := mean(val,na.rm = T)]
write.csv(df_linear,paste0(results_folder,"/all_prepped_data.csv"))


# write out list of locations we want to model for:
locs_viz_param <- as.data.table(unique(locs[,ihme_loc_id]))
setnames(locs_viz_param,"V1","ihme_loc_id")
# get rid of locations that don't have any data
locs_viz_param <- locs_viz_param[ihme_loc_id %in% df_linear[!is.na(val),ihme_loc_id]]

n_jobs <- nrow(locs_viz_param)
write.csv(locs_viz_param,paste0(ref_folder,"/gpr_parameters.csv"))


## argument for qsub:
c.proj <- "project"
queue <- "q"
logs_error <- paste0("FILEPATH/", user, "/errors/gpr_covid.e%A_%a")
logs_output <- paste0("FILEPATH/", user, "/output/gpr_covid.o%A_%a")

setwd(code_dir)

## currently running for all locs, but this will not predict out for locations without any data
sbatch <- paste0("sbatch -J gpr_covid",
                 " --mem 3G -c 1 -t 00:05:00 -p ",queue," -A ",c.proj, " -C archive ",
                 " -o ", logs_output, " -e ", logs_error,
                 " -a 1-", n_jobs, " ", 
                 python_shell, " -s ",
                 gpr_script,
                 " ", output_subdir,
                 " ", 0)

system(sbatch)

### integrate a job hold here
job_hold(job_name = paste0("gpr_covid_*"))

## check if all of the files are present
locs_produced <- gsub(".csv","",list.files(paste0(results_folder,"/gpr/")))
if(length(setdiff(df_linear$ihme_loc_id,locs_produced)) > 0 & nrow(locs[ihme_loc_id %in% setdiff(df_linear$ihme_loc_id,locs_produced)]) > 0){
  stop("Some files are missing!")
  message(paste(setdiff(df_linear$ihme_loc_id,locs_produced),collapse=", "))
}


all_gpr <- as.data.table(lapply(list.files(paste0(output_subdir,"/gpr_results/gpr"),full.names=T),fread)
                         %>% rbindlist(use.names = T))
if(nrow(all_gpr[gpr_mean <= -100]) > 0){
  stop("There are rows with predicted changes <= -100!")
}

all_gpr <- merge(all_gpr,map_years_ref,by="year_id")


raw_data <- fread(updated_avg_post_outliering_path)
all_gpr[,date := as.Date(date)]
raw_data[,date := as.Date(date)]


# Plot the final results (even if plot_bool=F)
final_plot_path <- paste0(output_subdir,"/FINAL_PLOTS.pdf")
pdf(final_plot_path,width=12,height=8)
for(l in unique(all_gpr[ihme_loc_id %in% locs_for_pub$ihme_loc_id,ihme_loc_id])){
  message(l)
  print(ggplot(all_gpr[ihme_loc_id == l])+
          geom_point(aes(date,val),alpha=0.5)+
          geom_line(aes(date, gpr_mean))+
          geom_point(data=raw_data[ihme_loc_id == l], aes(date,val),shape=4)+
          theme_bw()+
          ggtitle(unique(locs[ihme_loc_id == l, location_name])))
}
dev.off()

write.csv(all_gpr, file.path(output_subdir, "/all_gpr.csv"), row.names = F)


yaml::write_yaml(
  list(
    script = "03_gpr.R",
    output_dir = output_subdir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
