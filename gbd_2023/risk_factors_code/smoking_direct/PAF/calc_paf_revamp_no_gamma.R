###############################################################################################################
## Purpose: Calculate Smoking PAFs
###############################################################################################################

# Source libraries
message(Sys.time())
message("Preparing Environment")
source(FILEPATH)


# Read the location as an argument

# setting up for array jobs
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(paste0('task_id: ', task_id))
parameters <- fread(FILEPATH)

message("test1")

l <- parameters[task_id, location_id]
s <- parameters[task_id, sex_id]
output_path <-  parameters[task_id, output_path]
paf_path <-  parameters[task_id, paf_path]
current_prev_id <- parameters[task_id, current_prev_id]
former_prev_id <- parameters[task_id, former_prev_id]
year_list <-  parameters[task_id, year_list] %>% as.character
nolag <- parameters[task_id, nolag] %>% as.logical

print(l)
print(s)

years <- as.numeric(strsplit(year_list,",")[[1]])
years_og <- years
# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:999))

age_map <- get_age_metadata(24, release_id=16)

ages_og<-c(11:20, 30, 31, 32, 235)
# this is the file for all the RR path, need to update the path for current RR.
# use the file with no lag for GBD 2021 onward.
ref<-fread(FILEPATH)
if (s == 1) { ref<-ref[!(cause_id%in%c(429, 432))]}
if (s == 2) { ref<-ref[!(cause_id%in%c(438))]}

# remove lag if nolag==T
if(nolag){
  ref[, lag := 0]
}

# Read in the simulated histories: current smoking prevalence, former smoking prevalence, amount smoked, years smoked, pack-years, and years since quitting as a RData object
message("Loading simulated histories")
load(paste0(FILEPATH, "sex_", s, "/", l, ".RData"))
load(paste0(FILEPATH, "sex_", s, "/", l, ".RData"))

file_names <- list.files(paste0(FILEPATH, "sex_", s, "/", l, "/"),full.names = T)
file_names <- file_names[!(file_names %like% "RDataTmp")]

z <- length(file_names)
m <- 0
for (f in file_names){
  print(f)
  m <- m+1
  print(m/z)
  load(f)
}

# Resource functions
ages <- ages_og
years <- years_og

# Pull current smoking prevalence
message("Reading in Current Prevalence Draws")
current_prev<-fread(paste0(FILEPATH ,current_prev_id,"/draws_temp_0/", l, ".csv"))
temp<-current_prev[age_group_id==21]
for(i in c(30, 31, 32, 235)) {
  temp<-temp[, age_group_id:=i]
  current_prev<-rbind(current_prev, temp)
}
current_prev<-current_prev[age_group_id!=21]
current_prev<-current_prev[sex_id==s]
current_prev<-melt(current_prev, id.vars = idvars, measure.vars = drawvars)
current_prev_copy <- copy(current_prev)
setnames(current_prev_copy,"value","current")

# Pull former smoking prevalence
message("Reading in Former Prevalence Draws")

former_prev<-fread(paste0(FILEPATH ,former_prev_id,"/draws_temp_0/", l, ".csv"))
temp<-former_prev[age_group_id==21]
for(i in c(30, 31, 32, 235)) {
  temp<-temp[, age_group_id:=i]
  former_prev<-rbind(former_prev, temp)
}
former_prev<-former_prev[age_group_id!=21]
former_prev<-former_prev[sex_id==s]
former_prev<-melt(former_prev, id.vars = idvars, measure.vars = drawvars)
former_prev <- merge(former_prev,current_prev_copy,by=c("location_id","year_id","age_group_id","sex_id","variable"))
former_prev[,scaled := ifelse(value+current <= 1, value, 1-current)] # scale the current and former smokers. 
former_prev[,value := scaled]
former_prev[,c("scaled","current") := NULL]


if (F){
  load(paste0(FILEPATH, "sex_",s,"/",l,".RData"))
  
  current_prev_id_old <- 43997
  
  current_prev_old<-fread(paste0(FILEPATH, current_prev_id_old, "/", l, ".csv"))
  current_prev_old<-current_prev_old[sex_id==s]
  current_prev_old<-melt(current_prev_old, id.vars = idvars, measure.vars = drawvars)
  current_prev <- copy(current_prev_old)
  
  # Pull former smoking prevalence
  message("Reading in Former Prevalence Draws")
  former_prev_id_old <- 44441
  former_prev_old<-fread(paste0(FILEPATH, former_prev_id_old, "/", l, ".csv"))
  former_prev_old<-former_prev_old[sex_id==s]
  former_prev_old<-melt(former_prev_old, id.vars = idvars, measure.vars = drawvars)
  former_prev <- copy(former_prev_old)
}

#################################
## Calculate the PAFs
#################################

# Initialize the outframe
out<-NULL

# Loop through the causes to calculate PAFs
for (c in unique(ref$cause_id)) { 
  message(paste0("Calculating PAFs for Cause ID: ", c))
  # Set some cause-specific parameters
  path_current<-ref$path_current[ref$cause_id==c]
  path_former<-ref$path_former[ref$cause_id==c]
  exp_def<-ref$current_exp_def[ref$cause_id==c]
  exp_max<-ref$max_exp[ref$cause_id==c]
  lag<-ref$lag[ref$cause_id==c]
  
  if (exp_def == "prevalence") {
    rr_current<-fread(paste0(path_current))[cause_id==c]
    rr_current[, draw := paste0("draw_", draw)]
    setnames(rr_current, "draw", "variable")
    pafs_temp<-merge(rr_current, current_prev, by = c("variable", "age_group_id", "sex_id"))
    pafs_temp<-pafs_temp[, variable:=as.numeric(gsub(variable, pattern = "draw_", replacement = ""))]
    setnames(pafs_temp, "variable", "draw")
    pafs_temp<-pafs_temp[, paf:=((1-value)+(value*rr)-1)/((1-value)+(value*rr))]
    pafs_temp<-pafs_temp[,.(draw, age_group_id, sex_id, cause_id, location_id, year_id, paf)]
    for (age_orig in ages) {
      for (year_orig in years) {
        pafs_temp_lagged<-pafs_temp[age_group_id==lag_age_id(age_orig, lag) & year_id == year_orig - lag]
        pafs_temp_lagged<-pafs_temp_lagged[, age_group_id:=age_orig]
        pafs_temp_lagged<-pafs_temp_lagged[, year_id:=year_orig]
        out<-rbind(out, pafs_temp_lagged, fill = T)
      }
    }
  } else {
    # Read in the RR draws for that cause (current and former), age, sex
    rr_current <- fread(paste0(path_current))
    rr_former <- fread(paste0(path_former))
    
    # Cut at RR max
    if (!(is.na(exp_max))) {
      rr_current<-rr_current[exposure<=exp_max]
    }
    
    # Clean up the draws (we don't want protective effects or risk among unexposed)
    if (c != 544) {rr_former <- rr_former[rr<1, rr:=1]} # this makes sense

    # Loop through ages and do the time-independent calculations
    for (a_orig in ages) {
      print(a_orig)
      # Apply lag to age group
      a<-lag_age_id(a_orig, lag)
      # Flatten into a list
      rr_current_list<-split(rr_current[age_group_id==a & sex_id==s], by = "draw")
      # Interpolate and define a RR function
      rr_current_list<-lapply(rr_current_list, function(x) approxfun(x$exposure, x$rr, method = "linear", rule =2))
      
      # Loop through years and perform the time-dependent calculations
      for (y_orig in years) {
        print(y_orig)
        # Apply lag to year
        y<-y_orig-lag
        
        # Calculate the current smoker exposure integral (exposure-weighted RR). This line does not run...!! 
        # the cap for py is 225 and for amt is 60
        current_smoker_integral<-mapply(calc_integral, get(paste0(exp_def, "_", a, "_", s, "_", y)), rr_current_list, 0, cap_exposure(exp_def))
        
        # Calculate the former smoking risk curve based on the current smoking integral
        rr_former_temp<-rr_former[age_group_id==a & sex_id==s]
        rr_former_temp<-rr_former_temp[, max:=max(rr), by = c("draw", "age_group_id", "sex_id")]
        new_max_temp<-data.table(new_max=current_smoker_integral, draw=0:999)
        rr_former_temp<-merge(rr_former_temp, new_max_temp, by = "draw")
        
        if (c==544) {
          rr_former_temp$rr_new<-mapply(transform_former, orig_val = rr_former_temp$rr, new_max = 1, new_min = rr_former_temp$new_max)
        } else {
          rr_former_temp$rr_new<-mapply(transform_former, orig_val = rr_former_temp$rr, new_max = rr_former_temp$new_max, orig_max = rr_former_temp$max)
        }
        rr_former_list<-split(rr_former_temp, by = "draw")
        rr_former_list<-lapply(rr_former_list, function(x) approxfun(x$exposure, x$rr_new, method = "linear", rule =2))
        
        # Calculate the former smoking intergral (exposure-weighted RR)
        former_smoker_integral<-mapply(calc_integral, get(paste0("cess_", a, "_", s, "_", y)), rr_former_list, 0, age_map[age_group_id==a, age_group_years_end]) # use age_end "age_map[age_group_id==a, age_group_years_end]" instead of age_start as the maximum value
        
        # Calculate the PAF
        current_prev_vec<-as.vector(current_prev$value[current_prev$age_group_id==a & current_prev$sex_id==s & current_prev$year_id == y])
        former_prev_vec<-as.vector(former_prev$value[former_prev$age_group_id==a & former_prev$sex_id==s & former_prev$year_id == y])
        pafs_temp<-((1-(current_prev_vec+former_prev_vec)) + (former_prev_vec*former_smoker_integral) + (current_prev_vec*current_smoker_integral) - 1) / ((1-(current_prev_vec+former_prev_vec)) + (former_prev_vec*former_smoker_integral) + (current_prev_vec*current_smoker_integral))
        
        # Format the output
        pafs_temp<-data.table(cause_id = c, location_id = l, year_id = y_orig, age_group_id = a_orig, sex_id = s, draw = 0:999, paf = pafs_temp)

        out<-rbind(out, pafs_temp, fill = T)
      }
    }
  }
}

# Expand causes
# TB
# without drug resistant TB
if (297 %in% unique(out$cause_id)) { out<-expand_causes(data_in = out, cause_in = 297, causes_out = c(934, 946, 947, 954), drop_orig = T) }
# with drug-resistant TB
# Liver Cancer
if (417 %in% unique(out$cause_id)) {out<-expand_causes(data_in = out, cause_in = 417, causes_out = c(418, 419, 420, 421, 996), drop_orig = T)}
# Leukemia
# without myeloid and lyphoid leukemia broad categories
if (487 %in% unique(out$cause_id)) {out<-expand_causes(data_in = out, cause_in = 487, causes_out = c(845, 846, 847, 848, 943), drop_orig = T)}
# with myeloid and lyphoid leukemia broad categories
# Stroke
if (494 %in% unique(out$cause_id)) {out<-expand_causes(data_in = out, cause_in = 494, causes_out = c(495, 496, 497), drop_orig = T)}
# Diabetes
if (587 %in% unique(out$cause_id)) {out<-expand_causes(data_in = out, cause_in = 587, causes_out = c(976), drop_orig = T)}

# Format for save
out<-out[, c("mortality", "morbidity"):=1]
out<-out[, rei_id:=99]
sex_id<-s
location_id<-l

dir.create(paste0(FILEPATH, "pafs_ss_annual/"), showWarnings = T)
save_paf(dt = out, rid=99, rei="smoking_direct", n_draws=1000, out_dir=paste0(FILEPATH, "/pafs_ss_annual/"))

# Save summaries
out<-out[, mean:=mean(paf), by = c(idvars, "cause_id")]
out<-out[, lower:=quantile(paf, 0.025), by = c(idvars, "cause_id")]
out<-out[, upper:=quantile(paf, 0.975), by = c(idvars, "cause_id")]
out<-unique(out[,.(location_id, year_id, age_group_id, sex_id, cause_id, mean, lower, upper)])


dir.create(paste0(FILEPATH,"/paf_summaries_ss_annual/"), showWarnings = F)
write.csv(out, paste0(FILEPATH,"/paf_summaries_ss_annual/", l, "_", s, ".csv"), na = "", row.names = F)

