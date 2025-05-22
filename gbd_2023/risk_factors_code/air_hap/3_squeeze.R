# Purpose: squeeze disaggregated fuel-type proportions to overall dirty category

# -----------------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

project <- "USERNAME "
sge.output.dir <- " -o FILEPATH -e FILEPATH "

# load packages, install if missing
lib.loc <- paste0(h_root, "R/", R.Version()$platform, "/", R.Version()$major, ".", R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","pbapply","dplyr","ggplot2","parallel","readr")

for(p in packages){
  if(p %in% rownames(installed.packages())==F){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Values ###########################################
cores_provided = 10
fuels <- c("coal","crop","dung","wood")

# specify bundle IDs
clean_bundle_id <- 6164
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

# the date you saved the bundle versions
# if you saved multiple on one day go to line 87
bundle_date <- "7/16/2024"  # TODO change this when you want to squeeze a new version
run.date <- as.character("07/16/2024")  # TODO change this when you want to squeeze a new version

# set squeeze version ID
squeeze.version.id <- 1  # first squeeze attempt; squeezing to solid
squeeze.version.id <- 2  # squeeze for prelim burdenator run
squeeze.version.id <- 3  # with outliers
squeeze.version.id <- 4  # outliers part 2 for burdenator
squeeze.version.id <- 5  # GBD2023, new data

loc_set <- 35 #location set
release <- 16 #release id

# -----------------------------Functions and directories------------------------------------------------
source(file.path(central_lib, "FILEPATH/get_model_results.R"))
source(file.path(central_lib, "FILEPATH/get_location_metadata.R"))
source(file.path(central_lib, "FILEPATH/get_bundle_version.R"))

warning("pulling locs")
locs <- get_location_metadata(location_set_id = loc_set, release_id = release)
locs <- locs[level>=3]

home_dir <- "FILEPATH"
xwalk_dir <- file.path(home_dir, "xwalk")

out_dir <- paste0(home_dir, "/squeeze")
dir.create(out_dir, recursive=T)

# create output directories for each fuel type
for (x in fuels) {
  new_out<-paste0(out_dir, "/", x, "/")
  dir.create(new_out, recursive=T)
}

# -----------------------------Get bundle data for plotting---------------------------------------------
bundle_versions <- fread(paste0(xwalk_dir, "/bundle_version_metadata.csv"))
bundle_versions <- bundle_versions[date==bundle_date]
warning("pulling bundles")
input_data <- data.table()
for (n in 1:nrow(bundle_versions)){
  temp <- get_bundle_version(bundle_versions[n, bundle_version_id], fetch="all") %>% as.data.table
  temp <- temp[, fuel_type:=bundle_versions[n, fuel_type]]
  input_data <- rbind(input_data, temp,use.names=T, fill=T)
}

# -----------------------------Get ST-GPR outputs for each of the models--------------------------------
draw_names <- paste0("draw_", 0:999)

offset <- 0.00001  # this is the offset we added to the models when running them in STGPR

warning("pulling in tracking sheet")

tracker<-as.data.table(read.xlsx("FILEPATH/stgpr_run_log_GBD2023.xlsx"))

tracker<-as.data.table(read.xlsx("FILEPATH/stgpr_run_log_GBD2023.xlsx"))

# select the rows for the run_ids you want to use
tracker <- tracker[date_submitted == run.date, ]
tracker <- tracker[!is.na(run_id)]

# create fuel-type-specific draw directories
draws_dir_coal <- paste0("FILEPATH", tracker[me_name=="hap_coal" & best==1,run_id], "FILEPATH")
draws_dir_crop <- paste0("FILEPATH", tracker[me_name=="hap_crop" & best==1,run_id], "FILEPATH")
draws_dir_dung <- paste0("FILEPATH", tracker[me_name=="hap_dung" & best==1,run_id], "FILEPATH")
draws_dir_wood <- paste0("FILEPATH", tracker[me_name=="hap_wood" & best==1,run_id], "FILEPATH")

location_wrapper <- function(loc){
  print(paste0(loc))

  # read in fuel-type draws
  coal <- fread(paste0(draws_dir_coal, loc, ".csv"))
  crop <- fread(paste0(draws_dir_crop, loc, ".csv"))
  dung <- fread(paste0(draws_dir_dung, loc, ".csv"))
  wood <- fread(paste0(draws_dir_wood, loc, ".csv"))

  # subtract the offset from these draws (added an offset in the stgpr config)
  coal[, (names(coal)[grepl("^draw_", names(coal))]) := lapply(.SD, function(x) x - offset), .SDcols = patterns("^draw_")]
  crop[, (names(crop)[grepl("^draw_", names(crop))]) := lapply(.SD, function(x) x - offset), .SDcols = patterns("^draw_")]
  dung[, (names(dung)[grepl("^draw_", names(dung))]) := lapply(.SD, function(x) x - offset), .SDcols = patterns("^draw_")]
  wood[, (names(wood)[grepl("^draw_", names(wood))]) := lapply(.SD, function(x) x - offset), .SDcols = patterns("^draw_")]


  # squeeze to old solid fuel run (before disaggregation)
  solid <- fread(paste0("FILEPATH", tracker[me_name=="hap_solid" & best==1, run_id], "FILEPATH", loc, ".csv"))

  # create a squeeze function to scale each draw to the overall solid proportion
  squeeze <- function(draw) {
    total <- (coal[[draw]] + crop[[draw]] + dung[[draw]] + wood[[draw]])
    scale <- total/solid[[draw]]
    out <- temp[[draw]]/scale
  }

  temp <- coal

  # write files
  new_col_names <- c(draw_names, "location_id","year_id","age_group_id","sex_id")
  out_coal<- bind_cols(lapply(draw_names, squeeze)) %>% as.data.table

  out_coal[, location_id:=loc]
  out_coal[, year_id:=solid$year_id]
  out_coal[, age_group_id:=22]
  out_coal[, sex_id:=3]
  out_coal <- out_coal %>%
  rename_with(~ new_col_names)

  write.csv(out_coal, paste0(out_dir, "/coal/", loc, ".csv"), row.names=F)

  # write crop files
  rm(list="temp")
  temp <- crop
  out_crop <- bind_cols(lapply(draw_names, squeeze)) %>% as.data.table

  out_crop[, location_id:=loc]
  out_crop[, year_id:=solid$year_id]
  out_crop[, age_group_id:=22]
  out_crop[, sex_id:=3]
  out_crop <- out_crop %>%
  rename_with(~ new_col_names)

  write.csv(out_crop, paste0(out_dir, "/crop/", loc, ".csv"), row.names=F)

  # write dung files
  rm(list="temp")
  temp <- dung
  out_dung <- bind_cols(lapply(draw_names,squeeze)) %>% as.data.table

  out_dung[, location_id:=loc]
  out_dung[, year_id:=solid$year_id]
  out_dung[, age_group_id:=22]
  out_dung[, sex_id:=3]
  out_dung <- out_dung %>%
  rename_with(~ new_col_names)

  write.csv(out_dung, paste0(out_dir, "/dung/", loc, ".csv"), row.names=F)

  # write wood files
  rm(list="temp")
  temp <- wood
  out_wood <- bind_cols(lapply(draw_names, squeeze)) %>% as.data.table

  out_wood[, location_id:=loc]
  out_wood[, year_id:=solid$year_id]
  out_wood[, age_group_id:=22]
  out_wood[, sex_id:=3]
  out_wood <- out_wood %>%
    rename_with(~ new_col_names)

  write.csv(out_wood, paste0(out_dir, "/wood/", loc, ".csv"), row.names=F)

  rm(list=c("coal","crop","wood","dung","solid"))

  print(paste0("Done with ", locs[location_id==loc, location_name]))
}

# perform the function across all locations
mclapply(locs$location_id, location_wrapper, mc.cores=cores_provided)

print("Done with squeeze for all locs!")


# -----------------------------Create some diagnostic plots---------------------------------------------

# compare squeezed fuel types to each other, on the same plot with solid GBD current & solid prev

pdf(paste0(out_dir, "/fuels.pdf"), width=11, height=8.5, onefile = T)

location_wrapper <- function(loc){

  # read in fuel-type draws
  read_n_prep <- function(fuel){
    squeezed <- fread(paste0(out_dir, "/", fuel, "/", loc, ".csv"))

    squeezed[, mean:=rowMeans(.SD), .SD=c(grep("draw_",names(squeezed), value=T))]
    squeezed[, lower:=quantile(.SD,0.025), .SD=c(grep("draw_",names(squeezed),value=T)), by=1:nrow(squeezed)]
    squeezed[, upper:=quantile(.SD,0.975), .SD=c(grep("draw_",names(squeezed),value=T)), by=1:nrow(squeezed)]
    squeezed[, type:=fuel]
    squeezed <- squeezed[, list(location_id,year_id,age_group_id,sex_id,mean,lower,upper,type)]
  }

  plot <- rbindlist(lapply(fuels, read_n_prep))

  # read in solid results
  solid_20 <- get_model_results(gbd_team="epi", gbd_id=2511, age_group_id=22, location_id=loc, sex_id=c(1,2), decomp_step="iterative", year_id=1990:2022, gbd_round_id=7) %>% as.data.table
  solid_19 <- get_model_results(gbd_team="epi", gbd_id=2511, age_group_id=22, location_id=loc, sex_id=c(1,2), decomp_step="step4", year_id=1990:2019, gbd_round_id=6) %>% as.data.table

  solid_20 <- solid_20[, list(location_id,year_id,age_group_id,sex_id,mean,lower,upper)]
  solid_19 <- solid_19[, list(location_id,year_id,age_group_id,sex_id,mean,lower,upper)]

  solid_20[, type:="solid_gbd20"]
  solid_19[, type:="solid_gbd19"]

  # bind all results together
  plot <- rbindlist(list(plot, solid_20, solid_19), use.names=T)

  gg <- ggplot() +
    geom_point(data=plot[year_id%in%1990:2022], aes(x=year_id,y=mean,color=type)) +
    geom_ribbon(data=plot[year_id%in%1990:2022], alpha=.2,aes(ymin=lower,ymax=upper,y=mean,x=year_id,fill=type)) +
    geom_point(data=input_data[location_id==loc & is_outlier==0], aes(x=year_id,y=val,color=fuel_type)) +
    geom_errorbar(data=input_data[location_id==loc & is_outlier==0], aes(ymin = lower, ymax = upper, x = year_id,color=fuel_type)) +
    ggtitle(paste0("HAP disaggregation: ",locs[location_id==loc,location_name]," ",loc)) +
    theme_bw()
  print(gg)

}

mclapply(locs$location_id, location_wrapper, mc.cores=cores_provided)

dev.off()

# compare squeezed to unsqueezed
location_wrapper <- function(loc){
  if (fuel=="coal"){
    fuel_dir <- draws_dir_coal
  } else if (fuel=="crop"){
    fuel_dir <- draws_dir_crop
  } else if (fuel=="dung"){
    fuel_dir <- draws_dir_dung
  } else {
    fuel_dir <- draws_dir_wood
  }

  # read in fuel-type draws
  unsqueezed <- fread(paste0(fuel_dir, loc, ".csv"))
  squeezed <- fread(paste0(out_dir, "/", fuel, "/", loc, ".csv"))

  unsqueezed[, mean:=rowMeans(.SD),.SD=c(grep("draw_", names(unsqueezed), value=T))]
  unsqueezed[, lower:=quantile(.SD,0.025), .SD=c(grep("draw_", names(unsqueezed), value=T)), by=1:nrow(unsqueezed)]
  unsqueezed[, upper:=quantile(.SD,0.975), .SD=c(grep("draw_", names(unsqueezed), value=T)), by=1:nrow(unsqueezed)]
  unsqueezed[, squeezed:="unsqueezed"]

  squeezed[, mean:=rowMeans(.SD), .SD=c(grep("draw_",names(squeezed), value=T))]
  squeezed[, lower:=quantile(.SD,0.025), .SD=c(grep("draw_",names(squeezed), value=T)), by=1:nrow(squeezed)]
  squeezed[, upper:=quantile(.SD,0.975), .SD=c(grep("draw_",names(squeezed), value=T)), by=1:nrow(squeezed)]
  squeezed[, squeezed:="squeezed"]

  plot <- rbind(unsqueezed[, list(location_id,year_id,age_group_id,sex_id,mean,lower,upper,squeezed)],
                squeezed[, list(location_id,year_id,age_group_id,sex_id,mean,lower,upper,squeezed)],
                use.names=T)

  gg <- ggplot() +
    geom_point(data=plot, aes(x=year_id, y=mean, color=squeezed)) +
    geom_ribbon(data=plot,alpha=.2, aes(ymin=lower, ymax=upper, y=mean, x=year_id, fill=squeezed)) +
    geom_point(data=input_data[location_id==loc & fuel_type==fuel], aes(x=year_id,y=val)) +
    geom_errorbar(data=input_data[location_id==loc & fuel_type==fuel], aes(ymin = lower, ymax = upper, x = year_id))+
    ggtitle(paste0(locs[location_id==loc,location_name], " ", loc, " ", fuel)) +
    theme_bw()
  print(gg)

  return(gg)

}

for(fuel in fuels){
# # make coal plots
print(fuel)
pdf(paste0(out_dir, "/squeeze_", fuel, ".pdf"), width=11, height=8.5, onefile = T)
plots <- mclapply(locs$location_id, location_wrapper, mc.cores=cores_provided)

print(plots)
dev.off()

}