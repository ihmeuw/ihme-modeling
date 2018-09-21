
# Launch all stages of HIV results prep process

# Set up
rm(list=ls())

if (Sys.info()[1] == "Linux") {
  root <- "/home/j"
  user <- Sys.getenv("USER")
  code_dir <- paste0("FILEPATH")
} else {
  root <- "FILEPATH"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
}
library(data.table)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  # Set Spectrum run name
  spec_name <- args[1]
} else {
  # Set Spectrum run name
  spec_name <- ""
}

# Set toggles
prep_start <- 3
prep_end <- 3
hivrr <- T
test <- F
post.ensemble <- T

### For IND specific incidence choice between EPP and CIBA
india.epp.dir <- ""
india.ciba.dir <- ""


# Set list of countries to run age-splitting on
loc.table <- get_locations()
detailed.countries <- loc.table[most_detailed == 1, ihme_loc_id]
loc.list <- grep("IND", detailed.countries, value = T) # Updates this for a subset of locations
india.states <- loc.table[grepl("IND", ihme_loc_id) & level == 4, ihme_loc_id]
india.compile.locs <- setdiff(india.states, "IND_44538")
run.locs <- detailed.countries

##### end

agg.locs <- c("ZWE", "CIV", "MOZ", "HTI", "IND_44538", "SWE", "MDA", "ZAF", "KEN", "JPN", "GBR", "USA", "MEX", "BRA", "SAU", "IND", "GBR_4749", "CHN_44533")

parent.locs <- loc.table[spectrum ==1 & most_detailed == 0 & new_location_2016 != 1, ihme_loc_id]
parent.locs <- parent.locs[!(parent.locs %in% c("MDA_44823", "MDA_44824", "MOZ_44827"))]
parent.locs <- c(parent.locs, "IND_44538")

spec_out_dir <- paste0("FILEPATH")


find.children <- function(loc) {
  parent.loc <- loc.table[ihme_loc_id==loc, location_id]
  most.detailed <- loc.table[location_id==parent.loc, most_detailed]
  if(most.detailed==0){
    parent.level <- loc.table[location_id==parent.loc, level]
    child.list <- loc.table[parent_id==parent.loc & location_id!=parent_id & level==parent.level + 1, ihme_loc_id]
  } else {
    child.list <- loc.table[location_id==parent.loc, ihme_loc_id]
  }
  return(child.list)
}

### Tables
age_map <- data.table(get_age_map("all"))
write.csv(age_map,"FILEPATH")

##################################################################


### Launch India compile
if(any(grepl("IND", run.locs))) {
  for(loc in india.compile.locs) {
    compile <- paste0("qsub -P proj_hiv -pe multi_slot 2 ",
                                "-N ", loc, "_compile ", 
                                code_dir, "/shell_R.sh ",
                                code_dir,"/prep_spec_results/sort_IND.R ", 
                                loc, " ", india.epp.dir, " ", india.ciba.dir)
    print(compile)
    system(compile)
  }
}

### Aggregates
if (prep_start<=1) {
  for(parent in agg.locs) {
    aggregate <- paste0("qsub -P proj_hiv -pe multi_slot 10 ",
                       "-N ", "aggregate_", parent, " ",
                       "-hold_jid ", parent, "_compile ",
                       code_dir, "/shell_R.sh ",
                       code_dir,"/prep_spec_results/aggregate.R ",
                       parent, " ", spec_name)
    print(aggregate)
    system(aggregate)
  }
}

### Split
if (prep_start<=2 & prep_end >= 2 ) { 
  for(parent in parent.locs) {
    prep.loc.splits <- paste0("qsub -P proj_hiv -pe multi_slot 2 ",
                       "-N ", "prep_loc_split_", parent, " ",
                       "-hold_jid ", paste(paste0("aggregate_", agg.locs), collapse = ","), " ",
                       code_dir, "/shell_R.sh ",
                       code_dir,"/prep_spec_results/apply_location_splits.R ",
                       parent, " ", spec_name)
    print(prep.loc.splits)
    system(prep.loc.splits)
  }
}


### Split age and prep for the reckoning
if (prep_start<=3 & prep_end >= 3 ) {
  for (iso3 in  run.locs) {
    prep.age.splits <- paste0("qsub -P proj_hiv -pe multi_slot 2 ",
                       "-N ", iso3, "_age_split ",
                       "-hold_jid ", paste(paste0("prep_loc_split_", parent.locs), collapse = ","), " ",
                       code_dir, "/shell_R.sh ",
                       code_dir,"/prep_spec_results/apply_age_splits.R ",
                       iso3, " ", spec_name)
    print(prep.age.splits)
    system(prep.age.splits)
  }

}


### Prep HIV relative risk
if(hivrr) {
  temp.loc.table <- data.table(get_locations(level="estimate"))
  rr.locs <- temp.loc.table[, ihme_loc_id]
  for (loc in rr.locs) {
    plot.string <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G ",
                   "-N ", loc, "_hiv_rr ", 
                   code_dir, "/shell_R.sh ", 
                   code_dir, "/spectrum2016/hiv_rr_prep.R ",
                   loc, " ", spec_name)
    print(plot.string)
    system(plot.string)
  }
}


### End