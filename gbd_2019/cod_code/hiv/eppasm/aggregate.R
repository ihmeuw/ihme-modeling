
### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- "FILEPATH"

## Packages
library(data.table); library(parallel); library(assertable)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  parent <- args[1]
  run.name <- args[2]
  spec.run.name <- args[3]
  ncores <- args[4]
  
} else {
  parent <- "KEN"
  run.name <- "190630_rhino2"
  spec.run.name <- "190630_rhino_combined"
  ncores <- 2
}



id.vars <- c("run_num", "year", "sex", "age")

### Paths
in.dir <- "FILEPATH"
single.age.dir <- "FILEPATH"

### Functions
library(mortdb, lib = "/home/j/WORK/02_mortality/shared/r")
source(paste0(root, "Project/Mortality/shared/functions/get_age_map.r"))
source(paste0(root, "Project/Mortality/shared/functions/get_locations.r"))
source(paste0("/share/cc_resources/libraries/current/r/get_population.R"))

### Tables
age.table <- data.table(get_age_map(type="all"))
loc.table <- as.data.table(get_locations(hiv_metadata = T, gbd_year=2019))

### Code
## Find  children
child.locs <- c()
loc.id <- loc.table[ihme_loc_id == parent, location_id]
children <- loc.table[parent_id == loc.id, location_id]
child.locs <- c(child.locs, loc.table[location_id %in% children & spectrum == 1, ihme_loc_id])
new.parents <- loc.table[location_id %in% children & spectrum != 1, location_id]

while(length(new.parents) > 0) {
  parents <- new.parents
  new.parents <- c()
  for(cparent in parents) {
    children <- loc.table[parent_id == cparent, location_id]
    child.locs <- c(child.locs, loc.table[location_id %in% children & spectrum == 1, ihme_loc_id])
    new.parents <- c(new.parents, loc.table[location_id %in% children & spectrum != 1, location_id])
  }
}	

##Read in the first child file, then append the sum of other files to reduce memory requirements  
suffix <- ".csv"
loc_i <- child.locs[1]
if(length(child.locs) > 1){

  combined.dt <- fread(paste0(in.dir, "/", loc_i, suffix))
    for(loc in child.locs[2:length(child.locs)]){
    print(loc)
    in.path <- paste0(in.dir, "/", loc, suffix)
    dt <- fread(in.path,blank.lines.skip = T)
    dt[,pop_gt350 := as.numeric(pop_gt350)]
    combined.dt <- rbind(combined.dt,dt)[, lapply(.SD, sum), by = id.vars]
    rm(dt)
  }
  out.dt <- combined.dt
} else {
  out.dt <- fread(paste0(in.dir, "/", loc_i, suffix))
}


#Add this column to prevent future issues 
if(!("suscept_pop" %in% colnames(out.dt))){
  out.dt[,suscept_pop := pop_neg]
}

out.path <- paste0(in.dir, "/", parent, suffix)
write.csv(out.dt, out.path, row.names=F)

##Under 1 splits
suffix <- "_under1_splits.csv"
id.vars <- c("year","run_num")
  loc_i <- child.locs[1]
  combined.dt <- fread(paste0(in.dir, "/", loc_i, suffix))
  for(loc in child.locs[2:length(child.locs)]){
    print(loc)
    in.path <- paste0(in.dir, "/", loc, suffix)
    dt <- fread(in.path,blank.lines.skip = T)
    combined.dt <- rbind(combined.dt,dt)[, lapply(.SD, sum), by = id.vars]
    rm(dt)
  }
  out.dt <- combined.dt

out.path <- paste0(in.dir, "/", parent, suffix)
write.csv(out.dt, out.path, row.names=F)



# Multiply summed up India locations by ratio of Minor Territories pop to non-Minor Territories India
# Only needed if we end up putting India through EPP-ASM
if(parent == "IND_44538") {
  age.map <- fread(paste0("FILEPATH","/age_map.csv"))[age %in% unique(out.dt$age), .(age_group_id, age)]
  pop.locs <- loc.table[parent_id == 163, location_id]
  pop.table <- add.age.groups(get_population(age_group_id = -1,
                                             location_id = pop.locs, year_id = -1, 
                                             sex_id = 1:2, location_set_id = 79, 
                                             gbd_round_id = 6, decomp_step="step4"))
  merged.pop <- merge(pop.table, age.map, by = "age_group_id")
  merged.pop[, sex := ifelse(sex_id == 1, "male", "female")]
  merged.pop[, c("age_group_id", "sex_id") := NULL]
  setnames(merged.pop, "year_id", "year")
  
  other.dt <- copy(merged.pop[location_id != 44538])
  other.dt <- other.dt[, .(other_pop = sum(population)), by = .(year, age, sex)]
  minor.dt <- copy(merged.pop[location_id == 44538])
  merged.dt <- merge(minor.dt, other.dt, by = c("year", "age", "sex"))
  merged.dt[, ratio := population / other_pop]
  merged.dt[, age := as.integer(age)]
  
  merged.out <- merge(out.dt, merged.dt[, .(year, age, sex, ratio)], by = c("year", "sex", "age"), all.x = T)
  val.vars <- setdiff(names(merged.out), c("year", "sex", "age", "run_num", "ratio"))
  matrix <- as.matrix(merged.out[, val.vars, with = F])
  ratio <- merged.out$ratio
  ratio.matrix <- sweep(matrix, MARGIN = 1, ratio, `*`)
  ratio.dt <- as.data.table(ratio.matrix)
  bound.dt <- cbind(merged.out[, .(year, age, sex, run_num)], ratio.dt)
  out.dt <- copy(bound.dt)
  out.path <- paste0(in.dir, "/", parent, suffix)
  write.csv(out.dt, out.path, row.names=F)
}



### End