### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- "FILEPATH"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0("FILEPATH/hiv_gbd/")

## Packages
library(data.table); library(parallel); library(assertable); library(dplyr)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  parent <- args[1]
  run.name <- args[2]
  ncores <- args[3]
} else {
  parent <- "GBR"
  run.name <- "230809_meixin"
  ncores <- 20
}

n.draws <- 1000
stages <- c("stage_1")
suffix <- "_ART_data.csv"
id.vars <- c("run_num", "year", "sex", "age")


### Paths
in.dir <- paste0("FILEPATH/", run.name, "/compiled/")
single.age.dir <- paste0('FILEPATH/', run.name,'/detailed_deaths/')

### Functions
library(mortdb, lib = "FILEPATH")
source("FILEPATH/get_population.R")

add.age.groups <- function(pop.dt) {
  pop <- pop.dt[, c("year_id", "sex_id", "age_group_id", "population", "location_id"), with = F]
  if(!(1 %in% unique(pop$age_group_id))) {
    u5.gbd.pop <- pop[age_group_id %in% c(2,3,238,34,388,389)]
    u5.gbd.pop[, age_group_id := 1]
    u5.gbd.pop <- u5.gbd.pop[,.(population = sum(population)), by = c("year_id", "sex_id", "age_group_id", "location_id")]
    pop <- rbind(pop, u5.gbd.pop)	
  }
  if(!(21 %in% unique(pop$age_group_id))) {
    o80.gbd.pop <- pop[age_group_id %in% c(30:32, 235) ]
    o80.gbd.pop[, age_group_id := 21]
    o80.gbd.pop <- o80.gbd.pop[,.(population = sum(population)), by = c("year_id", "sex_id", "age_group_id", "location_id")]
    pop <- rbind(pop, o80.gbd.pop)	
  }
  return(pop)
}


### Tables
age.table <- data.table(get_age_map(type="all", gbd_year = 2020))
loc.table <- as.data.table(get_locations(hiv_metadata = T))


## We want to pull UTLA stage 2 results, so set location table logic to not pull state results
loc.table[grepl('GBR', ihme_loc_id) & level == 5, spectrum := 0]
loc.table[grepl('GBR', ihme_loc_id) & level == 4,]

### Code
## Find Spectrum children
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


# Read in child data
combined.dt <- rbindlist(
  mclapply(child.locs,
           function(loc) {
             for(stage in stages) {
               in.path <- paste0(in.dir, stage, "/", loc, suffix)
               if(file.exists(in.path)) {
                 out.stage <- stage
                 break
               }
             }
             dt <- fread(in.path)
           }
           , mc.cores = ncores)
)


# Determine stage of input data for output
loc <- child.locs[1]
for(stage in stages) {
  in.path <- paste0(in.dir, stage, "/", loc, suffix)
  if(file.exists(in.path)) {
    out.stage <- stage
    break
  }
}


# Add up children and write parent
out.dt <- combined.dt[, lapply(.SD, sum), by = id.vars]

# Multiply summed up India locations by ratio of Minor Territories pop to non-Minor Territories India
if(parent == "IND_44538") {
  age.map <- get_age_map()[, .(age_group_id, age = age_group_name_short)]
  age.map <- age.map[age %in% out.dt$age]
  pop.locs <- loc.table[parent_id == 163, location_id]
  pop.table <- get_population(age_group_id = -1,
                              location_id = pop.locs, year_id = -1, 
                              sex_id = 1:2, location_set_id = 79, 
                              release_id = 9)
  merged.pop <- merge(pop.table, age.map[!age_group_id==30], by = "age_group_id")
  merged.pop[, sex := ifelse(sex_id == 1, "male", "female")]
  merged.pop[, c("age_group_id", "sex_id") := NULL]
  setnames(merged.pop, "year_id", "year")
  
  other.dt <- copy(merged.pop[location_id != 44538])
  other.dt <- other.dt[, .(other_pop = sum(population)), by = .(year, age, sex)]
  minor.dt <- copy(merged.pop[location_id == 44538])
  merged.dt <- merge(minor.dt, other.dt, by = c("year", "age", "sex"))
  merged.dt[, ratio := population / other_pop]
  merged.dt[, age := as.integer(age)]
  merged.dt[, sex := as.character(sex)]
  merged.dt.22 <- merged.dt[year==2022]
  merged.dt.22[, year := 2023]
  merged.dt <- rbind(merged.dt, merged.dt.22)
  merged.dt.22[, year := 2024]
  merged.dt <- rbind(merged.dt, merged.dt.22)
  
  merged.out <- out.dt %>% left_join(merged.dt[, .(year, age, sex, ratio)]) %>% data.table()
  val.vars <- setdiff(names(merged.out), c("year", "sex", "age", "run_num", "ratio"))
  matrix <- as.matrix(merged.out[, val.vars, with = F])
  ratio <- merged.out$ratio
  ratio.matrix <- sweep(matrix, MARGIN = 1, ratio, `*`)
  ratio.dt <- as.data.table(ratio.matrix)
  bound.dt <- cbind(merged.out[, .(year, age, sex, run_num)], ratio.dt)
  out.dt <- copy(bound.dt)
}


in.dir <- paste0("FILEPATH/", run.name, "/compiled/")
out.path <- paste0(in.dir, out.stage, "/", parent, suffix)
write.csv(out.dt, out.path, row.names=F)

