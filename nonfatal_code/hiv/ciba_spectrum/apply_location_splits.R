################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH","FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH/", "FILEPATH"), FILEPATH )

## Packages
library(data.table)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
	parent.loc <- args[1]
	run.name <- args[2]
} else {
	parent.loc <- "IND_4849"
	run.name <- "170301_golf_redo"
}
stage.list <- c("stage_2", "stage_1")
id.vars <- c("run_num", "year", "sex", "age")

### Paths
spec.dir <- paste0("FILEPATH")

### Functions
source(paste0(code.dir, "FILEPATH"))
source(paste0(root, "FILEPATH"))

find.children <- function(parent) {
	child.list <- loc.table[parent_id==parent & location_id!=parent_id, location_id]
	final.child.list <- loc.table[location_id %in% child.list & most_detailed==1, location_id]
	need.child.list <- loc.table[location_id %in% child.list & most_detailed!=1, location_id]
	while(length(need.child.list) > 0){
		parent.list <- need.child.list
		child.list <- loc.table[parent_id %in% parent.list & location_id!=parent_id, location_id]
		add.list <- loc.table[location_id %in% child.list & most_detailed==1, location_id]
		final.child.list <- append(final.child.list, add.list)
		need.child.list <- loc.table[location_id %in% child.list & most_detailed!=1, location_id]
	}
	return(unique(final.child.list))
}

### Tables
loc.table <- get_locations()

### Code
## Determine path to parent and read in data
for(stage in stage.list) {
	parent.path <- paste0(spec.dir, stage, "FILEPATH")
	if(file.exists(parent.path)) break
}
parent.dt <- fread(parent.path)

## Read in GBD population for parent and children
parent.loc.id <- loc.table[ihme_loc_id == parent.loc, location_id]
children <- find.children(parent.loc.id)
loc.list <- c(parent.loc.id, children)
gbd.pop <- get_population(age_group_id = 22, location_id = loc.list, year_id = -1, sex_id = 3, gbd_round_id = 4)[, .(location_id, year_id, population)]
setnames(gbd.pop, "year_id", "year")

## Merge parent onto children and calculate population proportions
nat.pop <- gbd.pop[location_id == parent.loc.id, .(year, population)]
setnames(nat.pop, "population", "nat_pop")
merged.pop <- merge(gbd.pop[location_id != parent.loc.id], nat.pop, by = "year")
merged.pop[, pop_prop := population / nat_pop]

## Iterate through children, apply proportions to national data, and write seperate files for each child
for(child in children) {
	# child <- children[1]
	print(child)
	child.loc <- loc.table[location_id == child, ihme_loc_id]

	# Merge child proportions onto national data
	child.prop <- merged.pop[location_id == child, .(year, pop_prop)]
	child.dt <- merge(parent.dt, child.prop, by = "year")
	value.vars <- setdiff(names(parent.dt), id.vars)
	for(var in value.vars) {
		child.dt[, (var) := get(var) * pop_prop]		
	}
	child.dt[, pop_prop := NULL]
	# head(child.dt)
	write.csv(child.dt, paste0(spec.dir, stage, "FILEPATH"), row.names = F)
}

### End