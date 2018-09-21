################################################################################
## Purpose: Launch Spectrum, Ensemble and CIBA
## Date created: 
## Date modified:
## Run instructions: 
## Notes:
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0(ifelse(windows, "FILEPATH")

## Packages
library(data.table)


### Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {

} else {
	
	# Process toggles
	prep.inputs <- F
	stage1 <- T
	ciba <- T
	stage2 <- T
	prep.output <- F

	# Run arguments
	run.name <- ""  # Update for each new run
	cluster.project <- "proj_hiv"
	n.jobs <- 200
	n.runs <- 5
	stage.1.inc.adj <- 1
	test <- F
}

#### Stretegy table 

#### Loop all the permutation run 
# for (run.name in run.name.list) {
# run.name <- run.name.list[1]
### Functions

del_files <- function(dir, prefix = "", postfix = "") {
	system(paste0("perl -e 'unlink <", dir, "/", prefix,  "*", postfix, ">' "))
}

### Paths
shell.dir <- paste0("FILEPATH")
code.dir <- paste0("FILEPATH")
ciba.dir <- paste0("FILEPATH")

adj.inc.dir <- paste0(root, "FILEPATH")
adj.ratios.dir <- paste0(root, "FILEPATH")
config.dir <- paste0(FILEPATH)
input.folders.path <- paste0(FILEPATH)
stage1.out.dir <- paste0("FILEPATH")
stage2.out.dir <- paste0("FILEPATH")

## Create output directories
for(path in c(adj.inc.dir, adj.ratios.dir, paste0(config.dir, c("FILEPATH")))) {
	dir.create(path, recursive = T, showWarnings = F)
}

# Prep input folders sheet if needed
if(!file.exists(input.folders.path)) {
	dt <- fread(paste0(root, "FILEPATH"))
	write.csv(dt, input.folders.path, row.names = F)
}

### Tables
country.types <- fread(paste0(root, "FILEPATH"))
type.config <- fread(paste0(root, "FILEPATH"))
duration.type.config <- fread(paste0(root, "FILEPATH"))
loc.table <- get_locations()

### Code

## Get locations
spec.locs <- sort(loc.table[spectrum == 1, ihme_loc_id])
# rerun.list <-loc.table[group == "2C"  & spectrum == 1, ihme_loc_id]
# rerun.list <- c("ARG", "CHL", "URY")
# st.gpr.locs <- loc.table[location_id %in% unique(st.gpr$location_id), ihme_loc_id]
ind.locs <- loc.table[parent_id == 11, ihme_loc_id]
loc.list <- ind.locs 
stage1.list <- intersect(spec.locs, loc.list)
ciba.locs <- intersect(loc.table[group %in% c("1B","2A", "2B") & spectrum == 1, ihme_loc_id], loc.list)
ciba.ratio.locs <- intersect(loc.table[group %in% c("2C") & spectrum == 1, ihme_loc_id], loc.list)
stage2.list <- intersect(c(ciba.locs, ciba.ratio.locs), loc.list)

## Prep Input Data
if(prep.inputs) {
	prep.data <- paste0("qsub -pe multi_slot 40 -P ", cluster.project, " -l mem_free=2G ",
											"-N prep_data ", 
											shell.dir, "shell_R.sh ", 
											code.dir, "/prep_inputs/prep_inputs.R ", 
											run.name, " 40")
	print(prep.data)
	system(prep.data)
}

## First Stage Spectrum
if(stage1) {
	# Setup testing configuration
	if(test) {
		stage1.list <- stage1.list[1]
		n.jobs <- 1
		n.runs <- 1
	}
	for(loc in stage1.list) {
		# Launch first stage Spectrum
		for(i in 1:n.jobs) {
			first.spectrum <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G -l mortality=1 ", 
															 "-hold_jid prep_data ",
															 "-N ", loc, "_", gsub("/", "_", run.name), "_spectrum_", i, " ", 
															 shell.dir, "python_shell.sh ", 
															 code.dir, "cohort_spectrum.py ", 
															 loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1")
			print(first.spectrum)
			system(first.spectrum)
		}

		# Compile results
		compile <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G ",
											"-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum_", 1:n.jobs), collapse=","), " ", 
											"-N ", loc, "_", gsub("/", "_", run.name), "_compile ", 
											shell.dir, "python_shell.sh ", 
											code.dir, "compile_results.py ", 
											loc, " '", config.dir, "' stage_1 five_year ", run.name)
		print(compile)
		system(compile)

		# Compile cohort
		compile.cohort <- paste0("qsub -pe multi_slot 4 -P ", cluster.project, " -l mem_free=2G ",
											"-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum_", 1:n.jobs), collapse=","), " ", 
											"-N ", loc, "_", gsub("/", "_", run.name), "_compile_cohort ", 
											shell.dir, "shell_R.sh ", 
											code.dir, "compile_cohort.R ", 
											loc, " ", run.name, " 4")
		print(compile.cohort)
		system(compile.cohort)
	}
}

## CIBA
if(ciba) {
	# Launch CIBA for locations with vital registration data
	for(loc in ciba.locs) {
		ciba.hold <- paste(paste0(loc, "_", gsub("/", "_", run.name), c("_compile", "_compile_cohort")), collapse=",")
		ciba.string <- paste0("qsub -pe multi_slot 6 -P ", cluster.project, " -l mem_free=2G ",
									 "-hold_jid ", ciba.hold, " ",
									 "-N ", loc, "_", gsub("/", "_", run.name), "_ciba ", 
									 shell.dir, "shell_R.sh ", 
									 ciba.dir, "incidence_adjustment_duration_Group1_countries_single_age_run_toggle.r ", 
									 loc, " ", run.name, " 2A")
		print(ciba.string)
		system(ciba.string)
	}
		
	# Launch CIBA for locations without high quality vital registration data
	for(loc in ciba.ratio.locs) {
		ciba.hold <- paste(paste0(ciba.locs, "_", gsub("/", "_", run.name), "_ciba"), collapse=",")
		ciba.string <-  paste0("qsub -pe multi_slot 6 -P ", cluster.project, " -l mem_free=2G ",
										"-hold_jid ", ciba.hold, " ",
										"-N ", loc, "_", gsub("/", "_", run.name), "_ciba ", 
										shell.dir, "shell_R.sh ", 
										ciba.dir, "Group_2C_ciba.R ", 
										loc, " ", run.name, " 2C")
		print(ciba.string)
		system(ciba.string)
	}	
}

## Second Stage Spectrum
if(stage2) {
	for(loc in stage2.list) {
		# Launch second stage Spectrum
		for(i in 1:n.jobs){
			second.spectrum <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G -l mortality=1 ", 
															 "-hold_jid ", loc, "_", gsub("/", "_", run.name), "_ciba ",
															 "-N ", loc, "_", gsub("/", "_", run.name), "_spectrum2_", i, " ", 
															 shell.dir, "python_shell.sh ", 
															 code.dir, "cohort_spectrum.py ", 
															 loc, " ", run.name, " ", i, " ", n.runs, " 0 stage_2")
			print(second.spectrum)
			system(second.spectrum)
		}

		# Compile results
		compile <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G ",
											"-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum2_", 1:n.jobs), collapse=","), " ", 
											"-N ", loc, "_", gsub("/", "_", run.name), "_compile2 ", 
											shell.dir, "python_shell.sh ", 
											code.dir, "compile_results.py ", 
											loc, " '", config.dir, "' stage_2 five_year ", run.name)
		print(compile)
		system(compile)
	}
}

## Results prep
if(prep.output) {
	# Check that results are present

	prep.output.string <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G ", 
												 "-N ", loc, "_spectrum_", i, " ", 
												 shell.dir, "shell_R.sh ", 
												 home.dir, "prep_spec_results/00_launch_result_prep.R ", run.name)
	print(prep.output.string)
	system(prep.output.string)
}

}

### End