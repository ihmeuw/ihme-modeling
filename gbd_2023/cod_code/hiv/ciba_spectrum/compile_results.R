### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0("FILEPATH", "/hiv_gbd/")

## Packages
library(data.table); library(assertable)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	loc <- args[1]
	stage <- args[2]
	age.group <- args[3]
	run.name <- args[4]
	n <- as.numeric(args[5])
	del.draws <- as.logical(args[6])
	compile.cov <- as.logical(args[7])
	number_k <- args[8]
	out_folder <- args[9]
} else {

	loc <- 'BRN'
	age.group <- 'five_year'
	stage <- 'stage_1'
	run.name <- "240304_platypus"  # Update with new Spectrum name 
	n <- 5
	del.draws <- F
	compile.cov <- F
	number_k <- 0
	out_folder <- "240304_platypus" 

}



draws.dir <- paste0('FILEPATH/', run.name,'/draws/', stage, '/', loc)

if(grepl("grid", run.name)){
  draws.dir <- paste0('FILEPATH/',run.name, '/' , out_folder , '/draws/', stage, '/', loc)
}

print(draws.dir)
draws <- list.files(draws.dir)

art.draws <- draws[grepl(paste0(loc, '_ART_deaths'), draws)]

if(!length(art.draws) == n){
	draws.exist <- gsub('.csv', '', gsub(paste0(loc, '_ART_deaths_'), '', art.draws))
	missing <- data.table(ihme_loc_id = loc, run_num = setdiff(paste0(1:n), draws.exist), run_stage = stage)

	##Write files with missing draws
	missing.out.dir <- paste0('FILEPATH', run.name, '/')
	dir.create(missing.out.dir, recursive = T)
	write.csv(missing, paste0(missing.out.dir, loc, '.csv'), row.names = F)
	warning(paste0('Not all draws are present, missing: ', missing[, run_num]))
}

## Compile draws
header <- c('run_num', 'year', 'sex', 'age', 'hiv_deaths', 'new_hiv', 'hiv_births',	'suscept_pop', 'non_hiv_deaths', 'total_births', 'birth_prev', 'pop_neg', 'pop_lt200', 'pop_200to350', 'pop_gt350',	'pop_art')
compiled.dt <- rbindlist(lapply(art.draws, function(f){
	draw.dt <- fread(paste0(draws.dir, '/', f))
	names(draw.dt) <- header
	return(draw.dt)
}))


compiled.dt[hiv_deaths <0, hiv_deaths := 0]
compiled.dt[new_hiv <0, new_hiv := 0]
compiled.dt[hiv_births <0, hiv_births := 0]
compiled.dt[suscept_pop <0, suscept_pop := 0]
compiled.dt[non_hiv_deaths <0, non_hiv_deaths := 0]
compiled.dt[total_births <0, total_births := 0]
compiled.dt[birth_prev <0, birth_prev := 0]
compiled.dt[pop_neg <0, pop_neg := 0]
compiled.dt[pop_lt200 <0, pop_lt200 := 0]
compiled.dt[pop_200to350 <0, pop_200to350 := 0]
compiled.dt[pop_gt350 <0, pop_gt350 := 0]
compiled.dt[pop_art <0, pop_art := 0]

print('negative values replaced if necessary')

if(grepl('_a', loc)){
  loc <- unlist(strsplit(loc, split = '_'))[1]
}
if(grepl("iterate",run.name)){
  
  dir.create(paste0("FILEPATH/",run.name,"/compiled/iterations/",number_k,"/"), recursive = TRUE, showWarnings = FALSE)
  write.csv(compiled.dt, paste0('FILEPATH/', run.name, '/compiled/iterations/',number_k,'/',loc, '_ART_data.csv'), row.names = F)

} else if(grepl("grid",run.name)){
  
  dir.create(paste0("FILEPATH/",run.name,"/",out_folder,"/compiled/",stage,"/"), recursive = TRUE, showWarnings = FALSE)
  write.csv(compiled.dt, paste0("FILEPATH/",run.name,"/",out_folder,"/compiled/",stage,"/",loc, '_ART_data.csv'), row.names = F)
  
} else {
  
  dir.create(paste0("FILEPATH/",run.name,"/compiled//",stage,"/"), recursive = TRUE, showWarnings = FALSE)
  write.csv(compiled.dt, paste0('FILEPATH/', run.name, '/compiled/',stage,"/",loc, '_ART_data.csv'), row.names = F)
  compile.cov = TRUE
}


## Delete draw files
if(del.draws){
	system(paste0("perl -e 'unlink <", draws.dir, "/", loc, "_ART_deaths_", "*.csv>' "))
}

## Compile coverage (used for forecasting)

if(compile.cov){
	cov.draws <- draws[grepl('coverage', draws)]
	header <- c('run_num',	'year',	'age',	'sex',	'type',	'coverage',	'eligible_pop')
	compiled.dt <- rbindlist(lapply(cov.draws, function(f){
		draw.dt <- fread(paste0(draws.dir, '/', f))
		names(draw.dt) <- header
		return(draw.dt)
	}))
	write.csv(compiled.dt, paste0('FILEPATH/', run.name, '/compiled/', stage, '/', loc, '_coverage.csv'), row.names = F)
}


