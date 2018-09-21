###########################################################
### Purpose: Raking/Aggregation
###########################################################

###################
### Setting up ####
###################

rake_flag <- 1
draws <- 1000
collapse.draws <- T
cores <- 4
output_path <- "FILEPATH"
run_root <- ifelse(Sys.info()[1]=="Windows","J:","/home/j")
location_set_version_id <- 159

setwd("FILEPATH")

source("FILEPATH") #get locations_function
source("FILEPATH") #get populations function 

library(magrittr)
library(parallel)
library(data.table)
library(haven, lib.loc="FILEPATH")



###################################################################
# Raking blocks
###################################################################

calculate_rf <- function(df) {

	## Rakes all of children to parents for a given child level

	## Calculate parent sum
	sum <- copy(df)
	sum <- sum[, parent_sum := gpr_mean * pop_scaled]
	sum <- sum[, .(location_id, year_id, parent_sum)]
	setnames(sum, "location_id", "parent_id") 

	## Get sum of population weighted totals at level
	out <- copy(df)
  out <- out[, aggregated_sum := sum(gpr_mean * pop_scaled, na.rm=T), by=c("parent_id", "year_id")]
	## Merge parent sum on
	out <- merge(out, sum, by=c("parent_id", "year_id"), all.x=T)
	## Get ratio of parent to aggregation
	out <- out[, rake_factor := aggregated_sum/parent_sum]
  out <- out[, c("aggregated_sum", "parent_sum") := NULL]

	return(out)

}

rake_estimates <- function(df, lvl, vars, dont_rake) {
  df <- calculate_rf(df)
  ## Save unraked estimates if no draws
  if (draws == 0) df <- df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), 
                                  paste0(vars, "_unraked") := lapply(.SD, function(x) x), .SDcols=vars]
  ## Rake particular level if theres a rake factor and parent isnt an element in dont_rake
  df <- df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), 
            (vars) :=lapply(.SD, function(x) x/df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), rake_factor]), .SDcols=vars]
  ## Clear rake_factor
  df <- df[, rake_factor := NULL]
  return(df)
}

aggregate_estimates <- function(df, vars, parent_ids) {
  ## Drop locations being created by aggregation
  df <- df[!location_id %in% parent_ids]
  ## Subset to requested parent_ids
  agg <- df[parent_id %in% parent_ids] %>% copy
  key <- c("parent_id", "year_id")
  ## Aggregate estimates to the parent_id [ sum(var * pop) /sum_pop ]
  agg <- agg[, sum_pop := sum(pop_scaled), by=key]
  agg <- agg[, (vars) := lapply(.SD, function(x) x * agg[['pop_scaled']]), .SDcols=vars]
  agg <- agg[, (vars) := lapply(.SD, sum), .SDcols=vars, by=key]
  ## De-duplicate so get one set of estimates
  agg <- unique(agg[, c("parent_id", "year_id", "sum_pop", vars), with=F])
  ## Divide by sum_pop
  agg <- agg[, (vars) := lapply(.SD, function(x) x/agg[['sum_pop']]), .SDcols=vars]
  agg <- agg[, sum_pop := NULL]
  ## Rename parent_id -> location_id
  setnames(agg, "parent_id", "location_id")
  df <- rbind(df, agg, fill=T)
  return(df)
}

save_draws <- function(df) {

  ## Check that all locations exist
  locs <- get_locations(2016) %>% as.data.table %>% .[, location_id]
  missing.locs <- setdiff(locs, unique(df$location_id))
  if (length(missing.locs) != 0) stop(paste0("missing locations ", toString(missing.locs)))
  ## Restrict columns
  cols <- c("location_id", "year_id", grep("draw_", names(df), value=T))
  df <- df[, cols, with=F]

  ## Save by locs
  output_path <- "FILEPATH"
  system(paste0("rm -rf ", output_path))
  system(paste0("mkdir -m 777 -p ", output_path))
  mclapply(locs, function(x) write.csv(df[location_id==x], "FILEPATH", row.names=F, na=""), mc.cores=cores) %>% invisible
  print(paste0("Draw files saved to ", output_path))

}

###################################################################
# Raking Process
###################################################################

run_rake <- function(get.df=FALSE, collapse.draws = T) {


  ####################
  # Setup
  ####################

  ## Load
  print(class(draws))
    
    draws <- as.numeric(draws)

  if (draws > 0) df <- mclapply("FILEPATH" %>% list.files(.,full.names=T) %>% 
                                     grep(pattern = "sim", ., value = T), fread, mc.cores=cores) %>% rbindlist

  
  df[,  ':=' (sim = paste0("draw_", sim), year = year-.5)]
  
  df <- df %>% dcast.data.table(., formula = ihme_loc + year ~ sim, value.var = "mort")
  
  setnames(df, c("ihme_loc", "year"), c("ihme_loc_id", "year_id"))

	## Identify which variable names you need to rake
	if (draws > 0) vars <- paste0("draw_", seq(0, draws-1)) else vars <- c("gpr_mean", "gpr_lower", "gpr_upper")

		
  ## Set countries you don't want to rake for
  
  aggregate_ids <- c(6,95, 135) 
  
  ## Aggregate for location 6 automatically
  aggregate_ids <- aggregate_ids[!is.na(aggregate_ids)] %>% unique

    ## Merge on location hierarchy
    locs <- get_locations(2016) %>% as.data.table %>% .[, list(ihme_loc_id, location_id, parent_id, level)]
    df <- merge(df, locs, by="ihme_loc_id", all = T)
  
    ## Merge on populations

    	pops <- get_population(age_group_id = c(8:14), location_id = -1, location_set_version_id = 159, year_id = -1, sex_id = 2)
	
	pops <- pops[,list(pop_scaled = sum(population)), by = list(location_id, year_id)]
	
	
	df <- merge(df, pops, by=c("location_id", "year_id"), all.x = T)
	

  if (rake_flag) {

	  ## Calculate mean if draws
	  if (draws > 0) df <- df[, gpr_mean := rowMeans(df[, (vars), with=F])]
	  
	  ## Rake
	  for (lvl in c(4,5,6)) {
	      df <- rake_estimates(df=df, lvl=lvl, vars=vars, dont_rake=aggregate_ids)
	      df <- df[, gpr_mean := rowMeans(df[, (vars), with=F])]
	       
	  } 

	}

  #########################
  # Aggregate where needed
  #########################

  ## Aggregate
  if (!rake_flag) {
      lvl_parents <- locs[level==6, parent_id] %>% unique
      df <- aggregate_estimates(df=df, vars=vars, parent_ids=lvl_parents)
      df <- df[,c("pop_scaled", "parent_id", "level"):=NULL]
      df <- merge(df, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
      df <- merge(df, locs, by='location_id', all.x=T)
      lvl_parents <- locs[level==5, parent_id] %>% unique
      df <- aggregate_estimates(df=df, vars=vars, parent_ids=lvl_parents)
      df <- df[,c("pop_scaled", "parent_id", "level"):=NULL]
      df <- merge(df, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
      df <- merge(df, locs, by='location_id', all.x=T)
      lvl_parents <- locs[level==4, parent_id] %>% unique
      df <- aggregate_estimates(df=df, vars=vars, parent_ids=lvl_parents)
  } else {
      print(aggregate_ids)
    df <- aggregate_estimates(df=df, vars=vars, parent_ids=aggregate_ids)
  }
	
   
   stopifnot(nrow(df) == length(df[,unique(year_id)]) * length(df[, unique(location_id)]))
   

  #########################
  # Clean and save
  #########################
  	
  ## Clean
  varlist <- c("location_id", "year_id", vars)
  df <- df[, varlist, with=F]
    	  	
  ## Sort
  df <- df[order(location_id, year_id)]

  if (!get.df) {

  	
  	if (draws > 0) save_draws(df)

  } else {
      
      if (collapse.draws) {
          
          df <- melt(df, id.vars = c("location_id", "year_id"), variable.name = "draw", value.name = "tfr")
          
          df <- df[, .(gpr_mean = mean(tfr), lower = quantile(tfr, .025), upper = quantile(tfr, .975)), by = .(location_id, year_id)]
          
      }

      
      return(df)
      
  } 

}


