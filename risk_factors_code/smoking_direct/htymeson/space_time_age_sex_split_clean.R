##################################################################################################################################
#################### Space-Time Age-Sex Split ####################################################################################
##Purpose: Perform age-sex splitting using a space-time method to match training data (sources that reported results
#         with IHME's five-year age groups and separated by sex) with split data (sources in need of age-sex splitting)
#         
##Inputs: Input dataframe (df) with columns specifying location_id, age_start and age_end (ex. age_group_id 7: age_start = 10,
#         age_end = 14), year_id, sex_id, estimate and sample size for each data source. 
#       
#         wspace = the input to calculate space weight, 
#         where space weight = (1 - (distance*wspace)), distance = {0 if same location_id, 1 if same region_id, 2 if same super_region_id}
#
#         wtime = input to calculate time weight, where time weight = 1 - abs(year_split - year_train)*wtime 
#
#         pspace = weight given to the spatial component in calculating total space-time weight, 
#                  wheere total weight = ((pspace*spacewt) + (ptime*timewt))
#         NOTE: ptime = 1 - pspace
#
#         nkeep = # of training sources to use to estimate each source to be split (keeps those with the highest space-time weights)
#         
##################################################################################################################################
##################################################################################################################################

require(data.table)
require(dplyr)
require(stringr)
library(magrittr)

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}

## Resources
source(paste0(jpath, "FILEPATH/get_population.R"))
source(paste0(jpath, "FILEPATH/get_location_metadata.R"))
source(paste0(jpath, "FILEPATH/get_ids.R"))

#Locations
locations <- get_location_metadata(gbd_round_id = 4, location_set_id = 22) 
locs <- copy(locations[, c("location_id", "ihme_loc_id", "region_id", "super_region_id", "location_name", "region_name", "level"), with=F])

#Populations
pops <- get_population(location_set_version_id = 149, year_id = c(1970:2016), sex_id = -1, location_id = -1, 
                       age_group_id = -1) %>% as.data.table
pops[, process_version_map_id:=NULL]

#Split age_group_ids to get age_start and age_end information
ages <- get_ids("age_group")

invisible(ages[ , age_start:=str_split_fixed(unique(ages$age_group_name), " to ", 2)[,1]])
invisible(ages[, age_end:=str_split_fixed(unique(ages$age_group_name), " to ", 2)[,2]])

smk_ages <- c(7:20, 30:32, 235)
ages <- ages[age_group_id %in% smk_ages] %>% invisible
ages[age_group_id == 235, age_start:=95] %>% invisible
ages[age_group_id == 235, age_end := 199] %>% invisible
ages <- ages[, .(age_group_id, age_start, age_end)]

ages$age_start <- as.numeric(ages$age_start)
ages$age_end <- as.numeric(ages$age_end)
ages$age_group_id <- as.numeric(ages$age_group_id)
ages <- ages[, c("age_group_id", "age_start", "age_end"), with = F]

#Merge ages and populations so pops has an age_start and age_end column
setkeyv(pops, "age_group_id")
setkeyv(ages, "age_group_id")
pops <- merge(pops, ages)


st_age_sex_split <- function(df, gold_standard=NA, location_id, year_id, age_start, age_end, sex, estimate, sample_size, wspace = .33, wtime = .05, pspace = .5, nkeep = 1000){
  

  #Varlists
  location_ids <- c(location_id, "region_id", "super_region_id")
  age_sex <- c(age_start, age_end, sex)
  id_vars <- c(location_id, year_id)
  
  #Create split id 
  df[, split_id := 1:.N]
  
  ## Make sure age and sex are int
  cols <- c(age_start, age_end, sex)
  df[, (cols) := lapply(.SD, as.integer), .SDcols=cols]
  
  ## Save original values
  orig <- c(age_start, age_end, sex, estimate, sample_size)
  orig.cols <- paste0("orig.", orig)
  df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]
  
  ## Separate metadata from required variables
  cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size)
  meta.cols <- setdiff(names(df), cols)
  metadata <- df[, meta.cols, with=F]
  dt <- df[, c("split_id", cols), with=F]
  
  ## Round age groups to the nearest 5-y boundary
  dt[, age_start := age_start - age_start %%5]
  dt <- dt[age_start > 80, (age_start) := 80]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 80, age_end := 84]
  
  #Merge in region and super-region info
  dt <- merge(dt, locs, by=location_id)
  
  ## Split into training and split set
  if (is.na(gold_standard)) {
    training <- dt[((age_end - age_start) == 4) & ((get(sex)) %in% c(1,2))]
    split <- dt[((age_end - age_start) != 4) | ((get(sex)) == 3)]
  } else {
    training <- copy(gold_standard)
    split <- dt[((age_end - age_start) != 4) | (get(sex) == 3)]
  }
  
  ##########################
  ## Expand rows for splits
  ##########################
  
  split[, n.age := (age_end + 1 - age_start)/5]
  split[, n.sex := ifelse(get(sex)==3, 2, 1)]
  
  ## Expand for age 
  split[, age_start_floor := age_start]
  expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)
  split <- merge(expanded, split, by="split_id", all=T)
  split[, age.rep := 1:.N - 1, by=.(split_id)]
  split[, (age_start):= age_start + age.rep * 5 ]
  split[, (age_end) :=  age_start + 4 ]
  
  ## Expand for sex
  split[, sex_split_id := paste0(split_id, "_", age_start)]
  expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)
  split <- merge(expanded, split, by="sex_split_id", all=T)
  split <- split[get(sex)==3, (sex) := 1:.N, by=sex_split_id]
  
  ###########################
  #DROP LOWER AGES
  
  split <- split[!(age_start < 10)]
  
  ###########################
  
  #Create a *truly unique* identifier for all the expanded sources that need estimates
  split[, xid:=seq(.N)]
  id <- split$xid
  
  #Create an estimates table to fill with for-loop estimate values 
  estimatesx <- data.table("xid" = id)
  
  #Add in ptime logical corollary to pspace
  ptime <- 1 - pspace
  
  #create a table of weight proportion based on # of location-levels (6 max)
  location_weights <- data.table(loc_heirarchy = 1:6, locs6 = c(.7,  .25, .04, .01, NA, NA),  locs5 = c( .7,  .25, .04, .01, NA, NA),  
                                 locs4 = c(.7,  .25, .04, .01, NA, NA), locs3 = c(.7, .25, .05,  NA, NA, NA),
                                 locs2 = c(.7, .3, NA, NA, NA, NA), locs1 = c( 1, NA, NA, NA, NA, NA))
  
  
  for (source in unique(id)) {
    
    sex_source <- split[id == source, get(sex)]
    age1 <- split[id == source, age_start]
    age2 <- split[id == source, age_end]
    
    srid <- split[id == source, super_region_id]
    rid <- split[id == source, region_id]
    lid <- split[id == source, location_id]
    
    #create a "weights" data table to generate space-time weights from training set
    #only keep sources with sample size greater than thirty
    weights <- training[get(sex)==sex_source & age_start==age1 & age_end == age2, ]
    weights <- weights[sample_size>=30]
    
    #space weights
    weights[, spacewt:=0]
    weights[super_region_id == srid, spacewt:= 1 - (2*wspace)]
    weights[region_id == rid, spacewt:= 1- wspace]
    weights[location_id==lid, spacewt:=1]
    
    #time weights
    year_data <- split[id == source, get(year_id)] %>% invisible
    weights[, timewt:= 1 - ((abs(get(year_id) - year_data))*wtime)]
    weights[timewt<0, timewt:=0]
    
    #combining the two weights 
    #(and setting time weights to zero if space-weight == 0)
    #- to maintain that only countries with some geographical link are the only ones influencing the age-sex pattern
    weights[, totalwt:= ((pspace*spacewt) + (ptime*timewt))]
    
    #keep the n-closest 
    setorderv(weights, "totalwt", order= -1)
    
    if(nrow(weights[totalwt>0]) >= nkeep){
      weights <- weights[1:nkeep, ]
    } else{
      weights <- weights[totalwt > 0]
    }
    
    #allocate weight to scale to based on max spacewt
    setorderv(weights, "spacewt", order=-1)
    weights[, loc_heirarchy:=.GRP, by = "spacewt"]
    
    num_locs <- weights[, unique(loc_heirarchy)] %>% length
    
    
    for (n in 1:6){
      if(num_locs==n){
        weights <- merge(weights, location_weights[, c("loc_heirarchy", paste0("locs", n)), with=F],  by="loc_heirarchy")
      }
    }
    
    
    
    setnames(weights, paste0("locs", num_locs), "loc_wt") 
    
    #adjust weights matrix so that the max weight equals 1
    weights[, adjuster:=.N, by = "loc_heirarchy"]
    weights[, loc_wt:=loc_wt/adjuster]
    
    #calculate mean prev
    weights[, prop:= get(estimate)*loc_wt]
    estx <- sum(weights$prop, na.rm = T)
    
    estimatesx[id==source, est:=estx]
    
    
  }
  
  setkey(split, xid)
  setkey(estimatesx, xid)
  split <- merge(split, estimatesx)
  
  #Merge in populations
  
  #first, check to make sure there's no column named 'population' already
  if ("population" %in% names(split)) {
    split[, population:=NULL]
  }  
  
  split <- merge(split, pops[, c(age_sex, "location_id", "year_id", "population"), with=F], by.x = c(age_start, age_end, year_id, location_id, sex),
                 by.y = c("age_start", "age_end", "year_id", location_id, "sex_id"))
  
  #find ratio between estimate and original (unsplit) prevalence
  
  #lemma - offset any estimates ==0 to prevent r_group being zero and breaking the system
  split[est==0, est:=.001]
  
  #weight estimate by population and grouped population
  split[, R:=est*population]
  split[, R_group:=sum(R), by="split_id"]
  
  #calculate final estimates and split sample size based on population
  split[, pop_group:=sum(population), by = "split_id"]
  split[, (estimate):=(get(estimate)*(R/R_group)*(pop_group/population))] 
  split[, (sample_size):=(sample_size*(population/pop_group))] 
  
  ## Mark as split
  split[, cv_split := 1] 
  
  #############################################
  ## Append training, merge back metadata, clean
  #############################################
  
  ## Append training, mark cv_split
  out <- rbind(split, training, fill=T) 
  out <- out[is.na(cv_split), cv_split := 0]
  
  ## Append on metadata
  out <- merge(out, metadata, by="split_id", all.x=T)
  
  ## Clean
  out <- out[, c(meta.cols, cols, "cv_split", "n.sex", "n.age"), with=F]
  out[, c("xid", "split_id") := NULL]
  return(out)
}   

