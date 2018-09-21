################################
##author:USERNAME
##date: 6/1/2017
##purpose: create holdouts from a dataset
##notes:   based off of knockout.py function
##         
##         
##      
#
########################################################################################################################################################
#            ##### Knockout  Functions ######
#               prep_ko()
#               get_kos()
#       
################# prep_ko() ################################################################################################################################################
#
#       #### Purpose: Preps a dataset for the get_kos() function 
#       #### Notes: Requires the make_square() function in the central ubcov repo
#                   
#       ##### Arguments###############
#       #### REQUIRED ###
#         -data: a data table that contains the following columns:
#                       "data": The data of the modelable entity of interest
#                       "location_id": IHME location_id
#                       "year_id': IHME year_id
#
#       #### Optional ###
#
#         -remove_subnats: Logical. If T, then all subnational locations will be removed from the dataset and the square before returning
#             Defualt is F
#             This argument exists for STGPR users.  Linear models are being run without subnational data, so out of sample testing can be done w/o subnational data
#
#         -make_squre() optional arguments.. see that function's documentations
#               The only one that matters should be location_version_id.  The others, if not present in the data, won't be held out (because they won't be in the dataset you give)
#
#       ######### Output #############
#         -A list of four objects:
#             1: A data.table containing a square dataset- contains the original dataset as well as all missing locs-years-ages-sexes
#             2: A data.table of the original dataset where the "data" column has no missing values
#             3: A vector of all location_ids you are modelling (as defined by location_version_id)
#             4: A vector of all location_ids for which there is any data in the original 'data' data.table
# 
#         -These outputs are designed for direct input into the get_kos() function.

################# get_kos() ################################################################################################################################################
#
#       #### Purpose: Creates knockouts from a dataset prepped by the pre_ko() function
#       #### Notes: Currently holding out data by replicating patterns of missingness in the age-year matrix of one location in another
#                   
#       ##### Arguments###############
#       #### REQUIRED ###
#         -square_data: a data.table containing a square dataset- contains the original dataset as well as all missing locs-years-ages-sexes
#
#         -no_miss_data: A data.table of the original dataset where the "data" column has no missing values
#
#         -all_locs: A vector of all location_ids you are modelling (as defined by location_version_id)
#
#         -data_locs: A vector of all location_ids for which there is any data in the original 'data' data.table
#
#       #### Optional ###
#         -kos: Integer, number of knockouts to perform.  DUSERt is 5
#
#         -prop_to_hold: Numeric, proportion of the total dataset to hold out for each knockout. DUSERt is 0.30
#               Must be between 0 and 1
#
#         -seed: Numeric, seed to set for holding out data
#         
#
#       ######### Output #############
#         -A single data.table that is the same as the original data as supplied to prep_ko(), but with the added columns "ko*", for ko in kos
#             Each ko* column contains a binary 0 or 1; if a row is marked as 1, then it has been designated to be held out for that knockout
# 
########################################################################################################################################################

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "FILEPATH"
}



library(data.table)
library(rhdf5)

central<-paste0(j, "FILEPATH")


source(paste0(j, "FILEPATH/db_tools.r"))
source(paste0(j, "FILEPATH/setup.r"))
source(paste0(central, "get_location_metadata.R"))


################### Prep data for KO #########################################
######################################################

prep_ko<-function(data, location_version_id=149, start_year=1980, end_year=2016, by_sex=1, by_age=1, remove_subnats=F){
  if(F){
    #ko_items<-prep_ko(data.s, remove_subnats=T)
    location_version_id=149
    start_year=1980
    end_year=2016
    by_sex=0
    by_age=1
    remove_subnats=T
  }
  if(remove_subnats==T){
    ##USERNAME: get level from location metadata if it isn't already there
    locs<-get_location_metadata(version_id=location_version_id)
    locs<-locs[, .(location_id, level)]
    
    if(!"level" %in% names(data)){
      data<-merge(data, locs, by="location_id")
    }
    
    data<-data[level<4  |  location_id %in% c(44533, 4749, 4640, 4639, 434, 84, 4636),]
  }
  
  ##USERNAME: if there are multiple sexes in the data, by_sex should be 0 for now
  if(length(unique(data$sex_id))>1){
    if("temp_sex" %in% names(data)){stop(" 'temp_sex' is the name of a column in your data, this will cause problems")}
    data[, temp_sex:=sex_id]
    data[, sex_id:=3]
  }
  
  
  ##USERNAME:get square, the function in db_tools script.  The start and end year args don't really matter- if they aren't in the data, they won't be held out.
  ##USERNAME: if 1 and 2 are in sex_id, by_sex needs to be 0.  If unique sex_id is 3, then by sex should be 0
  sqr<-make_square(location_version_id, start_year, end_year, covariates=NA, by_age=by_age, by_sex=by_sex)
  
  ##USERNAME:drop subnats from the square
  if(remove_subnats==T){
    sqr<-merge(sqr, locs, by="location_id")
    sqr<-sqr[level<4  |  location_id %in% c(44533, 4749, 4640, 4639, 434, 84, 4636),]
    sqr[, level:=NULL]
  }
  
  ##no missing data
  no_na_data<-data[!is.na(data),] #.(location_id, year_id, age_group_id, data)]
  
  # if(by_sex==1){
  #   data.sqr<-merge(data, sqr, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.y=T, all.x=T)
  # }else{
    sqr[, sex_id:=NULL]
    data.sqr<-merge(data, sqr, by=c("location_id", "year_id", "age_group_id"), all.y=T, all.x=T)
  # }

  
  ##whole square
  data.sqr<-data.sqr[, .(location_id, year_id, age_group_id, data)]
  
  ##USERNAME: get all locations
  locations1<-unique(data.sqr$location_id)
  
  ##USERNAME: get locations w/ data
  locations2<-unique(no_na_data$location_id)
  
  
  list(data.sqr, no_na_data, locations1, locations2)
}


################### get_kos() #########################################
######################################################



get_kos<-function(square_data, no_miss_data, all_locs, data_locs, kos=5, prop_to_hold=.30, no_new_ages=T, only_data_locs=F, seed=32){
  if(F){  ##USERNAME: for interactive
    #ko_test<-get_kos(ko_items[[1]], ko_items[[2]], ko_items[[3]], ko_items[[4]])
    ko<-1
    all_locs<-ko_items[[3]]
    data_locs<-ko_items[[4]]
    square_data<-ko_items[[1]]
    no_miss_data<-ko_items[[2]]
    prop_to_hold=.250
    seed<-32595
    kos<-10
  }
  
  
  
  ##USERNAME: can overwrite all locations to include at least some data.. this avoids dropping 100% of the data in a location
  if(only_data_locs==T){
    all_locs<-data_locs
  }
  
  no_na_data<-copy(no_miss_data)
  no_na_data[, data_id:=seq_len(.N)]
  set.seed(seed)
  for(ko in 1:kos){
    ##USERNAME: ranomize locs
    all_locs<-sample(all_locs, length(all_locs))
    data_locs<-sample(data_locs, length(data_locs))
    
    ##USERNAME: create a loc1_loc2 var
    loc_link<-data.table(loc1=all_locs[1:length(data_locs)], loc2=data_locs)
    
    data_to_ko<-copy(no_na_data)
    #data_to_ko[, sex_id:=NULL]
    
    data_to_ko<-merge(loc_link, data_to_ko, by.x="loc2", by.y="location_id", all.y=T)
    missing_data<-square_data[is.na(data), .(location_id, age_group_id, year_id)]
    
    var<-"test1"
    missing_data[, (var):=T]  ##USERNAME: mark all age-years from these locs as possible to hold out
  
    ##USERNAME: bind together the missing data and the non-missing data. 
    #where test1==T, data was missing for that age-year in loc2 but not in loc1
    #where test1 is NA, data was present for both loc1 and loc2 for that age year
      data_to_ko<-merge(data_to_ko, missing_data, by.x=c("loc1", "age_group_id", "year_id"), by.y=c("location_id", "age_group_id", "year_id"), all.x=T)

    ##USERNAME: this while loop gets data_ids to drop until the specified proportion is hit
    loc_vect<-sample(unique(data_to_ko[test1==T, loc2]), size=length(unique(data_to_ko[test1==T, loc2])), replace=F)
    ids_to_drop<-c()
    i<-0
    while(length(unique(ids_to_drop))/nrow(no_na_data)<prop_to_hold){ i<-i+1;
    
    ##USERNAME: need to write this check in for diet data, replicated age-year patterns in FAO data
      if(i>length(loc_vect)){ 
        message(paste0("Tried to knockout more locs than available, randomly dropping the remaining proportion"))
        message(paste0("  This shouldn't happen unless you have a dataset w/ repeated age-year patterns across all locs!"))
        leftovers<-unique(no_na_data$data_id)[!unique(no_na_data$data_id) %in% ids_to_drop]
        
        prop_remaining<-prop_to_hold-length(unique(ids_to_drop))/nrow(no_na_data)
        randos<-data.table(ids=leftovers, drop=rbinom(length(leftovers), 1, prop_remaining))
        message(paste0("  Dropping ", sum(randos$drop), " random data points (", round(sum(randos$drop)/nrow(no_na_data)*100, digits=3), "% of the whole dataset)"))
        
        ids_to_drop<-c(ids_to_drop, randos[drop==1, ids])
        break
        }else{
          ##USERNAME: this is the normal KO process
          loc<-loc_vect[i]
          ids_to_drop<-c(ids_to_drop, data_to_ko[loc2==loc  & test1==T, data_id])
        }
    }
    ##USERNAME: create the ko column
    no_na_data[data_id %in% ids_to_drop, (paste0("ko", ko)):=1]
    no_na_data[is.na(get(paste0("ko", ko))), (paste0("ko",ko)):=0]
    message(" ", length(unique(ids_to_drop)), " (", round(sum(no_na_data[,get(paste0("ko", ko))])/nrow(no_na_data)*100, digits=3), "%) data points dropped for ko ", ko)

    
    ##USERNAME: need to check if an entire age group id gets dropped.. this is only a problem if factor(age_group_id) is in the model, it will break trying to predict new levels of a fixed effect
    if(no_new_ages==T){
      train_ages<-unique(no_na_data[get(paste0("ko",ko))==0, age_group_id])
      test_ages<-unique(no_na_data[get(paste0("ko",ko))==1, age_group_id])
      
      all_held_ages<-test_ages[!test_ages %in% train_ages]
      if(length(all_held_ages>1)){
        message(paste0("  Ko ", ko, " held out all data points for age group(s) ", paste0(all_held_ages, collapse=", "), ";"))
        message(paste0("   Adding these data points back to the training data to avoid trying to predict new levels"))
        
        ##USERNAME: put data points back into the training dataset
        no_na_data[age_group_id %in% all_held_ages, (paste0("ko",ko)):=0]
        message("   New amount of heldout data is ", nrow(no_na_data[get(paste0("ko", ko))==1,]), " (", round(sum(no_na_data[,get(paste0("ko", ko))])/nrow(no_na_data)*100, digits=3), "%) for ko ", ko)
        
        
      }
      
      
      
    }
    
  }
  
  if("temp_sex" %in% names(no_na_data)){
    no_na_data[, sex_id:=temp_sex]
    no_na_data[, temp_sex:=NULL]
  }
  return(no_na_data)
}




