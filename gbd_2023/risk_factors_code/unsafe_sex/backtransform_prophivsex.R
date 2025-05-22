## PR DATE: UNCOMMENT THIS TOP SECTION AND RUN IN COMMAND LINE (RECOMMENT BEFORE SUBMITTING JOBS THOUGH); 
## WHEN COMPLETE COMMENT OUT GET DRAWS PART AND UNCOMMENT SAVE_RESULTS_EPI PART AND RUN AGAIN 

##############################################
## STEP 1: SAVE TANSFORMED DRAWS QSUB
## GETTING OBJECTS FOR PASSING LOCATION AS ARGUMENT AND SUBMITTING JOBS:
# source("FILEPATH")
# 
# locs<-get_location_metadata(location_set_id = 35, gbd_round_id = 7)
# loc_list<-unique(locs$location_id[locs$level>=3])
## NOTE: I USUALLY SELECT LOCATIONS WHERE IS_ESTIMATE == 1 NOT BY LEVEL
# 
# project<-"PROJECT"
# 
## SKIP!! GO TO NEXT JOB SUBMIT LOOP 
# for(loc in  loc_list) {
#     command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q QUEUE -P ", project, " -e FILEPATH -N loc_", loc," FILEPATH -s FILEPATH " ,loc)
#     system(command)
# }
## RETAINED THE VERSION ABOVE IN CASE THERE'S INTEREST IN USING THE CENTRAL CODE REPO 
## INDICATED THERE INSTEAD BUT IF THERE IS A SCRIPT THERE IT HASN'T BEEN UPDATED FOR ROUND 7 AS THIS VERSION IS
# for(loc in  loc_list) {
#     command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q QUEUE -P ", project, " -e FILEPATH -N loc_", loc,"FILEPATH -s FILEPATH " ,loc)
#     system(command)
# }

# ##############################################
# ## STEP 1: ACTUAL CODE TO SOURCE
rm(list = ls())

source("FILEPATH")

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
l <- ifelse(!is.na(args[1]),args[1], 101) # take location_id
ages      <- c(2:20, 30:32, 235)
sexes     <- c(1,2)

df <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 24811, source = "epi",
            location_id = l, gbd_round_id = 7, year_id = 1990:2022,
            decomp_step = "iterative", age_group_id = ages, sex_id = sexes)
#
draw_cols<-paste0("draw_", 0:999)
df<-as.data.table(df)
df<-df[, (draw_cols):=lapply(.SD, function(x) {1-x}), .SDcols = draw_cols]

df<-df[, modelable_entity_id:=24812]

write.csv(df, paste0("FILEPATH"), na = "", row.names = F)


# ##############################################
# STEP 2: SAVE RESULTS EPI QSUB
# # project<-"proj_yld"
# # command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q QUEUE -P ", project, " -e FILEPATH -N backTransform_unsafeSex_PAF FILEPATH -s FILEPATH")
# # system(command)

## STEP 2: SAVE RESULTS EPI TO CUSTOM MODEL (MEID 24812)
# source("FILEPATH")
# save_results_epi(input_dir = "FILEPATH", input_file_pattern = "FILEPATH",
#                  modelable_entity_id = 24812, description = "Iterative Prop HIV due to Sex Back Transformed, preliminary save test PR DATE",
#                  year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022),
#                  sex_id = 1:2, measure_id = 18, gbd_round_id = 7, decomp_step = "iterative",
#                  bundle_id = 386,
#                  crosswalk_version_id = 19715, ## NOTE: REMEMBER TO UPDATE CROSSWALK_VERSION_ID IF A NEW VERSION HAS BEEN USED IN THE BEST MODEL
#                  mark_best = T)               

