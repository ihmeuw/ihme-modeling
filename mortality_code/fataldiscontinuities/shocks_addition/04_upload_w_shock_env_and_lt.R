# Author :NAME 
# Date   : 4/26/17
# Purpose: Upload the final, with-shock compiled summary life tables and 
#           compiled summary envelope to the mortality database.

rm(list=ls())

install.packages("slackr") # the next line is to select the download mirror
# 129
if (Sys.info()[1]=="Windows") root <- "" else root <- ""
library(data.table)
library(plyr)
library(haven)
shared_dir <- paste0(root,"PATH") # Where the shared functions live
source(paste0(shared_dir ,"PATH"))
source(paste0(shared_dir ,"PATH"))
source(paste0(shared_dir ,"PATH"))
source(paste0(shared_dir ,"PATH"))
source(paste0(shared_dir ,"PATH"))

################## format for upload ###########################################

# load compiled summary end and lt
lt  = fread(paste0(root, "PATH"))
env = fread(paste0(root, "PATH"))

# check for missing data
if(any(is.na(lt))  == TRUE) stop("there are NA values in your lt")
if(any(is.na(env)) == TRUE) stop("there are NA values in your env")

# drop unneeded columns
lt_cols_to_drop = c("n", "px", "dx", "Tx")
both_drop       = c("ihme_loc_id")

lt  <- lt[, c(lt_cols_to_drop, both_drop) := NULL]
env <- env[, (both_drop) := NULL]

#rename variables
lt          <- rename(lt, c("year"="year_id"))
names(env)  <- gsub("env_", "", names(env), fixed = TRUE)

# reshape long (melt) the lt and assign numbers to lt values as in lookup table
lt <- melt(lt, value.name="mean", 
                id.vars=c("sex_id", "age_group_id", "year_id", "location_id"),
                measure.vars=c("mx", "ax", "qx", "lx", "nLx", "ex"),
                variable.name="life_table_parameter_id")

lpt_ids_text    <- lt$life_table_parameter_id
lpt_ids_numeric <- mapvalues(lpt_ids_text, from=c("mx", "ax", "qx", "lx", "ex", "nLx"),
                                             to=c( 1  ,  2  ,  3  ,  4  ,  5  ,  7   ))
lt <- lt[, c("life_table_parameter_id") := lpt_ids_numeric]

# ----- add required columns ------
# add estimate stage_id, set to "final w/shock" (6)
lt[,'estimate_stage_id'] = 6
env[,'estimate_stage_id'] = 6

lt[, "upper"] = ""
lt[, "lower"] = ""



#******************************************************************************#
#                               Upload                                         #
#******************************************************************************#

env_comment <- " envelope with shock" # this should be requested as input
lt_comment <- " life table with shock" # this should be requested as input
# in a future version, then passed as an arg. That should happen in 00_run_shocks.R



####################### compiled summary lifetable #############################

# get the current best lt version id
# lt_version <- get_proc_version(model_name="death number",
#                           model_type="estimate",
#                           run_id="best")
lt_version = 23
write.csv(lt, paste0(root,"PATH", lt_version, "_with_shock.csv"), row.names=F)

#upload the compiled summary life table
upload_results(paste0(root,"PATH", lt_version, "_with_shock.csv"),
               "life table", "estimate",
               run_id= (lt_version),
               enforce_new=F) #lt_version))
update_status(model_name="life table",
              model_type="estimate", 
              run_id=(lt_version),
              new_status="best",
              new_comment=("updated pred_ex with corrected ax"))



###################### compiled summary envelope ###############################

# create new run_id and save to db folder
# env_version     <- gen_new_version( "death number", "estimate", comment)
env_version     <- 179
write.csv(env, paste0(root,"PATH", env_version, "_with_shock.csv"), row.names=F)

# upload the compiled summary life table
upload_results(paste0(root,"PATH", env_version, "_with_shock.csv"),
               "death number",
               "estimate",
               run_id = (env_version),
               enforce_new=F)
update_status(model_name="death number",
              model_type="estimate",
              run_id=(env_version),
              new_status='best',
              new_comment=("Hotsauce with correct groups 1A and 1B U5"))



############################# testing ##########################################
test_env = get_mort_outputs(model_name="death number",
                            model_type="estimate",
                            run_id="best",
                            location_ids=c(1),
                            year_ids=c(1990))

test_lt = get_mort_outputs(model_name="life table",
                           model_type="estimate",
                           run_id="best",
                           location_ids=c(1),
                           year_ids=c(1990))

print(paste0("Envelope estimate stage names for run_id=", unique(test_env$run_id), ": ", unique(test_env$estimate_stage_name)))
print(paste0("Life table estimate stage names for run_id=", unique(test_lt$run_id), ": ", unique(test_lt$estimate_stage_name)))


# end
print("Done with upload of w/ shock lt and env.")

