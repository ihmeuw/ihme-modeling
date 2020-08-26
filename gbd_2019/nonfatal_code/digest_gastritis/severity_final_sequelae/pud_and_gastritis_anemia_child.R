# 28 Feb 2018
# Child for anemia split epi model

#source("FILEPATH/pud_and_gastritis_anemia_child.R")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
}

##load in arguments from parent script (must start index at 3 because of shell)
cause<-commandArgs()[3]
child<-commandArgs()[4]
split_file<-commandArgs()[5]

age_groups<-c(2:20, 30:32, 235)
##EN, LN, PN, 1-4yo and 5-year groups through 94 and then 95+

measure <- 5
##prevalence

step <- "step4"

##Source ME_ids
pud_asymp <- 9314
pud_mild <- 20401
pud_mod <- 20403
pud_adj_acute <- 20399
pud_adj_complic <- 20400

gastritis_asymp <- 9528
gastritis_mild <- 20404
gastritis_mod <- 20406
gastritis_adj_acute <- 20408
gastritis_adj_complic <- 20409

##Proportion ME_ids
prop_pud_no_anemia <- 18852
prop_pud_mild_anemia <- 18849
prop_pud_mod_anemia <- 18850
prop_pud_sev_anemia <- 18851

prop_gastritis_no_anemia <- 18848
prop_gastritis_mild_anemia <- 18845
prop_gastritis_mod_anemia <- 18846
prop_gastritis_sev_anemia <- 18847	

##Target ME_ids
asymp_pud_no_anemia <- 16219
asymp_pud_mild_anemia <- 16216
asymp_pud_mod_anemia <- 16217
asymp_pud_sev_anemia <- 16218

mild_pud_no_anemia <- 16214
mild_pud_mild_anemia <- 16205
mild_pud_mod_anemia <- 16208
mild_pud_sev_anemia <- 16211

mod_pud_no_anemia <- 16215
mod_pud_mild_anemia <- 16206
mod_pud_mod_anemia <- 16209
mod_pud_sev_anemia <- 16212

adj_acute_pud_no_anemia <- 20293
adj_acute_pud_mild_anemia <- 20294
adj_acute_pud_mod_anemia <- 20295
adj_acute_pud_sev_anemia <- 20296

adj_complic_pud_no_anemia <- 19841
adj_complic_pud_mild_anemia <- 19842
adj_complic_pud_mod_anemia <- 19843
adj_complic_pud_sev_anemia <- 19844

asymp_gastritis_no_anemia <- 16235
asymp_gastritis_mild_anemia <- 16232
asymp_gastritis_mod_anemia <- 16233
asymp_gastritis_sev_anemia <- 16234

mild_gastritis_no_anemia <- 16230
mild_gastritis_mild_anemia <- 16221
mild_gastritis_mod_anemia <- 16224
mild_gastritis_sev_anemia <- 16227

mod_gastritis_no_anemia <- 16231
mod_gastritis_mild_anemia <- 16222
mod_gastritis_mod_anemia <- 16225
mod_gastritis_sev_anemia <- 16228

adj_acute_gastritis_no_anemia <- 20309
adj_acute_gastritis_mild_anemia <- 20310
adj_acute_gastritis_mod_anemia <- 20311
adj_acute_gastritis_sev_anemia <- 20312

adj_complic_gastritis_no_anemia <- 19888
adj_complic_gastritis_mild_anemia <- 19889
adj_complic_gastritis_mod_anemia <- 19890
adj_complic_gastritis_sev_anemia <- 19891

source("FILEPATH/split_epi_model.R")

# apply anemia proportions to asymp, mild, mod, adj_acute, adj_complic, using split epi model central function
target_ids <- c(get(paste0(child, "_", cause, "_no_anemia")), get(paste0(child, "_", cause, "_mild_anemia")), get(paste0(child, "_", cause, "_mod_anemia")), get(paste0(child, "_", cause, "_sev_anemia")))
prop_ids <- c(get(paste0("prop_", cause, "_no_anemia")), get(paste0("prop_", cause, "_mild_anemia")), get(paste0("prop_", cause, "_mod_anemia")), get(paste0("prop_", cause, "_sev_anemia")))
source_id <- get(paste0(cause, "_", child))  

split_epi_model(source_meid=source_id, target_meids=target_ids, prop_meids=prop_ids, split_measure_ids=5, output_dir=file.path(split_file), gbd_round_id=6, decomp_step=step)
