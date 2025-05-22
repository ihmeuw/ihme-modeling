# PAF calculator
# In GBD2023 they updated the PAF calculator so below is the new code to run the PAF calculator

rm(list=ls())

# Libraries ##########################
library("ihme.cc.paf.calculator", lib.loc = "FILEPATH") #PAF calculator
source("FILEPATH/get_best_model_versions.R")

# variables ######################
proj<-"proj_erf" #cluster project
release<-16 #GBD2023 release id
draws<-250 #number of draws
years<-c(1990:2024)
descrip<-"running annual yrs 1990-2024, try 2"

#rei ids
# wash rei_ids
wash <- c(#83 # wash_water
          84 # wash_sanitation
          #238 # wash_hygiene
)

# lead rei_ids
lead <- c(  242, # envir_lead_blood # dependent on COMO
            243 # envir_lead_bone
)

# occ rei_ids
occ <- c( #128 # occ_asthmagens
  # 129 # occ_particulates
  # 130 # occ_hearing # dependent on COMO
   132 # occ_ergo/backpain
  # 150 # occ_carcino_asbestos # dependent on CoDCorrect
  # 151, # occ_carcino_arsenic
  # 152, # occ_carcino_benzene
  # 153, # occ_carcino_beryllium
  # 154, # occ_carcino_cadmium
  # 155, # occ_carcino_chromium
  # 156, # occ_carcino_diesel
  # 158, # occ_carcino_formaldehyde
  # 159, # occ_carcino_nickel
  # 160, # occ_carcino_pah
  # 161, # occ_carcino_silica
  # 162, # occ_carcino_acid
  # 237  # occ_carcino_trichloroethylene
)

rei_ids <- c(#wash
  lead
 # occ
)

# Input files #######################
#Double check that we have all of inputs that will be going into the PAF calculator
inputs<-get_best_model_versions(entity = "rei", ids = rei_ids, release_id = release, source = "all")

inputs[, .(modelable_entity_id, modelable_entity_name, draw_type, model_version_id, best_start)]

# Run PAF calc #############################
paf_version<-ihme.cc.paf.calculator::launch_paf_calculator(
  rei_id = rei_ids,
  cluster_proj = proj,
  release_id = release,
  n_draws=draws,
  year_id = years,
  description = descrip
)













