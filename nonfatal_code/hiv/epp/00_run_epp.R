################################################################################
## Purpose: Launch all components of EPP model
## Run instructions: Adjust the toggles and change the run locations
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,FILEPATH,FILEPATH)
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, FILEPATH, FILEPATH), user, FILEPATH)

## Packages
library(data.table)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {

} else {
  prep.epp <- F
  epp <- T
  plot <- T
  art_new_dis <- F
  random_walk<- F
  region_anc <- F

  run.name <- paste0(substr(gsub("-","",Sys.Date()),3,8), "")

  cluster.project <- "proj_hiv"
  
  # Prep EPP arguments
  rank.draws <- F
  proj.end <- 2016

  # EPP arguments
  model.type <- "rspline"
  if(prep.epp) {
    prepped.dir <- run.name #"170110_EPP_round_2" #
  } else {
    prepped.dir <- "170323_ZAF_KEN_gbdpop"
  }
  n.draws <- 1000 # dUSERt 1000
}

### Paths

### Functions
source(paste0(code.dir, "shared_functions/get_locations.R"))

### Tables
loc.table <- get_locations()

### Code
## Run locations
loc.list <- c()
drop.locs <- loc.list[grep("IND", loc.list)]
loc.list <- setdiff(loc.list, drop.locs)

## Launch prep EPP
if (prep.epp) {
  for (loc in loc.list) {
    prep.epp.string <- paste0("qsub -P ", cluster.project, " -pe multi_slot 8 ",
                       "-e FILEPATH ",
                       "-o FILEPATH ",
                       "-N ", loc, "_prep_epp ",
                       code.dir, "shell_R.sh ",
                       code.dir, "EPP2016/prep_epp_data.R ",
                       loc, " ", run.name, " ", rank.draws, " ", proj.end)
    print(prep.epp.string)
    system(prep.epp.string)
  }
}

######## List of locations with anc.prior.mean=0.15 & SD=1 
###### Locations with both ANC and Survey data
prev.path <- paste0(FILEPATH)
anc.path <- paste0(FILEPATH)
prev.data <- fread(prev.path)
anc.data <- fread(anc.path)
### DUSERt ANC
loc.list.dUSERt.anc <- loc.list[loc.list %in% intersect(unique(anc.data$ihme_loc_id), unique(prev.data$ihme_loc_id))]
loc.list.dUSERt.anc <- loc.list.dUSERt.anc[!loc.list.dUSERt.anc %in% c("CPV", "DOM", "NER", "SEN")]  ### quite different pattern under dUSERt anc bias mean&SD
### Region specific ANC
loc.list.region.anc <- setdiff(loc.list, loc.list.dUSERt.anc)
loc.list.region.anc <- c(loc.list.region.anc,c("CPV", "DOM", "NER", "SEN") ) ### move those to region anc
######## List of location with random walk project 
loc.list.RW.survey <- c("BDI","CAF", "ETH", "TZA",
                        "HTI_44804", "HTI_44805", "HTI_44810", "HTI_44808", "ZWE_44844", "NER")
### Equilibrium Prior locations
loc.list.EP <- setdiff(loc.list, loc.list.RW.survey)
######## 4 combination
loc.list.EP.dUSERt.anc <- intersect(loc.list.dUSERt.anc, loc.list.EP) ## epp.R | random_walk=F & region_anc=F
loc.list.RW.dUSERt.anc <- intersect(loc.list.dUSERt.anc, loc.list.RW.survey)  ## epp_.R | random_walk=T & region_anc=F
loc.list.EP.region.anc <- intersect(loc.list.region.anc, loc.list.EP)  ## epp.R | random_walk=F & region_anc=T
loc.list.RW.region.anc <- intersect(loc.list.region.anc, loc.list.RW.survey)

## Launch EPP and draw compilation
if(epp) {
  for(loc in loc.list) {

    # EPP
    epp.string <- paste0("qsub -P ", cluster.project, " -pe multi_slot 1 ", 
                       "-e FILEPATH ",
                       "-o FILEPATH ",                    
                       "-N ", loc, "_epp ",
                       "-hold_jid ", loc, "_prep_epp ",
                       "-t 1:", n.draws, " ",
                       code.dir, "shell_R.sh ",
                       code.dir, "EPP2016/epp.R ",
                       loc, " ", run.name, " ", prepped.dir, " ", model.type, " ", art_new_dis, " ", random_walk, " ", region_anc)
    print(epp.string)
    system(epp.string)

    if(n.draws == 1000) {
      # Draw compilation
      draw.string <- paste0("qsub -P ", cluster.project, " -pe multi_slot 2 ", 
                     "-e FILEPATH ",
                     "-o FILEPATH ",                      
                     "-N ", loc, "_save_draws ",
                     "-hold_jid ", loc, "_epp ",
                     code.dir, "shell_R.sh ",
                     code.dir, "EPP2016/save_paired_draws.R ",
                     loc, " ", run.name, " ", n.draws)
      print(draw.string)
      system(draw.string)
    }
  }
}

## Plot and combine results
if(plot) {
  for(loc in loc.list) {
    # Plot individual locations
    plot.string <- paste0("qsub -P ", cluster.project, " -pe multi_slot 1 ", 
                       "-e FILEPATH ",
                       "-o FILEPATH ",                      
                       "-N ", loc, "_plot_epp ",
                       "-hold_jid ", loc, "_save_draws ",
                       code.dir, "shell_R.sh ",
                       code.dir, "EPP2016/plot_epp.R ",
                       loc, " ", run.name)
    print(plot.string)
    system(plot.string)
  }

  # Compile all locations
  combine.holds <- paste(paste0(loc.list, "_plot_epp"), collapse = ",")
  combine.string <- paste0("qsub -P ", cluster.project, " -pe multi_slot 1 ", 
                     "-e FILEPATH ",
                     "-o FILEPATH ",                      
                     "-N combine_epp_plots ",
                     "-hold_jid ", combine.holds, " ",
                     code.dir, "shell_R.sh ",
                     code.dir, "EPP2016/combine_epp_plots.R ",
                     run.name)
  print(combine.string)
  system(combine.string)
}

### End