############################################################################################################
## Purpose: Squeeze final cadre models to hrh_any envelope and then apply employment envelope to generate final estimates
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table,magrittr,parallel,ggplot2)

## settings
output.version <- 1
use_draws <- T ## T or F for whether using runs with draws
by_sex <- F
clean <- T

## in/out
in.dir <- file.path(j,"FILEPATH")
out.dir <- file.path(j,"FILEPATH")
summary.dir <- file.path(j,"FILEPATH")
run.db <- fread("FILEPATH")
model.dir <- "FILEPATH"

model_root <- "FILEPATH"
setwd(model_root)
source("init.r")
source(file.path(j,"FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
locs <- locations[,list(location_id,ihme_loc_id)]

## determine suffix of files of interest based on sex disaggregation and whether or not data was cleaned
if (by_sex & clean){
  suffix <- paste0("_sex_",output.version,"_clean.csv")
} else if (by_sex & !clean) {
  suffix <- paste0("_sex_",output.version,".csv")
} else if (!by_sex & clean) {
  suffix <- paste0("_",output.version,"_clean.csv")
} else {
  suffix <- paste0("_",output.version,".csv")
}

## create list of all cadres
files <- list.files(file.path(in.dir,"hrh_final"))
files <- files[grepl(suffix,files) & !grepl(paste0("_sex",suffix),files)]
categs <- gsub(suffix,"",files) ## all categories
cadres <- categs[!grepl("hrh_any",categs)] ## just the cadres (no hrh_any)

## find most recent run_id for each cadre (and assume it was successful and is the current best model)
## all have to either be run with draws or without them (change settings at top accordingly)
mes <- data.table(me_name = categs, run_id = lapply(categs, function(x) {run.db[me_name == x,max(run_id)]}) %>% unlist)

## conversion factors for proportion employed to proportion of total pop
employment <- fread(file.path(in.dir,"hrh/prop_emp_oftotal.csv"))

if (use_draws) {

  id.vars <- c("location_id","year_id","age_group_id","sex_id")
  draw.cols <- paste0("draw_",seq(0,999))

  ## read in final hrh_any envelope
  total <- rbindlist(lapply(list.files(file.path("FILEPATH",mes[me_name == "hrh_any",run_id],"draws_temp_1"),full.names = T),fread))
  total <- melt(total,id.vars = id.vars,value.name = "hrh_any",variable.name = "draw")
  total[,hrh_any := 10000*hrh_any]

  ## merge on cadre-specific estimates
  for (cadre in cadres) {
    print(paste0("MERGING ON ",cadre))
    x <- rbindlist(lapply(list.files(file.path("FILEPATH",mes[me_name == cadre,run_id],"draws_temp_1"),full.names = T),fread))
    x <- melt(x,id.vars = id.vars,value.name = cadre,variable.name = "draw")
    total <- merge(total,x,by=c(id.vars,"draw"))
  }

  ## generate sum of cadre-specific estimates and squeeze proportions to envelope
  total[,cadre_total := rowSums(.SD), .SDcols = cadres]
  total[, (cadres) := lapply(.SD,function(x) {x*hrh_any/cadre_total}),.SDcols = cadres]

  ## merge on ihme_loc_id
  total <- merge(total,locs,by="location_id")
  id.vars2 <- c(id.vars,"ihme_loc_id")

  ## write summary of results pre-application of employment ratios
  all_cat <- data.table()
  for (categ in c(categs,"agg_nurses")) {
    print(paste0("COLLAPSING ",categ, ", PRE-EMP"))
    if (categ == "agg_nurses") {
      all_nurses <- c("hrh_nurseprof","hrh_nurseass","hrh_midass")
      pre_emp <- total[,c(id.vars2,"draw",all_nurses),with=F]
      pre_emp[,agg_nurses := rowSums(.SD),.SDcols = all_nurses]
    } else {
      pre_emp <- total[,c(id.vars2,"draw",categ),with=F]
    }
    pre_emp[,c("lower","mean","upper") := list(quantile(get(categ),0.025),mean(get(categ)),quantile(get(categ),0.975)),by=c(id.vars2)]
    pre_emp <- pre_emp[,c(id.vars2,c("lower","mean","upper")),with=F] %>% unique()
    pre_emp[,me_name := categ]
    all_cat <- rbind(all_cat,pre_emp)
  }
  write.csv(all_cat,file.path(summary.dir,"all_categs_pre_emp.csv"),row.names=F)

  ## apply employment ratios
  employment <- melt(employment, id.vars = id.vars[1:2],value.name = "emp",variable.name = "draw")
  total <- merge(total,employment,by=c(id.vars[1:2],"draw"))
  total[, (categs) := lapply(.SD,function(x) {x*emp}),.SDcols = categs]

  ## write summary of results post-application of employment ratios
  all_cat <- data.table()
  for (categ in c(categs,"agg_nurses")) {
    print(paste0("COLLAPSING ",categ, ", POST-EMP"))
    if (categ == "agg_nurses") {
      all_nurses <- c("hrh_nurseprof","hrh_nurseass","hrh_midass")
      post_emp <- total[,c(id.vars2,"draw",all_nurses),with=F]
      post_emp[,agg_nurses := rowSums(.SD),.SDcols = all_nurses]
    } else {
      post_emp <- total[,c(id.vars2,"draw",categ),with=F]
    }
    post_emp[,c("lower","mean","upper") := list(quantile(get(categ),0.025),mean(get(categ)),quantile(get(categ),0.975)),by=c(id.vars2)]
    post_emp <- post_emp[,c(id.vars2,c("lower","mean","upper")),with=F] %>% unique()
    post_emp[,me_name := categ]
    all_cat <- rbind(all_cat,post_emp)
  }
  write.csv(all_cat,file.path(summary.dir,"all_categs.csv"),row.names=F)

  ## write out draws of final proportions of total population
  out.dir <- file.path(h,"hrh_draws")
  for (categ in categs){
    print(paste0("WRITING FINAL ",categ, " DRAWS TO FILE"))
    x <- total[,c(id.vars2,"draw",categ),with=F]
    x <- dcast(x, location_id + ihme_loc_id + year_id + age_group_id + sex_id ~ draw, value.var = categ)
    x[,me_name := categ]
    write.csv(x,file.path(out.dir,paste0(categ,suffix)),row.names=F)
  }

} else {

  id.vars <- c("ihme_loc_id","location_id","year_id","age_group_id","sex_id")
  og_cols <- c("gpr_mean","gpr_lower","gpr_upper")
  new_cols <- c("_mean","_lower","_upper")
  any_cols <- paste0("hrh_any",new_cols)

  draws <- paste0("draw_",seq(0,999))
  employment[,emp_prop := rowMeans(employment[,draws,with=F])]
  emp_conv <- employment[,list(location_id,year_id,emp_prop)]

  ## read in final hrh_any envelope results, merge on ihme_loc_id, and rename cols
  total <- model_load(mes[me_name == "hrh_any",run_id],'raked')
  total <- merge(total,locs,by="location_id")
  setnames(total,og_cols,any_cols)

  ## convert hrh_any to proportion of 10,000 to match cadre-specific
  total[, (any_cols) := lapply(.SD,function(x) {x*10000}),.SDcols = any_cols]

  ## merge on cadre-specific estimates
  for (cadre in cadres){
    x <- model_load(mes[me_name == cadre,run_id],'raked')[,list(location_id,year_id,gpr_mean,gpr_lower,gpr_upper)]
    setnames(x,og_cols,paste0(cadre,new_cols))
    total <- merge(total,x,by=c("location_id","year_id"))
  }

  ## generate sum of cadre-specific estimates
  total[,cadre_total := rowSums(.SD), .SDcols = paste0(cadres,"_mean")]

  cols <- paste0(rep(cadres,each=3),new_cols)
  total[, (cols) := lapply(.SD,function(x) {x*hrh_any_mean/cadre_total}),.SDcols = cols]

  ## write outputs pre-application of employment ratios
  for (categ in categs){
    x <- total[,c(id.vars,paste0(categ,new_cols)),with=F]
    setnames(x,paste0(categ,new_cols),gsub("_","",new_cols))
    x[,me_name := categ]
    write.csv(x,file.path(out.dir,paste0(categ,"_prop_ofemp",suffix)),row.names=F)
  }

  ## apply employment ratios (also being treated as a constant here)
  total <- merge(total,emp_conv,by=c("location_id","year_id"))
  cols <- paste0(rep(categs,each=3),new_cols)
  total[, (cols) := lapply(.SD,function(x) {x*emp_prop}),.SDcols = cols]

  ## write out final proportions of total population
  for (categ in categs){
    x <- total[,c(id.vars,paste0(categ,new_cols)),with=F]
    setnames(x,paste0(categ,new_cols),gsub("_","",new_cols))
    x[,me_name := categ]
    write.csv(x,file.path(out.dir,paste0(categ,suffix)),row.names=F)
  }
}
