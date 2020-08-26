#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation 
# Purpose: Functions to generate processed data from raw data bundles
# TODO: 
# A) for age_sex_split_lit
#  - add checks to verfiy that all data in the bundle that is marked 
#    with the split_indicator has a group_review value of 0 
#  - add checks for common epi uploader issues like mean or SE > 1
#--------------------------------------------------------------

# setup -------------------------------------------------------------------

require(data.table)
require(msm)
require(logitnorm)

share_path <- "FILEPATH"
code_general <- 'FILEPATH'

# source MRBRT functions
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(code_general, "/function_lib.R"))

# source custom func lib 
# source(paste0(h_root,"code_general/_lib/function_lib.R"))
source_shared_functions(c("get_population"))
source(paste0("FILEPATH/add_population_denominator.R"))

#   -----------------------------------------------------------------------


# functions ---------------------------------------------------------------

# Age-sex split data from sources reporting age-specific and sex-specific 
# separately 
age_sex_split_lit<-function(dt,split_indicator,return_full=T){
  #' @param dt a data.table containing either all data from the bundle or just data
  #'        needing to be age-sex split 
  #' @param split_indicator the name of a binary/logical column in the data indicating
  #'        whether or not the row(s) should be age-sex split 
  #' @param return_full T/F should the full dataset including new age-sex split rows be 
  #'        returned? Default = T     
  
  # Subset to data needing to be age-sex split
  dt_split<-copy(dt[get(split_indicator)==1])
  
  # Check that all rows have specificity that is either age or sex 
  if (any(!unique(dt_split[,specificity])%in%c("age","sex"))){
    stop(paste0("There are rows where",split_indicator,"is TRUE but specificity is not age or sex."))
  }
  
  # For each source, check that there are rows for age and sex
  invisible(
    lapply(unique(dt_split[,nid]),function(x){
    check_age_sex(x,dt_split)
      })
  )
  
  # Create datatable with just sex specific data
  dt_sex<-dt_split[specificity=="sex"]
  
  # Check that there are not rows marked as both sex
  if(any(unique(dt_sex[,sex])%in%c("Both"))){
    stop("There are rows in this bundle that are marked as sex = Both and specificty = sex")
  }
  
  # Calculate proportion of cases male and female
  dt_sex[, cases_total:= sum(cases), by = list(nid, group, specificity)]
  dt_sex[, prop_cases := cases / cases_total]
  # Mark which nids have 0 total caes
  zero_cases<-dt_sex[cases_total==0,unique(nid)]
  
  # Calculate proportion of sample male and female
  dt_sex[, ss_total:= sum(sample_size), by = list(nid, group, specificity)]
  dt_sex[, prop_ss := sample_size / ss_total]
  
  # Calculate standard error of % cases & sample_size M and F
  dt_sex[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  dt_sex[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  # Estimate the ratio and standard error of the ratio of %cases/%sample 
  dt_sex[, ratio := prop_cases / prop_ss]
  dt_sex[, se_ratio:= prop_se_div(prop_cases,se_cases,prop_ss,se_ss)]
  
  # Subset to necessary cols
  if ("seq" %in% names(dt_sex)){
    dt_sex <- dt_sex[, .(nid, seq, group, sex, ratio, se_ratio, prop_cases, prop_ss)]
  }else{
    dt_sex <- dt_sex[, .(nid, group, sex, ratio, se_ratio, prop_cases, prop_ss)]
  }
  
  # Rename seq of sex-specific rows
  if("seq" %in% names(dt_sex)) setnames(dt_sex,"seq","parent_seq_sex")
  
  # Prep age-specific rows for split
  dt_age<-copy(dt_split[specificity=="age"])
  
  # Rename seq of age-specific rows 
  if("seq" %in% names(dt_age)) setnames(dt_age,"seq","parent_seq_age")
  
  # Duplicate age dt for both sexes
  dt_age_m<-copy(dt_age[,sex:="Male"])
  dt_age_f<-copy(dt_age[,sex:="Female"])
  dt_age<-rbindlist(list(dt_age_m,dt_age_f),use.names=T)
  
  # Merge on sex ratios
  dt_age<-merge(dt_age,dt_sex,by=c("nid","group","sex"))
  
  # Calculate age-sex specific mean, standard_error, cases, sample_size
  dt_age[, mean := mean * ratio]
  dt_age[, standard_error := prop_se_mult(mean,standard_error,ratio,se_ratio) ]
  dt_age[, cases := round(cases * prop_cases, digits = 0)]
  dt_age[, sample_size := round(sample_size * prop_ss, digits = 0)]
  if (!("note_sr")%in%names(dt_age)) dt_age[,note_sr:=NA]
  dt_age[, note_sr := ifelse(is.na(note_sr),paste("age,sex split using sex ratio", round(ratio, digits = 2)),
                             paste(note_sr, "| age,sex split using sex ratio", round(ratio, digits = 2)))]
  dt_age[, c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  
  # Adjust group, group_review, specficity, and seq for new rows
  dt_age[,group_review:=1]
  dt_age[,specificity:="age,sex"]
  if ("seq" %in% names(dt_age)){
    dt_age[,seq:=""]
    dt_age[,seq_parent:=do.call(paste,c(.SD, sep = ",")),.SDcols=c("parent_seq_age","parent_seq_sex")]
  } 
  
  # Make mean = 0 for nids with 0 total cases 
  dt_age[nid%in%zero_cases,`:=`(mean=0)]
  
  # Clear other uncertainty information
  dt_age[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA)]
  
  # Return df
  if (return_full==T){
    return_dt<-rbindlist(list(dt,dt_age),use.names=T,fill=T)
    return(return_dt)
  }else{
    return_dt<-rbindlist(list(dt_split,dt_age),use.names=T,fill=T)
    return(return_dt)
  }
}

apply_mrbrt_sex_split <- function(input_dt, acause, bid, bvid, ds, gbd_rnd, trim_pct=0.1){
  
  #' @param input_dt a data.table containing data in the epi databse dismod shape which 
  #' will be split into sex-specific data 
  #' @param acause the acause this input_dt belongs to
  #' @param bid the bundle id this input_dt belongs to
  #' @param bvid the bundle version id this input_dt belongs to
  #' @param ds decomp step to pull pop from
  #' @param gbd_rnd gbd round id
  #' @param trim_pct the amount of trimming that was used in the MR-BRT model to be applied -
  #' should be 0.1 but make flexible to allow for changes in the future

  
  # split into both-sex and sex-specific data sets
  message("separating data into sex-specific and both-sex datasets - ignoring any both-sex data that is marked with group_review = 0")
  tosplit_dt <- copy(input_dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review %in% c(0))]
  tosplit_dt <- fsetdiff(tosplit_dt,nosplit_dt)
  message(paste("preparing to split",nrow(tosplit_dt),"of", nrow(input_dt),"rows of data"))
  
  # pull in mrbrt model
  message("reading in MRBRT model and making predictions")
  model_path<-paste0("FILEPATH")
  mrbrt_mod<-readRDS(model_path)
  
  # make predictions for ratio of prev_female:prev_male
  preds <- predict_mr_brt(mrbrt_mod, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  draw_cols <- paste0("draw_",0:999)
  pred_draws <- melt(pred_draws, id.vars = names(pred_draws)[!names(pred_draws)%in%draw_cols],
                     variable.name = "draw_num")
  pred_draws[, value:=exp(value)]
  
  # calculate mean and sd 
  pred_draws_collpased<-pred_draws[,.(ratio_mean = mean(value),
                            ratio_se = sd(value))]
  message(paste0("predicted ratio: ",pred_draws_collpased[,ratio_mean]," (",pred_draws_collpased[,ratio_se],")"))

  # split into both sex, male, and female data sets to calculate population for each sex
  message("pulling male, female, and both sex pops for every data point that needs to be split - this may take a while..")
  both_sex_pop_dt<-copy(tosplit_dt[,.(seq,location_id,year_start,year_end,age_start,age_end,sex)])
  m_pop_dt<-copy(tosplit_dt[,.(seq,location_id,year_start,year_end,age_start,age_end,sex)])[,sex:="Male"]
  f_pop_dt<-copy(tosplit_dt[,.(seq,location_id,year_start,year_end,age_start,age_end,sex)])[,sex:="Female"]
  
  # pull pops for each of sex
  both_sex_pop_dt<-calc_pop_denom(data = both_sex_pop_dt, gbd_rnd = gbd_rnd, decomp_step = ds, pop_col_name = "Both_population")
  m_pop_dt<-calc_pop_denom(data = m_pop_dt, gbd_rnd = gbd_rnd, decomp_step = ds, pop_col_name = "Male_population")
  f_pop_dt<-calc_pop_denom(data = f_pop_dt, gbd_rnd = gbd_rnd, decomp_step = ds, pop_col_name = "Female_population")
  
  # merge together and tack onto df
  pops_by_sex<-merge(both_sex_pop_dt[,.(seq,Both_population)],m_pop_dt[,.(seq,Male_population)],by="seq")
  pops_by_sex<-merge(pops_by_sex,f_pop_dt[,.(seq,Female_population)],by="seq")
  tosplit_dt<-merge(tosplit_dt,pops_by_sex,by=c("seq"))
  
  # merge on mrbrt draws
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  
  # split out into male and female
  message("adjusting data")
  split_dt[, val_male := mean * (Both_population/(Male_population + (value * Female_population)))]
  split_dt[, val_female := value * val_male]
  
  # collapse across draws
  message("collapsing across draws")
  split_dt<-split_dt[,.(Male_mean=mean(val_male),
                        Male_standard_error=sd(val_male),
                        Female_mean=mean(val_female),
                        Female_standard_error=sd(val_female)),
                     by = c(names(split_dt)[!names(split_dt)%in%c("merge","sex","mean","sample_size","cases",
                                                                  "lower","upper","standard_error","effective_sample_size",
                                                                  "Both_population","X_intercept","Z_intercept","draw_num",
                                                                  "value","val_male","val_female")])]

  # calculate standard error for mean 0 points
  split_dt[Male_mean == 0 & measure %in% c("prevalence","proportion"), Male_standard_error := wilson_se(prop=Male_mean,n=Male_population)]
  split_dt[Male_mean == 0 & measure == "incidence", Male_standard_error := 
             ((5-Male_mean*Male_population)/Male_population+Male_mean*Male_population*sqrt(5/Male_population^2))/5]
  split_dt[Female_mean == 0 & measure %in% c("prevalence","proportion"), Female_standard_error := wilson_se(prop=Female_mean,n=Female_population)]
  split_dt[Female_mean == 0 & measure == "incidence", Female_standard_error := 
             ((5-Female_mean*Female_population)/Female_population+Female_mean*Female_population*sqrt(5/Female_population^2))/5]
  
  # drop male and female population vals
  split_dt[,c("Male_population","Female_population"):=NULL]
  
  # melt sex-specific cols long
  split_dt<-melt(split_dt,id.vars = names(split_dt)[!names(split_dt)%like%c("Male|Female")])
  
  # pull out sex (& hack around some regex stuff)
  split_dt[variable%like%"Male",sex:="Male"]
  split_dt[variable%like%"Female",sex:="Female"]
  split_dt[,variable:=gsub("Male_|Female_","",variable)]
  
  # cast wide by variable
  lhs<-paste0(names(split_dt)[!names(split_dt)%in%c("variable","value")],collapse = "+")
  split_dt_wide<-dcast(split_dt,as.formula(paste0(lhs,"~variable")))

  # clear other uncertainty information
  split_dt_wide[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA)]
  
  # change seq to crosswalk_parent_seq
  split_dt_wide[,crosswalk_parent_seq:=seq]
  split_dt_wide[,seq:=NULL]
  
  total_dt <- rbindlist(list(nosplit_dt, split_dt_wide), use.names=T, fill = T)
  return(total_dt)
}

apply_mrbrt_xwalk <- function(input_dt, bid, alt_def, cv_ref_map_path){
  
  #' @param input_dt a data.table containing data in the epi databse dismod shape which 
  #' will be crosswalked
  #' @param bid the bundle id for the data being crosswalked
  #' @param alt_def the alternative definition to crosswalk
  #' @param cv_ref_map_path filepath to .csv file mapping bundle ids to crosswalks

  # read in cv_ref_map
  cv_ref_map<-fread(cv_ref_map_path)
  cv_ref_map<-cv_ref_map[bundle_id==bid & alt == alt_def] 
  if (nrow(cv_ref_map)>1) stop("the bundle_id-alt_def combination supplied indentifies more than one crosswalk model in the cv_ref_map")
  alt_indicator_cols<-unlist(str_split(cv_ref_map[, unique(cv_alt_indicator_cols)],","))
  ac<-cv_ref_map[, unique(acause)]
  stg<-cv_ref_map[, unique(stage)]
  cv_type<-cv_ref_map[, unique(cv)]
  ref_def<-cv_ref_map[, unique(reference)]
  mod_type<-cv_ref_map[, unique(xwalk_type)]
  
  message(paste0("Applying crosswalk for ", stg,": crosswalking ", alt_def," points to ", ref_def))
  xwalk_lab<-paste0(stg, " - ", alt_def, " to ", ref_def,":")
  
  # split into both-sex and sex-specific data sets
  message(paste(xwalk_lab,"separating data into alt and reference datasets - ignoring any data that is marked with group_review = 0"))
  toxwalk_dt <- copy(input_dt)
  noxwalk_dt <- toxwalk_dt[eval(parse(text = paste0("!(",paste0(alt_indicator_cols," %in% c(1)",collapse = "&"), ") | (group_review %in% c(0))", collapse = "")))]
  toxwalk_dt <- fsetdiff(toxwalk_dt,noxwalk_dt)
  message(paste(xwalk_lab,"preparing to crosswalk",nrow(toxwalk_dt),"of", nrow(input_dt),"rows of data"))
  
  # pull in mrbrt model
  message(paste(xwalk_lab,"reading in MRBRT model and making predictions"))
  model_parent_dir<-paste0("FILEPATH")
  xwalk_mod_dir<-"FILEPATH"
  mrbrt_mod<-readRDS("FILEPATH")
  
  # make predictions for the logit difference of alt-ref 
  if (mod_type=="all_age"){
    preds <- predict_mr_brt(mrbrt_mod, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  }
  if (mod_type=="age_spec"){
    toxwalk_dt[,age_midpoint:=(age_start+age_end)/2]
    toxwalk_dt[,sex_id:= ifelse(sex=="Male",1,2)]
    toxwalk_dt[,age_sex:=age_midpoint*sex_id]
    preds <- predict_mr_brt(mrbrt_mod, newdata = unique(toxwalk_dt[,.(age_midpoint,sex_id,age_sex)]), write_draws = T)
  }
  pred_draws <- as.data.table(preds$model_draws)
  draw_cols <- paste0("draw_",0:999)
  pred_draws <- melt(pred_draws, id.vars = names(pred_draws)[!names(pred_draws)%in%draw_cols],
                     variable.name = "draw_num")
  
  # adjust data 
  message(paste(xwalk_lab,"logit transforming non-0 data points"))
  toxwalk_dt[mean!=0,logit_mean:=logit(mean)]
  
  message(paste(xwalk_lab,"applying the delta method to derive standard error of the logit-transformed means"))
  toxwalk_dt[mean!=0,logit_standard_error:=sapply(1:nrow(toxwalk_dt[mean!=0]), function(i){
    mean_i <- toxwalk_dt[i,get("mean")]
    mean_se_i <- toxwalk_dt[i,get("standard_error")]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]
  
  # merge on mrbrt draws
  if (mod_type=="all_age"){
    toxwalk_dt[, merge := 1]
    pred_draws[, merge := 1]
    xwalk_dt <- merge(toxwalk_dt, pred_draws, by = "merge", allow.cartesian = T)
    xwalk_dt[,merge:=NULL]
  }
  if (mod_type=="age_spec"){
    xwalk_dt <- merge(toxwalk_dt, pred_draws, by.x = c("age_midpoint","sex_id","age_sex"), 
                      by.y = c("X_age_midpoint","X_sex_id","X_age_sex"), allow.cartesian = T)
  }
  
  # adjust data
  message(paste(xwalk_lab,"adjusting data"))
  xwalk_dt[,adj_mean:=logit_mean-value]

  # collapse across draws
  message(paste(xwalk_lab,"collapsing across draws"))
  xwalk_dt<-xwalk_dt[,.(adj_logit_mean=mean(adj_mean),
                        adj_logit_standard_error=sd(adj_mean),
                        xwalk_val=mean(value),
                        xwalk_val_se=sd(value)),
                     by = c(names(xwalk_dt)[!names(xwalk_dt)%in%c("sample_size","cases",
                                                                  "lower","upper","effective_sample_size",
                                                                  "X_intercept","Z_intercept","draw_num",
                                                                  "value","adj_mean")])]
  
  # inverse logit
  message(paste(xwalk_lab,"inverse logit transforming non-0 data points"))
  xwalk_dt[,adj_invlogit_mean:=invlogit(adj_logit_mean)]
  
  message(paste(xwalk_lab,"applying the delta method to derive standard error of the inverse logit transformed means"))
  xwalk_dt[,adj_invlogit_standard_error:=sapply(1:nrow(xwalk_dt), function(i){
    mean_i <- xwalk_dt[i,get("adj_logit_mean")]
    mean_se_i <- xwalk_dt[i,get("adj_logit_standard_error")]
    deltamethod(~exp(x1)/(1+exp(x1)),mean_i,mean_se_i^2)
  })]
  
  # calculate standard error for mean 0 points
  xwalk_dt[mean==0, adj_invlogit_mean := 0]
  xwalk_dt[mean==0, adj_invlogit_standard_error := sqrt(standard_error^2+xwalk_val_se^2)]

  # clean up - drop extra columns, set names, etc.
  remove_cols<-c("logit_mean","logit_standard_error","adj_logit_mean","adj_logit_standard_error",
                 "mean","standard_error","xwalk_val","xwalk_val_se","age_midpoint","sex_id","age_sex")
  remove_cols<-remove_cols[remove_cols%in%names(xwalk_dt)]
  xwalk_dt[,(remove_cols):=NULL]
  setnames(xwalk_dt,c("adj_invlogit_mean","adj_invlogit_standard_error"),c("mean","standard_error"))
  
  # clear other uncertainty information
  xwalk_dt[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA)]
  
  # change seq to crosswalk_parent_seq where crosswalk parent seq is na 
  xwalk_dt[is.na(crosswalk_parent_seq),crosswalk_parent_seq:=seq]
  xwalk_dt[,seq:=NULL]
  
  total_dt <- rbindlist(list(noxwalk_dt, xwalk_dt), use.names=T, fill = T)
  return(total_dt)
}

#   -----------------------------------------------------------------------


# helper functions --------------------------------------------------------

# check that there are age and sex specific rows for each group-nid 
# combination among data marked to age-sex split
check_age_sex<-function(id,dt){
  group_i<-unique(dt[nid==id,group])
  for (i in group_i){
    specs<-unique(dt[nid==id&group==i,specificity])
    if(!all("age"%in%specs,"sex"%in%specs)){
      stop(paste("NID",id,"is marked for age-sex spitting but also has", specs,"specific rows. Check
                to see if this source should be age-sex split, and if so, add both age and sex specific 
               data to the bundle"))
    }
  }
}

# Propagate standard error for the division or multiplication of two independent
# variables with their own standard error estimates
prop_se_div<-function(m1,se1,m2,se2){
  se_div<-sqrt((m1^2/m2^2)*((se1^2/m1^2)+(se2^2/m2^2)))
}

prop_se_mult<-function(m1,se1,m2,se2){
  se_mult<-sqrt((se1^2*se2^2)+(se1^2*m2^2)+(se2^2*m1^2))
}

#   -----------------------------------------------------------------------



