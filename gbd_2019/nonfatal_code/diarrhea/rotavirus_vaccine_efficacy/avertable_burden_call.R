#function of loc_id

os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}


source(fix_path("FILEPATH"))

draws_path <- fix_path("ADDRESS")
output_path <- fix_path("ADDRESS")

calculate_aversion <- function(input_list){
  deaths_total_observed <- input_list$deaths_total_observed
  paf_observed <- input_list$paf_observed
  paf_expected <- input_list$paf_expected
  ve <- input_list$ve
  
  #subset to only draws
  exp_labels <- "expected"
  exp_inds <- grep(pattern=exp_labels,x=names(paf_expected))
  non_inds <- which(1:ncol(paf_expected) %not in% exp_inds)
  
  obs_labels <- "draw"
  obs_inds <- grep(pattern=obs_labels,x=names(paf_observed))
  non_inds <- which(1:ncol(paf_observed) %not in% obs_inds)
  
  ve_labels <- "rv_impact"
  ve_inds <- grep(pattern=ve_labels,x=names(ve))
  non_inds <- which(1:ncol(ve) %not in% ve_inds)
  
  exp_draws <- paf_expected[,exp_inds]
  obs_draws <- paf_observed[,obs_inds]
  ve_draws <- ve[,ve_inds]
  
  deaths_resort <- paste0("draw_",0:999)
  deaths_total_draws <- as.data.frame(deaths_total_observed[,mget(deaths_resort)])
  
  ve_super <- ve_draws
  ve_super[1:nrow(deaths_total_draws),] <- ve_draws
  
  #sort datasets by draw number
  deaths_non_rv <- deaths_total_draws*(1-obs_draws)
  deaths_total_expected <- deaths_non_rv/(1-exp_draws)
  deaths_rv_observed <- deaths_total_draws*obs_draws
  deaths_rv_expected <- deaths_total_expected*exp_draws
  deaths_averted <- deaths_total_expected - deaths_total_draws
  deaths_avertable <- deaths_rv_observed - deaths_rv_expected*(1-ve_super)

  return(out_df)
}

get_inputs <- function(loc_id,year){
  deaths_total_observed <- get_draws(
    gbd_id_type="cause_id",
    gbd_id=302,
    measure_id=1,
    metric_id=1,
    location_id=loc_id,
    year_id=year,
    status="latest",
    sex_id=c(1,2),
    age_group_id=c(2,3,4,5),
    decomp_step="step4",
    source="codcorrect")
  
  ve_path <- "FILEPATH"
  ve_df <- read.csv(ve_path)
  ve_df <- subset(ve_df,year_id==year)
  
  paf_path <- fix_path("FILEPATH")
  obs_labels <- "draw"
  exp_labels <- "expected"
  paf_df <- read.csv(paf_path)
  obs_inds <- grep(pattern=obs_labels,x=names(paf_df))
  exp_inds <- grep(pattern=exp_labels,x=names(paf_df))
  non_inds <- which(1:ncol(paf_df) %not in% c(obs_inds,exp_inds))
  
  paf_observed <- paf_df[,c(non_inds,obs_inds)]
  paf_observed <- subset(paf_observed,year_id==year&location_id==loc_id&age_group_id %in% c(2,3,4,5))
  
  paf_expected <- paf_df[,c(non_inds,exp_inds)]
  paf_expected <- subset(paf_expected,year_id==year&location_id==loc_id&age_group_id %in% c(2,3,4,5))
  
  return(list(deaths_total_observed=deaths_total_observed,ve=ve_df,paf_observed=paf_observed,paf_expected=paf_expected))
}
