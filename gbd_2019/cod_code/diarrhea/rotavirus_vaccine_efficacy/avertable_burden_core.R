os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

draws_path <- fix_path("ADDRESS") ### Path to PAF and VE draws 
output_path <- fix_path("ADDRESS")

loc_ids <- commandArgs()[6]

calculate_aversion <- function(inputs){
  deaths_total_observed0 <- inputs$deaths_total_observed
  deaths_total_observed <- subset(deaths_total_observed0,age_group_id %in% c(4,5))
  deaths_total_observed0 <- subset(deaths_total_observed0,age_group_id %in% c(2,3))
  paf_observed0 <- subset(inputs$paf_observed,age_group_id %in% c(2,3))
  paf_expected0 <- subset(inputs$paf_expected,age_group_id %in% c(2,3))
  paf_observed <- subset(inputs$paf_observed,age_group_id %in% c(4,5))
  paf_expected <- subset(inputs$paf_expected,age_group_id %in% c(4,5))
  ve <- inputs$ve
  cases_total_observed0 <- inputs$cases_total_observed
  cases_total_observed <- subset(cases_total_observed0,age_group_id %in% c(4,5))
  cases_total_observed0 <- subset(cases_total_observed0,age_group_id %in% c(2,3))
  nf_paf_observed <- subset(inputs$nf_paf_observed,age_group_id %in% c(4,5))
  nf_paf_observed0 <- subset(inputs$nf_paf_observed,age_group_id %in% c(2,3))
  nf_paf_expected <- inputs$nf_paf_expected
  
  #subset to only draws
  exp_labels <- "expected"
  exp_inds <- grep(pattern=exp_labels,x=names(paf_expected))
  exp_inds0 <- grep(pattern=exp_labels,x=names(paf_expected0))
  nf_exp_inds <- grep(pattern=exp_labels,x=names(nf_paf_expected))
  non_inds <- which(1:ncol(paf_expected) %not in% exp_inds)
  
  obs_labels <- "draw"
  obs_inds <- grep(pattern=obs_labels,x=names(paf_observed))
  obs_inds0 <- grep(pattern=obs_labels,x=names(paf_observed0))
  nf_obs_inds <- grep(pattern=obs_labels,x=names(nf_paf_observed))
  nf_obs_inds0 <- grep(pattern=obs_labels,x=names(nf_paf_observed0))
  non_inds <- which(1:ncol(paf_observed) %not in% obs_inds)
  
  
  ve_labels <- "rv_impact"
  ve_inds <- grep(pattern=ve_labels,x=names(ve))
  non_inds <- which(1:ncol(ve) %not in% ve_inds)
  
  exp_draws <- paf_expected[,exp_inds]
  obs_draws <- paf_observed[,obs_inds]
  exp_draws0 <- paf_expected0[,exp_inds0]
  obs_draws0 <- paf_observed0[,obs_inds0]
  ve_draws <- ve[,ve_inds]
  nf_exp_draws <- nf_paf_expected[,nf_exp_inds]
  nf_obs_draws <- nf_paf_observed[,nf_obs_inds]
  nf_obs_draws0 <- nf_paf_observed0[,nf_obs_inds0]
  
  
  deaths_resort <- paste0("draw_",0:999)
  deaths_resort_inds <- which(names(deaths_total_observed) %in% deaths_resort)
  non_inds <- which(1:ncol(deaths_total_observed) %not in% deaths_resort_inds)
  non_names <- names(deaths_total_observed)[non_inds]
  deaths_total_draws <- as.data.frame(deaths_total_observed[,mget(deaths_resort)])
  
  deaths_resort <- paste0("draw_",0:999)
  deaths_resort_inds <- which(names(deaths_total_observed0) %in% deaths_resort)
  non_inds <- which(1:ncol(deaths_total_observed0) %not in% deaths_resort_inds)
  non_names <- names(deaths_total_observed0)[non_inds]
  deaths_total_draws0 <- as.data.frame(deaths_total_observed0[,mget(deaths_resort)])
  
  cases_resort <- paste0("draw_",0:999)
  cases_resort_inds <- which(names(cases_total_observed) %in% cases_resort)
  non_inds <- which(1:ncol(cases_total_observed) %not in% cases_resort_inds)
  non_names <- names(cases_total_observed)[non_inds]
  cases_total_draws <- as.data.frame(cases_total_observed[,(cases_resort)])
  
  cases_resort <- paste0("draw_",0:999)
  cases_resort_inds <- which(names(cases_total_observed0) %in% cases_resort)
  non_inds <- which(1:ncol(cases_total_observed0) %not in% cases_resort_inds)
  non_names <- names(cases_total_observed0)[non_inds]
  cases_total_draws0 <- as.data.frame(cases_total_observed0[,(cases_resort)])
  
  
  ve_super <- ve_draws
  ve_super[1:nrow(deaths_total_draws),] <- ve_draws
  
  #sort datasets by draw number
  deaths_non_rv <- deaths_total_draws*(1-obs_draws)
  deaths_total_expected <- deaths_non_rv/(1-exp_draws)
  deaths_rv_observed <- deaths_total_draws*obs_draws
  deaths_rv_expected <- deaths_total_expected*exp_draws
  deaths_averted <- deaths_total_expected - deaths_total_draws
  deaths_avertable <- deaths_rv_observed - deaths_rv_expected*(1-ve_super)
  
  deaths_rv_observed0 <- deaths_total_draws0*obs_draws0
  deaths_rv_observed <- rbind(deaths_rv_observed,deaths_rv_observed0)
  
  cases_non_rv <- cases_total_draws*(1-nf_obs_draws)
  cases_total_expected <- cases_non_rv/(1-nf_exp_draws)
  cases_rv_observed <- cases_total_draws*nf_obs_draws
  cases_rv_expected <- cases_total_expected*nf_exp_draws
  cases_averted <- cases_total_expected - cases_total_draws
  cases_avertable <- cases_rv_observed - cases_rv_expected*(1-ve_super)
  
  cases_rv_observed0 <- cases_total_draws0*nf_obs_draws0
  cases_rv_observed <- rbind(cases_rv_observed,cases_rv_observed0)
  
  summary_current <- quantile(colSums(deaths_rv_observed),c(0.025,0.5,0.975))
  summary_averted <- quantile(colSums(deaths_averted),c(0.025,0.5,0.975))
  summary_avertable <- quantile(colSums(deaths_avertable),c(0.025,0.5,0.975))
  
  nf_summary_current <- quantile(colSums(cases_rv_observed),c(0.025,0.5,0.975))
  nf_summary_averted <- quantile(colSums(cases_averted),c(0.025,0.5,0.975))
  nf_summary_avertable <- quantile(colSums(cases_avertable),c(0.025,0.5,0.975))
  
  out_l <- list(current=colSums(deaths_rv_observed),
                averted=colSums(deaths_averted),
                avertable=colSums(deaths_avertable),
                nf_current=colSums(cases_rv_observed),
                nf_averted=colSums(cases_averted),
                nf_avertable=colSums(cases_avertable))
  return(out_l)
 }

get_inputs <- function(loc_id,year){
  deaths_total_observed <- get_draws(
    gbd_id_type="cause_id",
    gbd_id=302,
    measure_id=1,
    metric_id=1,
    location_id=loc_id,
    year_id=year,
    #status="latest",
    version_id=107,
    sex_id=c(1,2),
    age_group_id=c(2,3,4,5),
    decomp_step="step4",
    source="codcorrect",
    gbd_round_id=6)
  
  
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
  
  
  inc0 <- as.data.frame(get_draws(
    gbd_id_type="cause_id",
    gbd_id=302,
    location_id=loc_id,
    year_id=year,
    age_group_id=c(2,3,4,5),
    sex_id=c(1,2),
    measure_id=6,
    version_id=458,
    decomp_step="step4",
    source="como",
    gbd_round_id=6))
  
  pop <- get_population(sex_id=c(1,2),age_group_id=c(2,3,4,5),location_id=loc_id,gbd_round_id=6,decomp_step="step4")
  inc <- merge(inc0,pop[,c("age_group_id","location_id","year_id","sex_id","population")])
  inds <- draw_inds(inc)
  cases_total_observed <- inc
  cases_total_observed[,inds[[1]]] <- inc[,inds[[1]]]*inc$population
  
  paf_path <- fix_path("FILEPATH")
  obs_labels <- "draw"
  exp_labels <- "expected"
  paf_df <- read.csv(paf_path)
  obs_inds <- grep(pattern=obs_labels,x=names(paf_df))
  exp_inds <- grep(pattern=exp_labels,x=names(paf_df))
  non_inds <- which(1:ncol(paf_df) %not in% c(obs_inds,exp_inds))
  
  nf_paf_observed <- paf_df[,c(non_inds,obs_inds)]
  nf_paf_observed <- subset(nf_paf_observed,year_id==year&location_id==loc_id&age_group_id %in% c(2,3,4,5))
  
  nf_paf_expected <- paf_df[,c(non_inds,exp_inds)]
  nf_paf_expected <- subset(nf_paf_expected,year_id==year&location_id==loc_id&age_group_id %in% c(4,5))
  
  return(list(deaths_total_observed=deaths_total_observed,
              ve=ve_df,
              paf_observed=paf_observed,
              paf_expected=paf_expected,
              cases_total_observed=cases_total_observed,
              nf_paf_observed=nf_paf_observed,
              nf_paf_expected=nf_paf_expected))
}

calculate <- function(loc_id,year){
  test_objs <- get_inputs(loc_id=loc_id,year=year)
  out <- calculate_aversion(test_objs)
  return(out)
}

year <- 2019
fs <- list.files(draws_path)

current <- data.frame(location_id=NULL,year_id=NULL,lb=NULL,mean=NULL,ub=NULL)
averted <- data.frame(location_id=NULL,year_id=NULL,lb=NULL,mean=NULL,ub=NULL)
avertable <- data.frame(location_id=NULL,year_id=NULL,lb=NULL,mean=NULL,ub=NULL)
start <- Sys.time()
for(loc_id in loc_ids){
  out <- calculate(loc_id,year)
  current <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$current)
  averted <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$averted)
  avertable <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$avertable)
  nf_current <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$nf_current)
  nf_averted <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$nf_averted)
  nf_avertable <- cbind(data.frame(location_id=loc_id,year_id=year),val=out$nf_avertable)
  write.csv(current, "FILEPATH",row.names=FALSE)
  write.csv(averted, "FILEPATH",row.names=FALSE)
  write.csv(avertable, "FILEPATH",row.names=FALSE)
  write.csv(nf_current, "FILEPATH",row.names=FALSE)
  write.csv(nf_averted, "FILEPATH",row.names=FALSE)
  write.csv(nf_avertable, "FILEPATH",row.names=FALSE)
}
print(Sys.time()-start)
