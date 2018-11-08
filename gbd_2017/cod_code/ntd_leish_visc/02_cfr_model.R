#death code test


rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))

logit <- function (x){
  if (any(omit <- (is.na(x) | x <= 0 | x >= 1))) {
    lv <- x
    lv[omit] <- NA
    if (any(!omit))
      lv[!omit] <- Recall(x[!omit])
    return(lv)
  }
  log(x/(1 - x))
}

ilogit <- function(x)1/(1+exp(-x))

leish_geo<-read.csv(sprintf("FILEPATH/geo_restrict_vl.csv",prefix), stringsAsFactors = FALSE)

leish_geo <- leish_geo[leish_geo$type == "status",]

require(lme4)

st_gpr_output<-read.csv(file=paste0(prefix, "FILEPATH/cfr_dataset_14June.csv"))

st_gpr_output$age_group_id <- as.factor(st_gpr_output$age_group_id)
st_gpr_output$sex_id <- as.factor(st_gpr_output$sex_id)
st_gpr_output$location_id <- as.factor(st_gpr_output$location_id)
st_gpr_output$region_id <- rep(NA, length(st_gpr_output$location_id))
st_gpr_output$superregion_id <- rep(NA, length(st_gpr_output$location_id))

for (i in 1:length(st_gpr_output$region_id)){
  tmploc <- which(leish_geo$loc_id == st_gpr_output$location_id[i])
  st_gpr_output$region_id[i] <- leish_geo$region_id[tmploc]
  st_gpr_output$superregion_id[i] <- leish_geo$spr_reg_id[tmploc]
}

st_gpr_output$region_id <- as.factor(st_gpr_output$region_id)
st_gpr_output$superregion_id <- as.factor(st_gpr_output$superregion_id)

st_gpr_output$logit_cfr <- logit(st_gpr_output$data)

mod3 <- lmer(logit_cfr ~ age_group_id + sex_id + (1 | superregion_id/region_id), data = st_gpr_output)

#save mod
# save(mod3,
#      file=paste0(prefix, "FILEPATH/test_CFR_withnested_22Mar.RData"))

#Prep components for populating case fatality rate matrices

intercept<-fixef(mod3)[1]
sex_id2<-fixef(mod3)[22]
#present the random effects values for regions
unique_regions<-unique(st_gpr_output$region_id)
unique_superregions<-unique(st_gpr_output$superregion_id)
region_vals<-ranef(mod3)$region_id
region_vals$rownames<-rownames(region_vals)
for (i in 1:nrow(region_vals)){
  region_vals$region_id[i]<-strsplit(region_vals$rownames[i],':')[[1]][1]
  region_vals$superregion_id[i]<-strsplit(region_vals$rownames[i],':')[[1]][2]
}
superregion_vals<-ranef(mod3)$superregion_id
superregion_vals$superregion_id<-rownames(superregion_vals)

#identify the std dev for the random effects and errors, and generate 1,000 draws
std_dev_df<-data.frame(VarCorr(mod3))
residual_std<-std_dev_df$sdcor[which(std_dev_df$grp=="Residual")]
residual_std_string<-rnorm(1000,0,residual_std)
region_super_residual_std<-std_dev_df$sdcor[which(std_dev_df$grp=="region_id:superregion_id")]
region_superregion_std_string<-rnorm(1000, 0, region_super_residual_std)
super_residual_std<-std_dev_df$sdcor[which(std_dev_df$grp=="superregion_id")]
super_residual_std_string<-rnorm(1000,0, super_residual_std)

#present the fixed effects from ages in a table
age_effects<-data.frame(fixef(mod3))
names(age_effects)<-"value"
age_effects$age_group_id<-c(seq(4,20,1),30,31,32,235,NA)

requirements_set<-get_demographics('ADDRESS')
required_ages<-requirements_set$age_group_id
required_sex<-requirements_set$sex_id
required_locations<-requirements_set$location_id
required_years<-requirements_set$year_id

initiator_cfr<-expand.grid(age_group_id = required_ages,sex_id = c(1,2))
initiator_cfr$year_id<-rep(NA, nrow(initiator_cfr))
initiator_cfr$location_id<-rep(NA, nrow(initiator_cfr))
initiator_cfr$cause_id<-rep(348, nrow(initiator_cfr))
initiator_cfr$measure_id<-rep(1, nrow(initiator_cfr))

#region_key
leish_geo<-read.csv(sprintf("FILEPATH/geo_restrict_vl.csv",prefix), stringsAsFactors = FALSE)
leish_geo<-subset(leish_geo, leish_geo$type=='status')

#save a case fatality rate output for every location_id. Is temporally invariant
start_time<-Sys.time()
cfr_list<-list()
n_reps<-1000
for (i in required_locations){
  placeholder_cfr<-initiator_cfr
  placeholder_cfr$location_id<-rep(i, nrow(placeholder_cfr))
  URegion<-leish_geo$region_id[which(leish_geo$loc_id==i)]
  USuperregion<-leish_geo$spr_reg_id[which(leish_geo$loc_id==i)]
  cfr_matrix<-matrix(data=NA, nrow=nrow(placeholder_cfr), ncol=1000)
  for(k in 1:nrow(placeholder_cfr)){
    UAge<-placeholder_cfr$age_group_id[k]
    USex<-placeholder_cfr$sex_id[k]

    if(UAge %in% c(2,3)){
      string<-rep(0,1000)
      cfr_matrix[k,]<-string
    }else{
    if(UAge == 4 & USex == 1){
      #mimic structure of age_group_id + sex_id + geography effects + errors
      if(URegion %in% unique_regions){
        URegion_string<-rep(region_vals[which(region_vals$region_id==URegion),1],1000)
      }else{
        URegion_string<-rep(0,1000)
      }
      if(USuperregion %in% unique_superregions){
        USuperregion_string<-rep(superregion_vals[which(superregion_vals$superregion_id==USuperregion),1],1000)
      }else{
        USuperregion_string<-rep(0,1000)
      }

      string<-rep(intercept,1000)+ rep(0,1000) + URegion_string + USuperregion_string + region_superregion_std_string + super_residual_std_string + residual_std_string
      cfr_matrix[k,]<-ilogit(string)
    }
    if(UAge == 4 & USex == 2){
      #mimic structure of age_group_id + sex_id + geography effects + errors
      if(URegion %in% unique_regions){
        URegion_string<-rep(region_vals[which(region_vals$region_id==URegion),1],1000)
      }else{
        URegion_string<-rep(0,1000)
      }
      if(USuperregion %in% unique_superregions){
        USuperregion_string<-rep(superregion_vals[which(superregion_vals$superregion_id==USuperregion),1],1000)
      }else{
        USuperregion_string<-rep(0,1000)
      }

      string<-rep(intercept,1000)+ rep(sex_id2,1000) + URegion_string + USuperregion_string + region_superregion_std_string + super_residual_std_string + residual_std_string
      cfr_matrix[k,]<-ilogit(string)
    }
    if(UAge %in% c(seq(5,20,1),30,31,32,235) & USex == 1){
      #mimic structure of age_group_id + sex_id + geography effects + errors
      if(URegion %in% unique_regions){
        URegion_string<-rep(region_vals[which(region_vals$region_id==URegion),1],1000)
      }else{
        URegion_string<-rep(0,1000)
      }
      if(USuperregion %in% unique_superregions){
        USuperregion_string<-rep(superregion_vals[which(superregion_vals$superregion_id==USuperregion),1],1000)
      }else{
        USuperregion_string<-rep(0,1000)
      }

      UAge_string<-rep(age_effects$value[which(age_effects$age_group_id==UAge)])

      string<-rep(intercept,1000)+ UAge_string + rep(0,1000) + URegion_string + USuperregion_string + region_superregion_std_string + super_residual_std_string + residual_std_string
      cfr_matrix[k,]<-ilogit(string)
    }
    if(UAge %in% c(seq(5,20,1),30,31,32,235) & USex == 2){
      #mimic structure of age_group_id + sex_id + geography effects + errors
      if(URegion %in% unique_regions){
        URegion_string<-rep(region_vals[which(region_vals$region_id==URegion),1],1000)
      }else{
        URegion_string<-rep(0,1000)
      }
      if(USuperregion %in% unique_superregions){
        USuperregion_string<-rep(superregion_vals[which(superregion_vals$superregion_id==USuperregion),1],1000)
      }else{
        USuperregion_string<-rep(0,1000)
      }

      UAge_string<-rep(age_effects$value[which(age_effects$age_group_id==UAge)])

      string<-rep(intercept,1000)+ UAge_string + rep(sex_id2,1000) + URegion_string + USuperregion_string + region_superregion_std_string + super_residual_std_string + residual_std_string
      cfr_matrix[k,]<-ilogit(string)
    }
    }
  }
  summary_matrix<-cbind(placeholder_cfr, cfr_matrix)
  cfr_list[[which(required_locations==i)]]<-summary_matrix
  print(paste0("Processed ",i, " - ", length(required_locations)-which(required_locations==i), " remaining"))
}
end_time<-Sys.time()
#save list as RDATA intermediate
save(cfr_list,
     file=paste0(prefix, "FILEPATH/cfr_location_summary_14June.RData"))
save(mod3,
     file=paste0(prefix, "FILEPATH/cfr_model_14June.RData"))

#generate death counts from incidence estimates, multiplied by population and case fatality rate
#identify important locations
presence_subset<-subset(leish_geo, leish_geo$X1980 %in% c("p", "pp"))
presence_locations<-unique(presence_subset$loc_id)

#save as an RData for weekend run crunch to load in a get going!
save.image(paste0(prefix, "FILEPATH/workspace.RData"))
