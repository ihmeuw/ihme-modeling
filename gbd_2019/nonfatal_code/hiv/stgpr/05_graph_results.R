## NAME
## Graph GPR results of HIV ST-GPR process
if (Sys.info()[1]=="Windows") {
  root <- "J:" 
  user <- Sys.getenv("USERNAME")
} else {
  root <- "/home/j"
  user <- Sys.getenv("USER")
}
library(ggplot2) ; library(reshape2); library(RMySQL); library(data.table)
library(mortdb, lib = "FILEPATH")
source(paste0(root,"FILEPATH"))


results_dir <- "FILEPATH"
archive_name <- "ARCHIVE_NAME"
graph_dir <- "FILEPATH"

## Create age and location maps
myconn <- dbConnect(RMySQL::MySQL(), host="modeling-mortality-db.ihme.washington.edu", username="dbview", password="E3QNSLvQTRJm") # Requires connection to shared DB
sql_command <- paste0("SELECT * from shared.age_group ",
                      "WHERE age_group_id IN(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,30,31,32,235) "
                      
                      )
age_map <- dbGetQuery(myconn, sql_command)
dbDisconnect(myconn)

age_map <- age_map[,c("age_group_id","age_group_name")]

locations <- get_locations()
locations <- locations[,c("ihme_loc_id","location_name","location_id","region_name")]

## Bring in Data for GPR
## Bring in this file INSTEAD of test_hiv_dataset because that one hasn't been converted to with-HIV envelope deaths yet

# raw_data <- read.csv(paste0(results_dir,"/test_hiv_dataset_",data_date,".csv"))
# raw_data$death_rate <- (raw_data$deaths / raw_data$pop) * 100
# names(raw_data)[names(raw_data) == "year"] <- "year_id"
# names(raw_data)[names(raw_data) == "sex"] <- "sex_id"
# names(raw_data)[names(raw_data) == "death_rate"] <- "raw_data"

raw_data <- read.csv(paste0(results_dir,"/linear_predictions.csv"))
names(raw_data)[names(raw_data)=="death_rate"] <- "current_input_data"
raw_data <- raw_data[,c("location_id","year_id","age_group_id","sex_id","raw_data")]
raw_data <- raw_data[raw_data$age_group_id >=4,] # Exclude Early and late Neonatal (no data was used to inform the regression) 

#adding in 2016 raw data
raw_data_compare <- read.csv(paste0(results_dir,"/archive/linear_predictions_",archive_name,".csv"))
names(raw_data_compare)[names(raw_data_compare)=="death_rate"] <- "input_data_rhino"
raw_data_compare <- raw_data_compare[,c("location_id","year_id","age_group_id","sex_id","raw_data_compare")]
raw_data_compare <- raw_data_compare[raw_data_compare$age_group_id >=4,] # Exclude Early and late Neonatal (no data was used to inform the regression)

### gbd_2019 final, current_model, input/raw_data
###gpr_mean, rhino, raw_data, input_data for gpr_mean.

## Bring in country-level parameters
params <- read.csv(paste0(results_dir, "/params.csv"))

## Bring in GPR Results
## First, archived results
results_compare <- read.csv(paste0(results_dir,"/archive/gpr_results_",archive_name,".csv"))
results_compare <- results_compare[,c("location_id","year_id","age_group_id","sex_id","gpr_mean")]
names(results_compare)[names(results_compare)=="gpr_mean"] <- "rhino_mean"

## Second, real results
results <- read.csv(paste0(results_dir, "/gpr_results_20191209_test_20.csv"))
results <- merge(results,results_compare,by=c("location_id","year_id","age_group_id","sex_id"),all.x = T)
results <- merge(results,age_map,by="age_group_id")
results <- merge(results,locations,by="location_id")
results$sex_name <- ""
results$sex_name[results$sex_id == 1] <- "male"
results$sex_name[results$sex_id == 2] <- "female"

# results$linear_pred <- exp(results$ln_dr_predicted) ## even though it's named ln_, it's actually in real values here
# names(results)[names(results) == "ln_dr_predicted"] <- "linear_pred"
# results$gpr_var <- results$ln_dr_predicted <- NULL

results <- melt(results,measure.vars = c("linear_pred","st_prediction","gpr_mean","rhino_190415_orca"),variable.name="estimate_type",na.rm=T)
results <- merge(results,raw_data,by=c("location_id","year_id","age_group_id","sex_id","age_group_id"),all.x=T) 
results <- merge(results,raw_data_compare,by=c("location_id","year_id","age_group_id","sex_id","age_group_id"),all.x=T)
results <- results[order(results$ihme_loc_id,results$age_group_id,results$estimate_type,results$year_id),]
results$age_label <- factor(results$age_group_id,labels=unique(results$age_group_name)) # Since age_group_id is sorted above, the labels should correspond appropriately
results <- melt(results,measure.vars = c("raw_data","raw_data_compare"),variable.name="raw_type",na.rm=F, value.name = "value2") 


write.csv(results,paste0(results_dir, "/results_for_graphs_1212.csv"),row.names = F)


## Plots results by sex to better-examine what's going on
plot_results_sex <- function(ddd, lll,sss,region) {
  locname <- unique(ddd$location_name[ddd$ihme_loc_id == lll & ddd$sex_name == sss])
  lambda <- round(unique(params$lambda[params$ihme_loc_id == lll]),2)
  zeta <- round(unique(params$zeta[params$ihme_loc_id == lll]),2)
  omega <- unique(params$omega[params$ihme_loc_id == lll])
  scale <- unique(params$scale[params$ihme_loc_id == lll])
  amp <- unique(params$amp[params$ihme_loc_id == lll])

  if(nrow(ddd[ddd$ihme_loc_id == lll & ddd$sex_name == sss & !is.na(results$raw_data),]) > 0) {
    plot <- ggplot(data = ddd[ddd$ihme_loc_id == lll & ddd$sex_name == sss,]) +
      geom_line(aes(x=year_id,y=value,color=estimate_type)) +
      geom_point(aes(x=year_id,y=raw_data), size = 1.5) +
      facet_wrap(~age_group_name, scales="free_y") +
      ylab("HIV Death Rate") +
      xlab("Year") +
      ggtitle(paste0("ST-GPR Results: ",lll," ",locname," ",sss,"\n",region,"\n","Lambda ",lambda,", Zeta ",zeta,", Omega ",omega,", Scale ",scale,", Amp ",amp,"\n Comparison to ",archive_name))
  } else {
    plot <- ggplot(data = ddd[ddd$ihme_loc_id == lll & ddd$sex_name == sss,]) +
      geom_line(aes(x=year_id,y=value,color=estimate_type)) +
      facet_wrap(~age_group_name, scales="free_y") +
      ylab("HIV Death Rate") +
      xlab("Year") +
      ggtitle(paste0("ST-GPR Results: ",lll," ",locname," ",sss,"\n",region,"\n","Lambda ",lambda,", Zeta ",zeta,", Omega ",omega,", Scale ",scale,", Amp ",amp,"\n Comparison to ",archive_name))
  }
  print(plot)
}


for(sex in c("male","female")) {
  pdf(paste0(graph_dir,"/hiv_stgpr_",sex,".pdf"), width = 11)
  for(region in unique(results$region_name)) {
    locs <- unique(results$ihme_loc_id[results$region_name == region])
    for(lll in locs) {
      print(lll)
      plot_results_sex(results,lll,sex,region)
    }
  }
  dev.off()
}