## NAME
## Graph GPR results of HIV ST-GPR process
if (Sys.info()[1]=="Windows") {
  root <- "ADDRESS"
  user <- Sys.getenv("USERNAME")
} else {
  root <- "ADDRESS"
  user <- Sys.getenv("USER")
}
library(RMySQL);library(reshape); library(ggplot2)
source(paste0(root,"FILEPATH"))

results_dir <- "FILEPATH"
archive_name <- "20160523_refresh_env"
data_date <- "12022015" # Formatted month/date/year
graph_dir <- paste0(root,"/temp/grant")

## Create age and location maps
myconn <- dbConnect(RMySQL::MySQL(), host="ADDRESS", username="USERNAME", password="PASSWORD") # Requires connection to shared DB
sql_command <- paste0("SELECT * from shared.age_group ",
                      "WHERE age_group_id <=21 ",
                      "AND age_group_id != 1"
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
names(raw_data)[names(raw_data)=="death_rate"] <- "raw_data"
raw_data <- raw_data[,c("location_id","year_id","age_group_id","sex_id","raw_data")]
raw_data <- raw_data[raw_data$age_group_id >=4,] # Exclude Early and late Neonatal (no data was used to inform the regression)

## Bring in country-level parameters
params <- read.csv(paste0(results_dir,"/params.csv"))

## Bring in GPR Results
## First, archived results
results_compare <- read.csv(paste0(results_dir,"/archive/gpr_results_",archive_name,".csv"))
results_compare <- results_compare[,c("location_id","year_id","age_group_id","sex_id","gpr_mean")]
names(results_compare)[names(results_compare)=="gpr_mean"] <- "compare_mean"

## Second, real results
results <- read.csv(paste0(results_dir,"/gpr_results.csv"))
results <- merge(results,results_compare,by=c("location_id","year_id","age_group_id","sex_id"))
results <- merge(results,age_map,by="age_group_id")
results <- merge(results,locations,by="location_id")
results$sex_name <- ""
results$sex_name[results$sex_id == 1] <- "male"
results$sex_name[results$sex_id == 2] <- "female"

# results$linear_pred <- exp(results$ln_dr_predicted) ## even though it's named ln_, it's actually in real values here
names(results)[names(results) == "ln_dr_predicted"] <- "linear_pred"
results$gpr_lower <- results$gpr_upper <- results$gpr_var <- results$ln_dr_predicted <- NULL

results <- melt(results,measure.vars = c("linear_pred","st_prediction","gpr_mean","compare_mean"),variable_name="estimate_type",na.rm=T)
results <- merge(results,raw_data,by=c("location_id","year_id","age_group_id","sex_id","age_group_id"),all.x=T)
results <- results[order(results$ihme_loc_id,results$age_group_id,results$estimate_type,results$year_id),]
results$age_label <- factor(results$age_group_id,labels=unique(results$age_group_name)) # Since age_group_id is sorted above, the labels should correspond appropriately

## Plot everything!
plot_results <- function(ddd, lll,region) {
  locname <- unique(ddd$location_name[ddd$ihme_loc_id == lll])
  plot <- ggplot(data = ddd[ddd$ihme_loc_id == lll,]) +
    geom_line(aes(x=year_id,y=value,color=estimate_type,linetype=sex_name)) +
    geom_point(aes(x=year_id,y=raw_data, shape=sex_name), size = 1.5) +
    facet_wrap(~age_group_name, scales="free_y") +
    ylab("HIV Death Rate") +
    xlab("Year") +
    ggtitle(paste0("ST-GPR Results: ",lll," ",locname,"\n",region))
  print(plot)
}

# pdf(paste0(graph_dir,"/hiv_stgpr_results.pdf"), width = 11)
# for(region in unique(results$region_name)) {
#   locs <- unique(results$ihme_loc_id[results$region_name == region])
#   for(lll in locs) {
#     plot_results(results,lll,region)
#   }
# }
# dev.off()

## Plots by sex to better-examine what's going on
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

#
# lll <- "USA_523"
# sss <- "male"
# region <- results$region_name[results$ihme_loc_id==lll]
# pdf(paste0(graph_dir,"/hiv_stgpr_",lll,"_",sss), width = 11)
# plot_results_sex(results,lll,sss,region)
# dev.off()
#


