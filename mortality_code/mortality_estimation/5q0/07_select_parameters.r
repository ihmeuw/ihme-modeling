################################################################################
## Description: Compile results of all holdouts and select parameters
################################################################################

  rm(list=ls())
  library(foreign)

  if (Sys.info()[1] == "Linux") root <- "FILEPATH" else root <- "FILEPATH"
  setwd(paste(root, "FILEPATH", sep=""))
  
## define loss function
  loss_function <- function(are, coverage) loss = ifelse(coverage <= 0.95, are + ((1-coverage)-0.05)/5, are + (0.05 - (1-coverage))/1)

## compile all loss files for countries in each data type 
  countries <- read.csv("FILEPATH/prediction_model_results_all_stages.txt")
  countries <- unique(countries[,c("iso3","gbd_region", "mse")])
  
  if (Sys.info()[1] == "Linux"){
     setwd("FILEPATH")
  }else{
     setwd("FILEPATH")
  }

  all_loss <- NULL 
  all_files <- dir("loss")
  for (rr in sort(unique(countries$gbd_region))) { 
    loss <- NULL
    count <- 0 
    gbd_region <- rr 
    for (ii in which(countries$gbd_region == rr)) { 
      files <- grep(paste(countries$iso3[ii], sep="_"), all_files, value=T)
      for (ff in files) {
        count <- count + 1
        loss[[ff]] <- read.csv(paste("loss/", ff, sep=""))
      } 
    }
    
    loss <- do.call("rbind", loss)
    loss$are <- abs(loss$re)
    all_loss[[gbd_region]] <- loss
  } 
  


## minimize the loss function to select parameters
llist <- NULL
for(kk in 1:10){
  min <- lapply(all_loss,
                function(x) {
                  y <- aggregate(x[x$ho <= kk*10,c("are", "coverage")], x[x$ho <= kk*10,c("scale", "amp2x")], mean)
                  y$loss <- loss_function(y$are, y$coverage)
                  y$best <- as.numeric(y$loss == min(y$loss))
                  return(y) 
                })
  for (ii in names(min)) min[[ii]]$gbd_region <- ii 
  min <- do.call("rbind", min)

  llist[[kk]] <- min

}

for(kk in 1:10)llist[[kk]]$iteration <- kk

llist1 <- do.call("rbind",llist)

setwd("FILEPATH")
write.csv(llist1, file = "iteration_param_select.txt", row.names = F)

min <- lapply(all_loss,
              function(x) {
                y <- aggregate(x,c("are", "coverage")], x[,c("scale", "amp2x")], mean)
                y$loss <- loss_function(y$are, y$coverage)
                y$best <- as.numeric(y$loss == min(y$loss))
                return(y)
              })
for (ii in names(min)) min[[ii]]$gbd_region <- ii
min <- do.call("rbind", min)
  
## merge on mse 
  min <- merge(min, unique(countries[,c("gbd_region", "mse")]), all.x=T)
  min$amp2 <- min$amp2x * min$mse                         

## save
  setwd("FILEPATH")
  write.csv(min, file="selected_parameters.txt", row.names=F)
  write.csv(min, file=paste("archive/selected_parameters_", Sys.Date(), ".txt", sep=""), row.names=F)
