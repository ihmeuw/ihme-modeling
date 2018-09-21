
## Compile sims to make country files


set.seed(seed)

rm(list=ls())
library(foreign); library(data.table); library(plyr) 

if (Sys.info()[1]=="Windows") root <- "filepath" else root <- "filepath"

gpr_dir <- "filepath"

loc <- commandArgs()[3]
sims <- as.numeric(commandArgs()[4])

data <- list()

for (i in 1:sims) {
  if (file.exists(paste0("filepath"))) {
    data[[paste0(i)]] <- read.csv(paste0("filepath"),stringsAsFactors=F) 
    data[[paste0(i)]]$groups <- i
  } else {
    print(paste0("Sim ",i, " not read in"))
  }
}

data <- as.data.frame(rbindlist(data))

## add new sim numbers
newsim <- read.csv(paste0("filepath"),stringsAsFactors=F)
newsim$ihme_loc_id <- NULL


stopifnot(length(unique(data$sim))==1000) 
data <- merge(data,newsim,by="sim")
stopifnot(length(unique(data$sim))==1000)
stopifnot(length(unique(data$newdraw))==1000)
data$sim <- data$newdraw
data$newdraw <- NULL

## Add HIV Sims and 1st stage with RE
  sim_list <- data.frame(groups=1:250)
  
  append_sims <- function(groups) {
    filepath <- paste0("filepath",groups)
    tryCatch(read.csv(paste0(filepath,".txt"), stringsAsFactors = F), error = function(e) print(paste(groups,"not found")))
  }
  
  hiv_sims <- mdply(sim_list,append_sims, .progress = "text")
  hiv_sims <- unique(hiv_sims[hiv_sims$ihme_loc_id == loc,c("hiv","pred.1.wRE","pred.1.noRE","pred.2.final","groups","sex","year")]) # Subset to columns and remove duplicate years for multiple datapoints

  ## Convert HIV Sims to Mx space
  hiv_sims$pred.1.wRE <- log(1-hiv_sims$pred.1.wRE)/(-45)

data <- merge(data,hiv_sims, by=c("year","groups","sex"))

data <- data[order(data$ihme_loc_id,data$sex,data$year,data$sim),]
stopifnot(length(unique(data$sim))==1000)

codes <- read.csv(paste0("filepath"))
parents <- codes[codes$level_1==1 & codes$level_2==0,]
nonparents <- codes[!(codes$ihme_loc_id %in% parents$ihme_loc_id),]
nonparents <- nonparents[nonparents$level == 3 | nonparents$ihme_loc_id %in% c("CHN_354", "CHN_361"),]
data$groups <- NULL

## draws file
for (sex in c("male","female")) {
   if (loc %in% unique(nonparents$ihme_loc_id)){
    write.csv(data[data$sex == sex,],paste0("filepath"),row.names=F)
    } else { write.csv(data[data$sex == sex,],paste0("filepath"),row.names=F)
  }
}


data <- data.table(data)
setkey(data,ihme_loc_id,sex,year)
data <- as.data.frame(data[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                 mort_upper = quantile(mort,probs=.975),
                                 med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                 med_stage1=quantile(pred.1.noRE,.5),
                                 med_stage2 =quantile(pred.2.final,.5)
                                 ),by=key(data)])


## summary file
for (sex in c("male","female")) {
  if (loc %in% unique(nonparents$ihme_loc_id)){
    write.csv(data[data$sex == sex,],paste0("filepath"),row.names=F) 
  } else { write.csv(data[data$sex == sex,],paste0("filepath"),row.names=F)  
  }
}



