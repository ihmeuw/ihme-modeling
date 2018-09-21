#############################################################
## Author: 

rm(list=ls())
library(data.table)
library(plyr)
library(haven)
library(rhdf5)

if (Sys.info()[1] == 'Windows') {
  username <- "PATH"
  root <- ""
} else {
  username <- Sys.getenv("USER")
  root <- ""
  loc <- commandArgs()[3] 
  loc_id <- as.integer(commandArgs()[4])
  output_version_id <- commandArgs()[5] ## shocks version
  env_version <- commandArgs()[6]
  
} 

source(paste0(root,"PATH"))
source(paste0(root,"PATH"))
source(paste0(root,"PATH"))
source(paste0(root, "PATH")) ## generates the rest of the LT from mx, qx, and ax

###########################
## Lifetable functions 
###########################

mx_to_qx <- function(n, mx){
  qx <- 1- exp(-n*mx)
  return(qx)
}

mxax_to_qx <- function(n, ax, mx){
  qx <- (n*mx) / (1+(mx*(n-ax)))
  return(qx)
}

qx_to_mx <- function(n, qx){
  mx <- (-log(1-qx))/n
  return(mx)
}

qxax_to_mx <- function(n, ax, qx){
  mx <- qx/(n-(n*qx) + (ax*qx))
  return(mx)
}


###############################
###############################
## Envelope
###############################
###############################

######################################
## Reading in files, basic data prep
######################################

# FOR TESTING PURPOSES (testing must occur on the cluster to read files correctly)
# library(RMySQL)
# myconn <- dbConnect(RMySQL::MySQL(), host="HOST", username="******", password="************") # Requires connection to shared DB
# sql_command <- paste0("SELECT output_version_id FROM cod.output_version WHERE best_end IS NULL AND best_start IS NOT NULL;")
# output_version_id <- dbGetQuery(myconn, sql_command)
# dbDisconnect(myconn)

# output_version_id <- 21 # This refers to output of the shock aggregator. (see line 137) 
# loc_id <- 44548
# loc <- "SAU_44548"
# env_version <- 152 # updated 3/5/17 to 95, now has files for line 161. Was 44 around 1/15 for draft submission. -AT

# quick test of argument passing:
print(paste0("Args are (loc_id, loc, env_verison_id, output_version_id) = ", loc_id, ", ", loc, ", ", env_version, ", ", output_version_id ))


# lowest_level <- get_locations(level="lowest")
lowest_level <- read.csv(paste0(root, "PATH"))

## reading in shocks, envelope, lifetable, and population files
if(loc_id %in% lowest_level$location_id){
  data <- read.csv(paste0("PATH", output_version_id ,"PATH", loc_id, ".csv"))  ## lowest level shocks
  data <- data[data$cause_id==294,] ## in shocks file, cause_id 294 is all-cause shocks
  data <- data[,!colnames(data) %in% c("cause_id", "location_id")]
  qx <- read_dta(paste0("PATH", loc, "_noshocks_sims.dta")) 
} else {
  data <- as.data.frame(fread(paste0("PATH", output_version_id, "/shocks_", loc_id, ".csv")))
  data <- data[,!colnames(data) %in% "location_id"]
  qx_file <- paste0("PATH", loc_id, ".csv")
  if(file.exists(qx_file)){
    qx <- read.csv(paste0("PATH", loc_id, ".csv"))
  } else{
    qx <- read_dta(paste0("PATH", loc, "_noshocks_sims.dta"))
  }
}
years <- c(1970:2016) # for GBD 2016

#load in draw-level envelope data from hdfs
filepaths_env <- paste0("PATH",env_version,"PATH",years,".h5")
data2 <- data.table(rbindlist(lapply(filepaths_env, load_hdf, by_val = loc_id)))

#load in mx_ax data from hdfs
filepaths_mx_ax <- paste0("PATH",env_version,"PATH",years,".h5") 
lt_orig <- data.table(rbindlist(lapply(filepaths_mx_ax, load_hdf, by_val = loc_id)))

test <- lt_orig[is.na(lt_orig$mx)]
ages <- unique(test$age_group_id)
if(nrow(test)>0) stop(paste0("you have missing mx values in your input dataset for the following ages ", ages, "\n"))


# reshape envelope and lt data wide by draw
data2 <- dcast(data2, location_id + year_id + sex_id + age_group_id ~ paste0("draw_", draw), value.var = "mx_avg_whiv")


## aggregate up the shock deaths to both sexes
## definitly a better way to do this
draws <- grep("draw", names(data), value=T)
agg_shocks <- copy(data)
agg_shocks <- data.table(agg_shocks)
agg_shocks <- melt(agg_shocks, value.name="deaths", id.vars=c("sex_id", "age_group_id", "year_id"), measure.vars=draws, variable.name="draw")
setkey(agg_shocks, age_group_id, year_id, draw)
agg_shocks <- agg_shocks[,list(deaths=sum(deaths)), by=key(agg_shocks)]
agg_shocks <- dcast(agg_shocks, age_group_id+year_id~draw, value.var="deaths")
agg_shocks <- data.frame(agg_shocks)
agg_shocks$sex_id <- 3

data <- rbind(data, agg_shocks)

## rename the draws of the envelope and shocks so that they match 
names(data2)[grep("env",names(data2))] <- gsub("env","draw",names(data2)[grep("env",names(data2))])  ## may not be necessary depending on shocks formatting

## formating the envelope so that it matches the shocks file; dropping "both" sexes
data2 <- data2[,!colnames(data2) %in% c("location_id", "pop")]


#####################################
## Generate the with-shock envelope
#####################################

## drops the aggregate age groups in the non-shock envelope in order to collapse properly 
data2 <- data2[data2$age_group_id %in% c(2:20, 30:32, 235),] #  check that data does NOT contain age_group 21 (80+)... make it below
data  <- data[data$age_group_id != 22,]                          # get rid of all-ages data in the 1970s, 
## collapses data by draws (that is, the first draw of the non shock envelope to the first draw of shocks and so on)

data <- rbind(data, data2)
data <- as.data.table(data)
test_copy <- copy(data)
melt.dt <- melt.data.table(data, id.vars = c("age_group_id","sex_id", "year_id"), variable.name = "draw")
setkey(melt.dt, age_group_id, sex_id, year_id, draw)
# NO LAPPLY HERE: setkey(data, age_group_id, sex_id, year_id)

# cols <- paste0("draw_", 0:999)  # Find out what exactly you are supposed to be summing
# data <- data[, lapply(.SD, sum), by = .(age_group_id, sex_id, year_id), .SDcols = cols]
data <- melt.dt[, .(value = sum(value)), by = key(melt.dt)]
cast.dt <- dcast.data.table(data, age_group_id + sex_id + year_id ~ draw, value.var = "value")
data <- cast.dt

# NO LAPPLY HERE: data <- data[,lapply(.SD, sum), by = key(data)]
data <- as.data.frame(data)


# making age group aggregate for All Ages
for_env <- copy(data)
all_age <- copy(data)
all_age <- melt(all_age, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "env_number")
all_age <- as.data.table(all_age)
setkey(all_age, sex_id, year_id, draw)
all_age <- all_age[,list(env_number=sum(env_number)), by=key(all_age)]
all_age <- as.data.frame(all_age)
all_age$age_group_id <- 22
all_age <- dcast(all_age, age_group_id+year_id+sex_id ~draw, value.var="env_number")

## making age group aggregates under5, 5-14, 15-49, 50-69, 70+, 80+
age_agg <- copy(for_env)
age_agg <- melt(for_env, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "env_number")
under5 <- c(2, 3, 4, 5)
age5to14 <- c(6,7)
age15to49 <- c(8:14)
age50to69 <-c(15:18)
age70plus <- c(19, 20, 30, 31, 32, 235)
age80plus <- c(30:32, 235)
age_agg$age_group_id[age_agg$age_group_id %in% under5] <- 1
age_agg$age_group_id[age_agg$age_group_id %in% age5to14] <- 23
age_agg$age_group_id[age_agg$age_group_id %in% age15to49] <- 24
age_agg$age_group_id[age_agg$age_group_id %in% age50to69] <- 25
age_agg$age_group_id[age_agg$age_group_id %in% age70plus] <- 26
age_agg$age_group_id[age_agg$age_group_id %in% age80plus] <- 21
age_agg <- as.data.table(age_agg)
setkey(age_agg, sex_id, year_id, draw, age_group_id)
age_agg <- age_agg[,list(env_number=sum(env_number)), by=key(age_agg)]
age_agg <- as.data.frame(age_agg)
age_agg <- dcast(age_agg, age_group_id+year_id+sex_id ~draw, value.var="env_number")
for_env <- rbind(for_env, age_agg)

## making age group aggregate for under 1 (couldn't do above because not mutally exclusive with under 5)
under1 <- copy(for_env)
under1 <- melt(for_env, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "env_number")
under1 <- under1[under1$age_group_id %in% c(2,3,4),]
under1 <- as.data.table(under1)
setkey(under1, sex_id, year_id, draw)
under1 <- under1[,list(env_number=sum(env_number)), by=key(under1)]
under1 <- as.data.frame(under1)
under1$age_group_id <- 28
under1 <- dcast(under1, age_group_id+year_id+sex_id ~draw, value.var="env_number")
for_env <- rbind(for_env, under1)
for_env <- rbind(for_env, all_age)
write.csv(for_env, paste0("PATH", loc_id, ".csv"),row.names=F)


env <-  melt(for_env, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "env_number")
env$draw = substr(env$draw, 6, 11)
env <- as.data.table(env)
setkey(env, age_group_id, sex_id, year_id)
env <- env[,list(env_mean = mean(env_number), env_upper=quantile(env_number, probs=0.975), env_lower=quantile(env_number, probs=0.025)), by=key(env)]
env <- as.data.frame(env)
env$ihme_loc_id <- loc
env <- env[,c("ihme_loc_id", "year_id", "sex_id", "age_group_id", "env_lower", "env_mean", "env_upper")] 
write.csv(env, paste0("PATH", loc_id, ".csv"),row.names=F )

##########################################
## Make scalars
## Merge with non-shock LT
## data cleaning
##########################################

## merging the evelope with the envelope+shock to generate scalars
data2 <- melt(data2, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "env_number") # data 2 is envelope; data is (envelope +shock) from above
data <- melt(data, id.vars = c("age_group_id", "sex_id", "year_id"), variable.name = "draw", value.name = "shocks_plus_env_number")
data <- join(data, data2, by=c("age_group_id", "sex_id", "year_id", "draw"), type="full")

stopifnot(!is.na(c(data$env_number, data$shocks_plus_env_number))) ## breaks code if there's not a 1 to 1 match ; 

data$draw = substr(data$draw, 6, 11)
data$shocks_plus_env_number[data$age_group_id %in% c(235, 30, 31, 32) & data$shocks_plus_env_number==0] <- 0.01
data$env_number[data$age_group_id %in% c(235, 30, 31, 32) & data$env_number==0] <- 0.01
data$scalar <- data$shocks_plus_env_number / data$env_number

## bringing in life table flie merging on to scalar file

lt <- copy(lt_orig)
lt <- as.data.frame(lt)
lt <- lt[,!colnames(lt) %in% c("location_id")]

stopifnot(!is.na(data$age_group_id)) ## breaks code if there are missing age_group_ids

## merging LT with scalars and shocks

lt <- join(lt, data, by= c("year_id", "sex_id", "age_group_id", "draw"), type="full") 

###################################################
###################################################
## Generate the With-Shock Lifetable
###################################################
###################################################

########################################################
## (Step 1) NN and Child LT
########################################################

## Subsetting to make Neonatal and 1-4 dataset
lt <- as.data.frame(lt)
lt_nn <- lt[lt$age_group_id==28 | lt$age_group_id==2 | lt$age_group_id==3 | lt$age_group_id==4,]

## some data prep
qx$year <- qx$year-0.5
qx <- qx[,!colnames(qx) %in% c("ihme_loc_id", "q_u5","location_id")]
qx <- qx[qx$year>=1970,]
qx$sex_id[qx$sex=="male"] <- 1
qx$sex_id[qx$sex=="female"] <- 2
qx$sex_id[qx$sex=="both"] <- 3
qx <- rename(qx, c("simulation" = "draw"))
qx <- qx[,!colnames(qx) %in% "sex"]
qx <- melt(qx, id.vars=c("sex_id", "year", "draw"), variable.name="age", value.name="qx")
qx$age <- substr(qx$age, 3,5)
qx <- qx[qx$age!="ch",] 
qx$age_group_id[qx$age=="enn"] <- 2
qx$age_group_id[qx$age=="lnn"] <- 3
qx$age_group_id[qx$age=="pnn"] <- 4
qx <- qx[,!colnames(qx) %in% "age"]
qx <- rename(qx, c("year"="year_id"))
qx <- qx

## merging qx onto the neonatal LT
lt_nn <- lt_nn[,!colnames(lt_nn) %in% c("qx", "age")]
lt_nn <- join(lt_nn, qx, by=c("age_group_id", "sex_id", "draw", "year_id"), type="full")
if(nrow(lt_nn[is.na(lt_nn$qx) & lt_nn$age_group_id!=28,])!=0) stop ("Missing qx values")
lt_nn$mx <- NA
lt_nn$n[lt_nn$age_group_id==2] <- 7/365 
lt_nn$n[lt_nn$age_group_id==3] <- 21/365
lt_nn$n[lt_nn$age_group_id==4] <- 337/365

lt_nn$mx_non_shock <- qx_to_mx(n=lt_nn$n, qx=lt_nn$qx) ## this is the non-shock mx
lt_nn$mx <- lt_nn$scalar * lt_nn$mx_non_shock ## this gives the with-shock mx
lt_nn <- lt_nn[,!colnames(lt_nn) %in% "mx_non_shock"]
lt_nn$qx <- NA
lt_nn$qx <- mx_to_qx(n=lt_nn$n, mx=lt_nn$mx) ## with-shock qx
if(nrow(lt_nn[is.na(lt_nn$qx) & lt_nn$age_group_id!=28,])!=0) stop("Missing qx values")

lt_nn$px <- 1 - lt_nn$qx ## caluclates px
lt_nn <- lt_nn[order(lt_nn$year_id, lt_nn$sex_id, lt_nn$draw, lt_nn$age_group_id),]

if(length(lt_nn[lt_nn$age_group_id==2,])!= length(lt_nn[lt_nn$age_group_id==3,])) stop("Different number of observations per age")
if(length(lt_nn[lt_nn$age_group_id==3,])!= length(lt_nn[lt_nn$age_group_id==28,])) stop("Different number of observations per age")
if(length(lt_nn[lt_nn$age_group_id==28,])!= length(lt_nn[lt_nn$age_group_id==4,])) stop("Different number of observations per age")
stopifnot(is.na(lt_nn$qx[lt_nn$age_group_id==28]))

## Conditional Probabilities to get 1q0
lt_nn$qx[lt_nn$age_group_id==28] <- 1-(lt_nn$px[lt_nn$age_group_id==2] * lt_nn$px[lt_nn$age_group_id==3] * lt_nn$px[lt_nn$age_group_id==4]) ## aggregating enn. lnn, pnn into under1
if(nrow(lt_nn[is.na(lt_nn$qx) & lt_nn$age_group_id==28,])!=0) stop("Missing qx values in <1 age group") 
stopifnot(!is.na(lt_nn$qx[lt_nn$age_group_id==28])) 

## 1q0 to 1m0 calculation
lt_nn$mx[lt_nn$age_group_id==28] <-NA 
lt_nn$n[lt_nn$age_group_id==28] <- 1
lt_nn$mx[lt_nn$age_group_id==28] <- qxax_to_mx(n=lt_nn$n[lt_nn$age_group_id==28], qx=lt_nn$qx[lt_nn$age_group_id==28], ax=lt_nn$ax[lt_nn$age_group_id==28]) ## gives under 1 with shock mx
stopifnot(!is.na(lt_nn$mx[lt_nn$age_group_id==28])) ## breaks code if mx are missing
lt_nn <- lt_nn[,!colnames(lt_nn) %in% "px"]
 
## This is probably redundant but this file is used later
lt_young <- lt_nn[lt_nn$age_group_id %in% c(2,3,4),]
lt_young <- lt_young[,colnames(lt_young) %in% c("year_id", "sex_id", "qx", "draw", "age_group_id", "location_id")]

lt_nn <- lt_nn[lt_nn$age_group_id %in% c(28),]

## Now need to create with-shock mx and qx non neonatal life table so that the under 1 and 1-4 can be appended
lt <- lt[!lt$age_group_id %in% c(28, 2, 3, 4),]

#####################
## Step 2: Adult LT
#####################

if(!loc_id %in% lowest_level$location_id){
  lt <- lt[!lt$age_group_id %in% c(1, 22:26),]
}

# 95+ ##########
scalar_95 <- lt[lt$age_group_id==235,] # use 90-94 for 95+... there is no age_group_ie == 235 in the lt 
scalar_95 <- scalar_95[,colnames(scalar_95) %in% c("sex_id", "year_id", "draw", "scalar")]
scalar_95 <- scalar_95[order(scalar_95$year_id, scalar_95$sex_id, scalar_95$draw),]
scalar_95 <- rename(scalar_95, c("scalar" = "scalar_95"))
lt <- merge(lt, scalar_95, by=c("year_id", "sex_id", "draw"), all.x=T) # this takes a long time
ninetyfive_plus_ages <- c(33,44,45,148)

lt$scalar[(lt$age_group_id %in% ninetyfive_plus_ages) & is.na(lt$scalar)] <- 
    lt$scalar_95[(lt$age_group_id %in% ninetyfive_plus_ages) & is.na(lt$scalar)]
lt <- lt[!colnames(lt) %in% c("scalar_95")]
if(nrow(lt[is.na(lt$scalar),])!=0) stop("Missing scalar values")

lt <- lt[!lt$age_group_id==235,] 

## creates with-shock mx, calculates with-shock qx
lt <- rename(lt, c("mx"="non_shock_mx"))
lt$mx <- NA
lt$mx <- lt$scalar * lt$non_shock_mx
lt <- lt[,!colnames(lt) %in% "non_shock_mx"]
lt$qx <- NA
lt$n[lt$age_group_id!=5] <- 5
lt$n[lt$age_group_id==5] <- 4
lt$qx <- mxax_to_qx(n=lt$n, mx=lt$mx, ax=lt$ax)

if(nrow(lt[is.na(lt$qx),])!=0) stop("Adult qx values are missing")

## append file with with-shock under1 and 1-4 mx and qx to file with the rest of with-shock mx,qx
lt <- rbind(lt, lt_nn)

lt$draw <- as.numeric(lt$draw)
lt <- lt[order(lt$year_id, lt$draw, lt$age_group_id, lt$sex),]
lt <- lt[,colnames(lt) %in% c("year_id", "sex_id", "age_group_id", "draw", "mx", "ax", "qx")]  


##################################
## calcuating with-shock age-sex
##################################

child <- copy(lt)
child <- child[child$age_group_id==5 | child$age_group_id==28,]
child <- child[,!colnames(child) %in% c("mx", "ax")]
child <- rbind(child, lt_young)

## creating q_nn 
nn <- child[child$age_group_id %in% c(2,3),]
nn$px <- 1-nn$qx
nn <- data.table(nn)
setkey(nn,sex_id,year_id,draw)
nn <- as.data.frame(nn[,list(px = prod(px)),by=key(nn)])
nn$qx <- 1 - nn$px
nn$px <- NULL
nn$age_group_id <- 42
child <- rbind(child,nn)

## creating 5q0
u5 <- child[child$age_group_id %in% c(28,5),]
u5$px <- 1-u5$qx
u5 <- data.table(u5)
setkey(u5,sex_id,year_id,draw)
u5 <- as.data.frame(u5[,list(px = prod(px)),by=key(u5)])
u5$qx <- 1 - u5$px
u5$px <- NULL
u5$age_group_id <- 1
child <- rbind(child,u5)

if(nrow(child[is.na(child$qx),])!=0) stop("missing child qx values")

## saving draws
write.csv(child, paste0("PATH", loc_id, ".csv"), row.names=F)


## mean level age-sex
summ <- child
summ <- data.table(summ)
setkey(summ,sex_id,year_id,age_group_id)
summ <- as.data.frame(summ[,list(qx_mean=mean(qx),qx_lower=quantile(qx,probs=c(.025)),qx_upper=quantile(qx,probs=c(.975))),by=key(summ)])

## replacing iwth mean of middle 99% (or 95% if necessary) if upper > mean
if(nrow(summ[summ$qx_mean>summ$qx_upper,])>0){
  high_draws <- copy(summ)
  high_draws <- summ[summ$qx_mean>summ$qx_upper,]
  high_draws <- high_draws[,colnames(high_draws) %in% c("sex_id", "year_id", "age_group_id")] 
  replace <- copy(child)
  replace <- join(high_draws, replace, type="left")
  low <- quantile(replace$qx, .01) 
  high <- quantile(replace$qx, .99)
  replace <- replace[replace$qx>low,]
  replace <- replace[replace$qx<high,]
  replace <- data.table(replace)
  setkey(replace,sex_id,year_id,age_group_id)
  replace <- as.data.frame(replace[,list(qx_mean_new=mean(qx)),by=key(replace)])
  summ <- join(summ, replace)
  summ$qx_mean[!is.na(summ$qx_mean_new)] <- summ$qx_mean_new[!is.na(summ$qx_mean_new)]
  summ <- summ[,!colnames(summ) %in% c("qx_mean_new")]
  # if that wasn't enough, take the middle 80% of what remains.
  if(nrow(summ[summ$qx_mean>summ$qx_upper,])>0) {
      print("After taking the middle 99% of draws, the upper ui is still less than the mean. Taking the middle 95%.")
      high_draws <- copy(summ)
      high_draws <- summ[summ$qx_mean>summ$qx_upper,]
      high_draws <- high_draws[,colnames(high_draws) %in% c("sex_id", "year_id", "age_group_id")]
      replace <- copy(child)
      replace <- join(high_draws, replace, type="left")
      low <- quantile(replace$qx, .025) 
      high <- quantile(replace$qx, .975)
      replace <- replace[replace$qx>low,]
      replace <- replace[replace$qx<high,]
      replace <- data.table(replace)
      setkey(replace,sex_id,year_id,age_group_id)
      replace <- as.data.frame(replace[,list(qx_mean_new=mean(qx)),by=key(replace)])
      summ <- join(summ, replace)
      summ$qx_mean[!is.na(summ$qx_mean_new)] <- summ$qx_mean_new[!is.na(summ$qx_mean_new)]
      summ <- summ[,!colnames(summ) %in% c("qx_mean_new")] 
  } 
  if(nrow(summ[summ$qx_mean>summ$qx_upper,])>0) stop("Even after taking the middle 95% of draws, the upper ui is still less than the mean")
}


##############################################
## Scaling Under 5 to fix consistency issues
##############################################

mean <- copy(summ)
mean$qx_mean[!mean$age_group_id %in% c(2,3,4,5)] <- NA

# model_locs <- get_locations(level="estimate")
model_locs <- read.csv(paste0(root, "PATH"))
if(loc_id %in% model_locs$location_id){
  scale <- read_dta(paste0("PATH", loc, "_scaling_numbers.dta"))
  scale <- melt(scale, id.vars=c("ihme_loc_id", "sex", "year"), measure.vars=grep("scale", names(scale), value=T), variable.name="age", value.name="scale")
  scale$age_group_id[scale$age=="scale_enn"] <- 2
  scale$age_group_id[scale$age=="scale_lnn"] <- 3
  scale$age_group_id[scale$age=="scale_pnn"] <- 4
  scale$age_group_id[scale$age=="scale_ch"] <- 5
  
  scale$sex_id[scale$sex=="male"] <- 1
  scale$sex_id[scale$sex=="female"] <- 2
  scale$sex_id[scale$sex=="both"] <- 3
  scale$year_id <- floor(scale$year)
  scale <- scale[,!colnames(scale) %in% c("ihme_loc_id", "sex", "year", "age")]
  mean <- merge(mean, scale, by=c("year_id", "sex_id", "age_group_id"), all.x=T)
  if(nrow(mean[is.na(mean$scale) & mean$age_group_id %in% c(2,3,4,5),])!=0) stop("Missing scalar values under 5 lt")
  
  ## scaling with shock most specific age group qx
  mean$qx_mean[mean$age_group_id %in% c(2,3,4,5)] <- mean$qx_mean[mean$age_group_id %in% c(2,3,4,5)] * mean$scale[mean$age_group_id %in% c(2,3,4,5)]
}

## using conditional probabilities to calculate 5q0, q_ch and q_nn
mean$px <- 1 -mean$qx_mean

## under 1
mean_inf <-mean[mean$age_group_id %in% c(2,3,4),]
mean_inf <- data.table(mean_inf)
setkey(mean_inf, year_id, sex_id)
mean_inf <- mean_inf[,c("qx_mean", "qx_upper", "qx_lower", "scale"):=NULL]
mean_inf <- mean_inf[,list(px=prod(px)), by=key(mean_inf)]
mean_inf$qx_inf <- 1-mean_inf$px
mean_inf <- mean_inf[,px:=NULL]

mean <- merge(mean, mean_inf, by=c("year_id","sex_id"), all=T)
mean$qx_mean[mean$age_group_id==28] <- mean$qx_inf[mean$age_group_id==28]
mean$qx_inf <- NULL

## neonatal 
mean_nn <-mean[mean$age_group_id %in% c(2,3),]
mean_nn <- data.table(mean_nn)
setkey(mean_nn, year_id, sex_id)
mean_nn <- mean_nn[,c("qx_mean", "qx_upper", "qx_lower", "scale"):=NULL]
mean_nn <- mean_nn[,list(px=prod(px)), by=key(mean_nn)]
mean_nn$qx_nn <- 1-mean_nn$px
mean_nn <- mean_nn[,px:=NULL]

mean <- merge(mean, mean_nn, by=c("year_id","sex_id"), all=T)
mean$qx_mean[mean$age_group_id==42] <- mean$qx_nn[mean$age_group_id==42]
mean$qx_nn <- NULL

## 5q0
mean_u5 <-mean[mean$age_group_id %in% c(28,5),]
mean_u5$px <- 1-mean_u5$qx_mean
mean_u5 <- data.table(mean_u5)
setkey(mean_u5, year_id, sex_id)
mean_u5 <- mean_u5[,c("qx_mean", "qx_upper", "qx_lower", "scale"):=NULL]
mean_u5 <- mean_u5[,list(px=prod(px)), by=key(mean_u5)]
mean_u5$qx_u5 <- 1-mean_u5$px
mean_u5 <- mean_u5[,px:=NULL]

mean <- merge(mean, mean_u5, by=c("year_id","sex_id"), all=T)
mean$qx_mean[mean$age_group_id==1] <- mean$qx_u5[mean$age_group_id==1]

mean <- mean[,!colnames(mean) %in% c("scale", "px", "qx_u5")]

if(nrow(mean[is.na(mean$qx_mean),])>0) stop("Missing qx values age-sex")

mean$ihme_loc_id <- loc
############

write.csv(mean,paste0("PATH",loc_id,"_summary.csv"),row.names=F)

###############################################

############# calculating 45q15################
# 
adult <- copy(lt)
adult <- adult[adult$age_group_id %in% 8:16,]
adult$px <- 1 - adult$qx
adult <- data.table(adult)
setkey(adult, sex_id, year_id, draw)
adult <- adult[,list(px=prod(px)), by=key(adult)]
adult[,qx_adult:=1-px]
setkey(adult, year_id, sex_id)
adult <- adult[,list(qx_adult_mean = mean(qx_adult), qx_adult_upper=quantile(qx_adult, probs=0.975), qx_adult_lower=quantile(qx_adult, probs=0.025)), by=key(adult)]
adult <- data.frame(adult)
adult$ihme_loc_id <- loc
write.csv(adult, paste0("PATH", loc_id, ".csv"),row.names=F)

################################################

## Applying the lifetable function to generate the rest of the lt
## making the format for the lifetable function

# lt_ages <- get_age_map(type="lifetable")
# lt_ages <- lt_ages[,colnames(lt_ages) %in% c("age_group_id", "age_group_name_short")]
lt_ages <- read.csv(paste0(root, "temp/USER/ages.csv"))
lt_ages <- rename(lt_ages, c("age_group_name_short"="age"))
lt <- join(lt, lt_ages, by="age_group_id")
lt <- rename(lt, c("year_id"="year", "draw"="id"))
lt$sex[lt$sex_id==1] <- "male"
lt$sex[lt$sex_id==2] <- "female"
lt$sex[lt$sex_id==3] <- "both"
lt$age <- as.numeric(lt$age)
     
lt <- lt[!colnames(lt) %in% c("pop", "n", "sex_id", "age_group_id")]
lt <- as.data.frame(lt)
lt_full <- lifetable(data=lt, preserve_u5 = 1, output_anyway=T) 

if(nrow(lt_full[lt_full$qx>=1 & lt_full$age!=110,]) > 0){
  check_n <- nrow(lt_full) 
  fix <- lt_full[lt_full$qx>=1 & lt_full$age!=110,]
  
  fix$qx <- NULL
  fix$qx <- 1-exp(-fix$n * fix$mx)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (1)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx * fix$n * (fix$qx-1))) / (fix$mx * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (1)")
  
  lt_full <- lt_full[!(lt_full$qx>=1 & lt_full$age!=110),] 
  lt_full <- rbind(lt_full, fix)
  if(nrow(lt_full)!=check_n) stop("your fix resulted in a different number of rows than expected (1)")
}

lt_full <- lifetable(data=lt_full, preserve_u5 = 1)
if(nrow(lt_full[lt_full$qx>1,])>0) stop("after your qx fix, there are still over 1 qx values")

if(nrow(lt_full[lt_full$ax<0,]) > 0){
  check_n <- nrow(lt_full) 
  fix <- lt_full[lt_full$ax<0,]
  if(0 %in% unique(fix$age) | 5 %in% unique(fix$age)) stop ("you have bigger problems, under 5 ax values are negative")
  
  fix$qx <- NULL
  fix$qx <- 1-exp(-fix$n * fix$mx)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (1)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx * fix$n * (fix$qx-1))) / (fix$mx * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (1)")
  
  lt_full <- lt_full[!(lt_full$ax<0),] 
  lt_full <- rbind(lt_full, fix)
  if(nrow(lt_full)!=check_n) stop("your fix resulted in a different number of rows than expected (1)")
}

if(nrow(lt_full[lt_full$ax<0,])>0) stop("after your qx fix, you introduced nonsense ax values ")


lt_full <- rename(lt_full, c("id"="draw", "year"="year_id"))
lt_full <- lt_full[, !duplicated(colnames(lt_full))] 

## calculating life expectancy uncertainty intervals#############
le <- copy(lt_full)
le$sex_id[le$sex=="male"] <- 1
le$sex_id[le$sex=="female"] <- 2
le$sex_id[le$sex=="both"] <- 3
le <- le[le$age %in% c(0, 50, 65), colnames(le) %in% c("draw", "age", "year_id", "sex_id", "ex")]
le <- data.table(le)
setkey(le, year_id, sex_id, age)
le <- le[,list(ex_upper=quantile(ex, probs=0.975), ex_lower=quantile(ex, probs=0.025)), by=key(le)]
#############################################

## and now going back to the standard variables....
lt_full <- join(lt_full, lt_ages, by="age")
lt_full$sex_id[lt_full$sex=="male"] <- 1
lt_full$sex_id[lt_full$sex=="female"] <- 2
lt_full$sex_id[lt_full$sex=="both"] <- 3
lt_full <- lt_full[,!colnames(lt_full) %in% c("age", "sex")]

## adding location variable
lt_full$location_id <- loc_id

## writing draw level lifetables
write.csv(lt_full, paste0("PATH", loc_id, ".csv"),row.names=F )


## Summary Lt file

## swapping in scaled 1q0 and 4q1 values instead of having them be values of the MLT
q28 <- summ[summ$age_group_id==28,]  ## 1q0
q28 <- q28[,!colnames(q28) %in% "location_id"]
q28 <- rename(q28, c("qx_mean"="qx28"))
q28 <- q28[,!colnames(q28) %in% c("qx_upper", "qx_lower")]

q14 <- summ[summ$age_group_id==5,] ## 4q1
q14 <- q14[,!colnames(q14) %in% "location_id"]
q14 <- rename(q14, c("qx_mean"="qx14"))
q14 <- q14[,!colnames(q14) %in% c("qx_upper", "qx_lower")]


lt_sum <- copy(lt_full)
lt_sum <- as.data.table(lt_sum)
setkey(lt_sum, age_group_id, sex_id, year_id)
lt_sum <- lt_sum[,list(mx = mean(mx),
                       ax = mean(ax),  qx=mean(qx)), by=key(lt_sum)]

lt_sum <- join(lt_sum, q28[,c("sex_id","year_id","age_group_id","qx28")], by=c("age_group_id", "sex_id", "year_id"))
lt_sum <- join(lt_sum, q14[,c("sex_id","year_id","age_group_id","qx14")], by=c("age_group_id", "sex_id", "year_id"))

lt_sum$qx[lt_sum$age_group_id==28] <- lt_sum$qx28[lt_sum$age_group_id==28]
lt_sum$qx[lt_sum$age_group_id==5] <- lt_sum$qx14[lt_sum$age_group_id==5]

lt_sum <- as.data.frame(lt_sum)
lt_sum <- lt_sum[,!colnames(lt_sum) %in% c("qx28", "qx14")]

lt_sum$id <- 1
lt_sum <- rename(lt_sum, c("year_id"="year"))
lt_sum <- join(lt_sum, lt_ages, by="age_group_id")

lt_sum$sex[lt_sum$sex_id==1] <- "male"
lt_sum$sex[lt_sum$sex_id==2] <- "female"
lt_sum$sex[lt_sum$sex_id==3] <- "both"
lt_sum <- lt_sum[!colnames(lt_sum) %in% c("sex_id", "age_group_id")]
lt_sum$age <- as.numeric(lt_sum$age)

lt_sum <- lifetable(data=lt_sum, preserve_u5 =1) 
lt_sum <- lt_sum[,!colnames(lt_sum) %in% "id"]

lt_sum$sex_id[lt_sum$sex=="male"] <- 1
lt_sum$sex_id[lt_sum$sex=="female"] <- 2
lt_sum$sex_id[lt_sum$sex=="both"] <- 3
lt_sum <- lt_sum[,!colnames(lt_sum) %in% "sex"]

## getting the le from the mean life table#############
le_mean <- copy(lt_sum)
le_mean <- rename(le_mean, c("ex"= "ex_mean", "year"="year_id"))
if(loc=="AFG"){
  write.csv(le_mean, paste0(root, "PATH"), row.names=F)
}
le_mean <- le_mean[le_mean$age %in% c(0, 50, 65), colnames(le_mean) %in% c("age", "year_id", "sex_id", "ex_mean")]
le <- join(le, le_mean, type="full", by=c("age", "year_id", "sex_id")) ## CHECK TO MAKE SURE THIS HAS IHME LOC ID
le$ihme_loc_id <- loc

write.csv(le, paste0("PATH", loc_id, ".csv"),row.names=F)

## and now going back to the standard variables....
lt_sum <- join(lt_sum, lt_ages, by="age")
lt_sum <- lt_sum[,!colnames(lt_sum) %in% c("age", "sex")]
lt_sum$ihme_loc_id <- loc
lt_sum$location_id <- loc_id

write.csv(lt_sum, paste0("PATH", loc_id, ".csv"), row.names=F )


