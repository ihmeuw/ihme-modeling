

rm(list=ls()); library(foreign); library(haven);library(reshape2);library(data.table)
if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- "FILEPATH"
  workdir <-  paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/lt_functions.R")
  source("FILEPATH/get_locations.r")
  loc <- "ZAF"
  in_hiv <- paste0("FILEPATH/temp/")
  in_nhiv <- paste0("FILEPATH/temp/")
  out_hiv <- paste0("FILEPATH/prematch/")
  out_nhiv <- paste0("FILEPATH/hiv_free/")
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  workdir <- paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/lt_functions.R")
  source("FILEPATH/get_locations.r")
  loc <- commandArgs()[3]
  in_hiv <- commandArgs()[4]
  in_nhiv <- commandArgs()[5]
  out_hiv <- commandArgs()[6]
  out_nhiv <- commandArgs()[7]
}

locs <- get_locations(level="estimate")

dhiv <- list()
for (param in c("ax","mx","qx")) {
  dhiv[[param]] <- fread(paste0(in_hiv,"/lt_",param,"_",loc,".csv"))
  dhiv[[param]] <- melt(dhiv[[param]],id.vars=c("age","sex","year","ihme_loc_id"))
  dhiv[[param]] <- as.data.frame(dhiv[[param]])
  names(dhiv[[param]])[names(dhiv[[param]])=="variable"] <- "draw"
  names(dhiv[[param]])[names(dhiv[[param]])=="value"] <- param
  
  dhiv[[param]]$draw <- gsub(param,"",dhiv[[param]]$draw)
  dhiv[[param]]$draw <- as.integer(dhiv[[param]]$draw)
  stopifnot(!length(dhiv[[param]]$draw[is.na(dhiv[[param]]$draw)])>0)
  dhiv[[param]]$id <- NULL
  cat(paste0(param," done \n")); flush.console()
}


input <- merge(dhiv[["ax"]],dhiv[["mx"]],by=c("ihme_loc_id","sex","year","age","draw"),all=T)
input <- merge(input,dhiv[["qx"]],by=c("ihme_loc_id","sex","year","age","draw"),all=T)
dhiv <- input

dnhiv <- list()
for (param in c("ax","mx","qx")) {
  dnhiv[[param]] <- fread(paste0(in_nhiv,"/lt_",param,"_",loc,".csv"))
  dnhiv[[param]] <- melt(dnhiv[[param]],id.vars=c("age","sex","year","ihme_loc_id"))
  dnhiv[[param]] <- as.data.frame(dnhiv[[param]])
  names(dnhiv[[param]])[names(dnhiv[[param]])=="variable"] <- "draw"
  names(dnhiv[[param]])[names(dnhiv[[param]])=="value"] <- param
  
  dnhiv[[param]]$draw <- gsub(param,"",dnhiv[[param]]$draw)
  dnhiv[[param]]$draw <- as.integer(dnhiv[[param]]$draw)
  stopifnot(!length(dnhiv[[param]]$draw[is.na(dnhiv[[param]]$draw)])>0)
  dnhiv[[param]]$id <- NULL
  cat(paste0(param," done \n")); flush.console()
}


input <- merge(dnhiv[["ax"]],dnhiv[["mx"]],by=c("ihme_loc_id","sex","year","age","draw"),all=T)
input <- merge(input,dnhiv[["qx"]],by=c("ihme_loc_id","sex","year","age","draw"),all=T)
dnhiv <- input

dnhiv$age <- as.numeric(dnhiv$age)
dhiv$age <- as.numeric(dhiv$age)
dnhiv$ihme_loc_id <- as.character(dnhiv$ihme_loc_id)
dhiv$ihme_loc_id <- as.character(dhiv$ihme_loc_id)
dnhiv$sex <- as.numeric(dnhiv$sex)

dhiv <- dhiv[order(dhiv$ihme_loc_id,dhiv$draw,dhiv$sex,dhiv$age),]
dnhiv <- dnhiv[order(dnhiv$ihme_loc_id,dnhiv$draw,dnhiv$sex,dnhiv$age),]
if (dim(dhiv)[1] != dim(dnhiv)[1]) stop("wrong dimensions")
if (nrow(dhiv[dhiv$mx < (dnhiv$mx-.0000001),]) > 0) stop("potential issues with hiv-free being above with-hiv")

as <- read_dta(paste0("FILEPATH",loc,"_noshocks_sims.dta"))
names(as)[names(as)=="simulation"] <- "draw"
as$year <- as$year - .5
as$q_inf <- 1 - ((1-as$q_enn)*(1-as$q_lnn)*(1-as$q_pnn))
as <- as[,c("ihme_loc_id","year","draw","sex","q_inf","q_ch")]
as$sex[as$sex == "male"] <- "1"
as$sex[as$sex == "female"] <- "2"
as$sex[as$sex == "both"] <- "3"
as$sex <- as.numeric(as$sex)
as <- data.table(as)
as <- melt(as,id.vars=c("ihme_loc_id","year","draw","sex"))
setnames(as,c("variable","value"),c("age","qx_as"))
as <- as.data.frame(as)
as$age <- as.character(as$age)
as$age[as$age == "q_inf"] <- "0"
as$age[as$age == "q_ch"] <- "1"
as$age <- as.numeric(as$age)


dim1 <- dim(dhiv)[1]
dhiv <- merge(dhiv,as,all.x=T,by=c("ihme_loc_id","year","draw","sex","age"))
if (dim1 != dim(dhiv)[1]) stop("Merge created different rows")
dhiv$age <- as.numeric(dhiv$age)
dhiv <- dhiv[order(dhiv$ihme_loc_id,dhiv$draw,dhiv$year,dhiv$sex,dhiv$age),]
dhiv$n <- unlist(tapply(dhiv$age, list(dhiv$ihme_loc_id,dhiv$sex,dhiv$year), function(x) c(x[-1],max(x)) - x ))
dhiv$mx_as <- dhiv$qx_as/(dhiv$n-dhiv$n*dhiv$qx_as+dhiv$qx_as*dhiv$ax)
dhiv$rat <- dhiv$mx_as/dhiv$mx

dnhiv <- merge(dnhiv,dhiv[,c("ihme_loc_id","year","draw","sex","age","rat")],all.x=T,by=c("ihme_loc_id","year","draw","sex","age"))
if (dim1 != dim(dnhiv)[1]) stop("Merge created different rows")
dnhiv$mx[dnhiv$age < 5] <- dnhiv$rat[dnhiv$age < 5]*dnhiv$mx[dnhiv$age < 5]
dnhiv$age <- as.numeric(dnhiv$age)
dnhiv <- dnhiv[order(dnhiv$ihme_loc_id,dnhiv$draw,dnhiv$year,dnhiv$sex,dnhiv$age),]
dnhiv$n <- unlist(tapply(dnhiv$age, list(dnhiv$ihme_loc_id,dnhiv$sex,dnhiv$year), function(x) c(x[-1],max(x)) - x ))
dnhiv$qx <- dnhiv$n*dnhiv$mx/(1+(dnhiv$n-dnhiv$ax)*dnhiv$mx)


dhiv$mx[dhiv$age < 5] <- dhiv$mx_as[dhiv$age < 5]
dhiv$qx[dhiv$age < 5] <- dhiv$qx_as[dhiv$age < 5]


dhiv$qx_as <- dhiv$mx_as <- dhiv$rat <- dhiv$n <- NULL
dnhiv$rat <- dnhiv$n <- NULL

## use lt function to make full lifetable
dhiv$id <- paste0(dhiv$ihme_loc_id,"-",dhiv$draw)
whiv <- lifetable(dhiv,preserve_u5=1)
whiv$id <- whiv$ihme_loc_id <- NULL


## use lt function to make full lifetable
dnhiv$id <- paste0(dnhiv$ihme_loc_id,"-",dnhiv$draw)
nhiv <- lifetable(dnhiv,preserve_u5=1)
nhiv$id <- nhiv$ihme_loc_id <- NULL

## save hiv-free

write.csv(nhiv,paste0(out_nhiv,"/lt_",loc,".csv"),row.names=F)


whiv$ihme_loc_id <- loc
whiv <- merge(whiv,locs[,c("ihme_loc_id","location_id")],by="ihme_loc_id",all.x=T)
whiv$sex_id <- as.numeric(whiv$sex)
whiv <- whiv[,c("ihme_loc_id","year","draw","sex","age","ax","mx","qx","location_id","sex_id")]

pop <- data.frame(fread("FILEPATH/lt_pop.csv"))
pop <- pop[pop$location_id %in% unique(whiv$location_id) & pop$sex_id %in% unique(whiv$sex_id),]

setnames(pop, "population", "pop")
pop <- pop[,!colnames(pop) %in% "process_version_map_id"]
pop <- merge(pop,locs[,c("ihme_loc_id","location_id")],by="location_id",all.x=T)
ages <- get_age_map(type="all")
pop <- merge(pop, ages[,c("age_group_id", "age_group_name")], by="age_group_id", all.x=T)

keep_ages <- c(5:20, 28, 30:32, 235)
pop <- pop[pop$age_group_id %in% keep_ages, 
           c("pop","sex_id","age_group_name","age_group_id","ihme_loc_id","year_id")]
pop[pop$age_group_name =="<1 year",]$age_group_name <- "0 to 1"
pop[pop$age_group_name =="95 plus",]$age_group_name <- "95"
pop$age <- as.numeric(sub(' .*', '', pop$age_group_name))

old <- pop[pop$age == 95,]
for(age in seq(100, 110, 5)){
  temp <- old 
  temp$age <- age
  pop <- rbind(pop, temp)
}

pop <- pop[,c("pop", "sex_id", "year_id", "age", "ihme_loc_id")]

source(paste0(root,"FILEPATH/get_age_map.r"))
agemap <- get_age_map("lifetable")
names(agemap)[names(agemap)=="age_group_name_short"] <- "age"

setnames(pop, "year_id", "year")

whiv <- merge(whiv,pop,by=c("age","sex_id","ihme_loc_id","year"),all.x=T)
if (nrow(whiv[is.na(whiv$pop),]) > 0) stop("missing pops")

whiv$death <- whiv$mx*whiv$pop
whiv$ax_w <- whiv$ax*whiv$death

both <- data.table(whiv)
setkey(both,age,location_id,year,draw,ihme_loc_id)
both <- as.data.frame(both[,list(death=sum(death),ax=sum(ax_w),pop=sum(pop)),by=key(both)])
both$mx <- both$death/both$pop
both$ax <- both$ax/both$death
both$death <- NULL
both$pop <- NULL
both$sex <- 3

both <- merge(both,as,all.x=T,by=c("ihme_loc_id","year","draw","sex","age"))
both$age <- as.numeric(both$age)
both <- both[order(both$ihme_loc_id,both$draw,both$year,both$sex,both$age),]
both$n <- unlist(tapply(both$age, list(both$ihme_loc_id,both$sex,both$year), function(x) c(x[-1],max(x)) - x ))

both$qx <- both$n*both$mx/(1+((both$n-both$ax)*both$mx))
both$qx[both$age <5] <- both$qx_as[both$age <5]
both$mx_as <- both$qx_as/(both$n-both$n*both$qx_as+both$qx_as*both$ax)
both$mx[both$age <5] <- both$mx_as[both$age < 5]
both$mx_as <- both$qx_as <- both$n <- both$pop <- both$death <- NULL

## now append both to each sex
whiv$death <- whiv$pop <- whiv$ax_w <- whiv$sex_id <- NULL
whiv <- rbind(whiv,both)

## use lt function to make full lifetable
whiv$draw <- as.numeric(whiv$draw)
whiv$location_id <- NULL
whiv$id <- paste0(whiv$ihme_loc_id,"-",whiv$draw)
final <- lifetable(whiv,preserve_u5=1) 
final$id <- final$ihme_loc_id <- NULL

write.csv(final,paste0(out_hiv,"/lt_",loc,".csv"),row.names=F)




