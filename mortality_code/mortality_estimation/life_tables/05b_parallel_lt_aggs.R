
rm(list=ls()); library(foreign); library(haven); library(data.table); library(plyr)
if (Sys.info()[1] == 'Windows') {
  username <- "FILEPATH"
  root <- "FILEPATH"
  workdir <-  paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/get_locations.r")
  sim <- 1
  test <- T
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  workdir <- paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/get_locations.r")
  sim <- as.integer(commandArgs()[3])
  test <- F
  print(sim)
}

in_dir <- "FILEPATH"
map_dir <- "FILEPATH"
out_dir <- "FILEPATH"

if(test==T){
  in_dir <- "FILEPATH"
  map_dir <- in_dir
  out_dir <- "FILEPATH"
}
source(paste0(root,"FILEPATH/lt_functions.R"))
source(paste0(root, "FILEPATH/get_age_map.r"))
source(paste0(root, "FILEPATH/get_population.R"))

super_map <- read.csv(paste0(map_dir,"/super_map.csv"),stringsAsFactors=F)
region_map <- read.csv(paste0(map_dir,"/region_map.csv"),stringsAsFactors=F)
lowest_map <- read.csv(paste0(map_dir,"/lowest_map.csv"),stringsAsFactors=F)
age_map <- read.csv(paste0(map_dir, "/agemap.csv"), stringsAsFactors=F)

pop <- fread( "FILEPATH/lt_pop.csv")

pop_locs <- copy(lowest_map)
pop_locs <- pop_locs[,names(pop_locs) %in% c("ihme_loc_id", "location_id")]
pop <- merge(pop, pop_locs, by="location_id")

pop <- merge(pop, age_map, by="age_group_id")
setnames(pop, "population", "pop")
pop <- as.data.frame(pop)

pop <- pop[,names(pop) %in% c("pop","sex_id","age_group_name","age_group_id","ihme_loc_id","year_id")]
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

pop$sex <- as.character(pop$sex)
pop[pop$sex=="1",]$sex <- "male"
pop[pop$sex=="2",]$sex <- "female"
setnames(pop, "year_id", "year")

regs <- read.csv("FILEPATH/base_map.csv")
regs <- unique(regs$region_id)

lt <- list()
missing_files <- c()
for (s in c("male","female")) {
  for (r in regs) {
    file <- paste0(in_dir,"/",s,"_",r,"/LT_sim_withhiv_withsim_",r,"_",s,"_",sim,".dta")
    if(file.exists(file)){
      lt[[paste0(s,r)]] <- as.data.table(read_dta(paste0(in_dir,"/",s,"_",r,"/LT_sim_withhiv_withsim_",r,"_",s,"_",sim,".dta")))
    } else {
      missing_files <- c(missing_files, file)
    }
  }
}

if(length(missing_files)>0) stop("With HIV lt sim files are missing.")
lt <- rbindlist(lt, use.names=T)

setkey(lt, ihme_loc_id, sex, year, age)
lt <- lt[order(ihme_loc_id,sex,year,age),]


ifelse(any(is.na(lt$mx)), stop("There are missing mx values"), print("All rows have mx values"))

names(lt)[names(lt)=="ax_hivfree"] <- "ax"
lt <- lt[,c("ihme_loc_id", "sex", "year", "age", "mx", "ax", "nn", "qx_adj"), with = F]
names(lt)[names(lt) == "qx_adj"] <- "qx"
lt$age <- as.numeric(lt$age)

lt <- merge(lt, pop, by=c("age", "sex", "ihme_loc_id", "year"), all.x=T)

missing <- lt[is.na(lt$pop),]
if(nrow(lt[is.na(lt$pop),])!=0) stop(paste0("There are missing population values before scaling in locations ", unique(missing$locations),
                                            "for age groups ", unique(missing$age)), " for sexes ", unique(missing$sex))



lt$mx_scaled <- lt$mx 
n <- nrow(lt)

locs <- read.csv("FILEPATH/base_map.csv")
locs <- locs[,c("ihme_loc_id", "level", "parent_id", "location_id")]

locs <- locs[locs$ihme_loc_id!="CHN",]

china_exeptions <- c("CHN_354", "CHN_361")
locs[locs$ihme_loc_id %in% china_exeptions,]$level <- 3

lt <- merge(lt, locs, all.x =T, by="ihme_loc_id")
if(nrow(lt[is.na(lt$parent_id),])!=0) stop("There are missing parent ids before scaling")

ZAF <- lt[grepl("ZAF", ihme_loc_id),]
lt <- lt[!grepl("ZAF", ihme_loc_id),]

#####################
## four to three
###################

## save the level three values in a set
three <- lt[level==3,]
three <- three[,c("location_id", "sex", "year", "age", "mx_scaled", "ihme_loc_id"), with=F]
three <- setnames(three, old = c("location_id", "mx_scaled", "ihme_loc_id"),
                  new= c("parent_id", "parent_mx", "parent_ihme_loc_id"))

## save the level four values in a set
four <- lt[level==4,]

## collapse 4 mx by population weight
four <- four[,list(agg_mx_4 = weighted.mean(mx_scaled, w =pop)), by = list(age, sex, year, parent_id)]

# merge three onto 4 by parent id
four <- merge(four, three, by=c("parent_id", "sex", "age", "year"))

# create scalar
four$scale_4 <- four$parent_mx/four$agg_mx_4

## actually scale the 4's to the 3's
lt <- merge(lt,four, by=c("parent_id", "sex", "year", "age"), all.x=T) 
lt$parent_ihme_loc_id <- NULL
lt$parent_mx <- NULL
lt[!is.na(scale_4),]$mx_scaled <- lt[!is.na(scale_4),]$mx_scaled*lt[!is.na(scale_4),]$scale_4

#####################
## five to four
####################

## scale the level 5's to the level 4's
## get scaled 4 values 
four <- lt[level==4,]
four <- four[,c("location_id", "sex", "year", "age", "mx_scaled", "ihme_loc_id"), with=F]
four <- setnames(four, old = c("location_id", "mx_scaled", "ihme_loc_id"),
                 new= c("parent_id", "parent_mx", "parent_ihme_loc_id"))


## save the level five values in a set
five <- lt[level==5,]

## collapse 5 mx by population weight
five <- five[,list(agg_mx_5 = weighted.mean(mx_scaled, w =pop)), by = list(age, sex, year, parent_id)]

# merge four onto 5 by parent id
five <- merge(five, four, by=c("parent_id", "sex", "age", "year"))

# create scalar
five$scale_5 <- five$parent_mx/five$agg_mx_5

## actually scale the 5's to the 4's
lt <- merge(lt,five, by=c("parent_id", "sex", "year", "age"), all.x=T) 
lt$parent_ihme_loc_id <- NULL
lt$parent_mx <- NULL

lt[!is.na(scale_5),]$mx_scaled <- lt[!is.na(scale_5),]$mx_scaled*lt[!is.na(scale_5),]$scale_5

##################
## six to five
#################

## get scaled 5 values
five <- lt[level==5,]
five <- five[,c("location_id", "sex", "year", "age", "mx_scaled", "ihme_loc_id"), with=F]
five <- setnames(five, old = c("location_id", "mx_scaled", "ihme_loc_id"),
                 new= c("parent_id", "parent_mx", "parent_ihme_loc_id"))

## save scaled six values
six <- lt[level==6,]

## collapse 6 mx by population weight
six <- six[,list(agg_mx_6 = weighted.mean(mx_scaled, w =pop)), by = list(age, sex, year, parent_id)]

## merge five onto 6 by parent id
six <- merge(six, five, by=c("parent_id", "sex", "age", "year"))

## create scalar
six$scale_6 <- six$parent_mx/six$agg_mx_6

## scale the 6's up to the 5's
lt <- merge(lt,six, by=c("parent_id", "sex", "year", "age"), all.x=T) 
lt$parent_ihme_loc_id <- NULL
lt$parent_mx <- NULL

lt[!is.na(scale_6),]$mx_scaled <- lt[!is.na(scale_6),]$mx_scaled*lt[!is.na(scale_6),]$scale_6

#####  validity checks

## check that all level 4 locations have scale 

ifelse(any(is.na(lt[level==4 & ihme_loc_id != "CHN_44533",]$scale_4)), 
       stop("Not all level 4 locations have a scale!"), print("All level 4 locations have a scale"))

## check that all level 5 locations have a scale 
ifelse(any(is.na(lt[level==5,]$scale_5)), stop("Not all level 5 locations have a scale!"), 
       print("All level 5 locations have a scale"))

## check that all level 6 locations have a scale 
ifelse(any(is.na(lt[level==6,]$scale_6)), stop("Not all level 6 locations have a scale!"), 
       print("All level 6 locations have a scale"))

## calculate overall scalars to be used for without hiv mx
lt <- rbind(lt, ZAF, fill=T)

# check that there are the same number of rows in lt now as in the beginning

ifelse(n==nrow(lt), print("the life table has the correct number of rows"),
       stop("STOP: the life table does not have the correct number of rows!"))

lt$scale_final <- lt$mx_scaled/lt$mx

scale <- lt[,c("sex", "year", "scale_final", "ihme_loc_id", "age"), with=F]


final <- lt
final$n <- final$nn
final$nn <- NULL

## calculate qx from mx
final$qx <-  (final$n*final$mx_scaled)/(1+(final$n-final$ax)*final$mx_scaled)

if(nrow(final[(final$qx>=1 & final$age!=110),]) > 0){
  check_n <- nrow(final) 
 
  fix <- final[(final$qx>=1 & final$age!=110) ,]

  fix$qx <- NULL
  fix$qx <- 1- exp(-fix$n * fix$mx_scaled)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (1)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx_scaled * fix$n * (fix$qx-1))) / (fix$mx_scaled * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (1)")
  final <- final[!(final$qx>=1 & final$age!=110),] 
  
  final <- rbind(final, fix)
  if(nrow(final)!=check_n) stop("your fix resulted in a different number of rows than expected (1)")
}

if(nrow(final[final$ax<0])>0){
  check_n = nrow(final)
  fix <- final[final$ax<0,]
  
  fix$qx <- NULL
  fix$qx <- 1- exp(-fix$n * fix$mx_scaled)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (1)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx_scaled * fix$n * (fix$qx-1))) / (fix$mx_scaled * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (1)")
  final <- final[!final$ax<0]
  
  final <- rbind(final, fix)
  if(nrow(final)!=check_n) stop("your fix resulted in a different number of rows than expected (1)")
  
}

if(nrow(final[final$qx>1 & final$age!=110,]) > 0) stop("your qx/ax fix didn't actually work (1)")
if(nrow(final[final$ax<0,]) > 0) stop("your qx/ax fix didn't actually work (1)")


final[age==110,]$qx <- 1

final <- final[,c("sex", "year", "ihme_loc_id", "age", "mx_scaled", "ax", "n", "qx"), with = F]
setnames(final, old = "mx_scaled", new="mx")


write.csv(final, paste0(out_dir, "/LT_sim_withhiv_withsim_", sim, ".csv"),row.names=F)

final_save <- final
eightyplus <- NULL
five <- NULL
four <- NULL
lowest_map <- NULL
lt <- NULL
region_map <- NULL
super_map <- NULL
temp <- NULL
three <- NULL
final <- NULL


lt <- list()
missing_files <- c()
for (s in c("male","female")) {
  for (r in regs) {
    file <- paste0(in_dir,"/",s,"_",r,"/LT_sim_nohiv_",r,"_",s,"_",sim,".dta")
    if(file.exists(file)){
      lt[[paste0(s,r)]] <- as.data.table(read_dta(paste0(in_dir,"/",s,"_",r,"/LT_sim_nohiv_",r,"_",s,"_",sim,".dta")))
    } else {
      missing_files <- c(missing_files, file)
    }
  }
}
if(length(missing_files)>0) stop("Without HIV lt sim files are missing.")
lt <- rbindlist(lt)



setkey(lt, ihme_loc_id, sex, year, age)
lt <- lt[order(ihme_loc_id,sex,year,age),]


ifelse(any(is.na(lt$mx)), stop("There are missing mx values"), print("All rows have mx values"))

## restrict to relevant values

lt <- lt[,c("ihme_loc_id", "sex", "year", "age", "mx", "ax", "nn", "qx_adj"), with = F]
names(lt)[names(lt) == "qx_adj"] <- "qx"
lt$age <- as.numeric(lt$age)

lt <- merge(lt, pop, by=c("age", "sex", "ihme_loc_id", "year"), all.x=T)

lt <- merge(lt, scale, by=c("year", "age", "ihme_loc_id", "sex"), all.x=T)

lt$mx_scaled <- lt$mx*lt$scale_final


final <- lt
final$n <- final$nn
final$nn <- NULL


## calculate qx from mx
final$qx <-  (final$n*final$mx_scaled)/(1+(final$n-final$ax)*final$mx_scaled)


if(nrow(final[final$qx>=1 & final$age!=110,]) > 0){
  check_n <- nrow(final) 
   fix <- final[final$qx>=1 & final$age!=110,]
  
  fix$qx <- NULL
  fix$qx <- 1-exp(-fix$n * fix$mx_scaled)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (2)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx_scaled * fix$n * (fix$qx-1))) / (fix$mx_scaled * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (2)")
  
  final <- final[!(final$qx>=1 & final$age!=110),] 
  final <- rbind(final, fix)
  if(nrow(final)!=check_n) stop("your fix resulted in a different number of rows than expected (2)")
}


if(nrow(final[final$ax<0])>0){
  check_n = nrow(final)
  fix <- final[final$ax<0,]
  
  fix$qx <- NULL
  fix$qx <- 1- exp(-fix$n * fix$mx_scaled)
  if(nrow(fix[is.na(fix$qx),])>0) stop("your fix created missing qx values (2)")
  
  fix$ax <- NULL
  fix$ax <- (fix$qx + (fix$mx_scaled * fix$n * (fix$qx-1))) / (fix$mx_scaled * fix$qx)
  if(nrow(fix[is.na(fix$ax),])>0) stop("your fix created missing ax values (2)")
  final <- final[!final$ax<0]
  
  final <- rbind(final, fix)
  if(nrow(final)!=check_n) stop("your fix resulted in a different number of rows than expected (2)")
  
}

if(nrow(final[final$qx>1 & final$age!=110,]) > 0) stop("your qx/ax fix didn't actually work (2)")
if(nrow(final[final$ax<0,]) > 0) stop("your qx/ax fix didn't actually work (2)")


final[age==110,]$qx <- 1

final[qx > 1, ]$qx<- .9999


final <- final[,c("sex", "year", "ihme_loc_id", "age", "mx_scaled", "ax", "n", "qx"), with = F]
setnames(final, old = "mx_scaled", new="mx")


setnames(final_save, old="mx", new = "mx_withhiv")
setnames(final_save, old="ax", new = "ax_withhiv")
setnames(final_save, old="qx", new = "qx_withhiv")

final <- merge(final, final_save, by=c("sex", "year", "ihme_loc_id", "age", "n"))


final$diff <- final$mx_withhiv - final$mx
if(min(final$diff) < -.00001){
  stop("Houston, we have a problem: the hiv free mx are significantly larger than the with-hiv mx")
}
final[final$diff < 0, ]$mx <- final[final$diff < 0, ]$mx_withhiv


final$diff2 <- final$qx_withhiv - final$qx
if(min(final$diff2) < -.0001){
  final$qx <- final$qx_withhiv
}

final[final$diff2 < 0, ]$qx <- final[final$diff2 < 0, ]$qx_withhiv


final <- final[,c("sex", "year", "ihme_loc_id", "age", "mx", "ax", "n", "qx"), with = F]
write.csv(final, paste0("FILEPATH/LT_sim_nohiv_", sim, ".csv"),row.names=F)

