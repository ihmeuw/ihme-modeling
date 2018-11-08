
args<-commandArgs(trailingOnly = TRUE)
education_run_id <- 43314
rei_id <- 363
me_id <- 18740 # 18740 = dismod model, 20949 = interpolated annual exposure
cause_ids <- c(568, 571)
location <- args[1]
years <- c(1990:2017)
ages <- c(5:20, 30:32, 235) # ages 2:4 are infant ages and are filled later

#########################################################
############## Load and prep required data ##############
#########################################################

#### Load required settings / functions ####
if (Sys.info()['sysname'] == 'Linux') {j_root <- '/home/j/'} else {j_root <- 'J:/'}
rlogit <- function(x){exp(x)/(1+exp(x))}
source(paste0(j_root, "FILEPATH"))
source(paste0(j_root, "FILEPATH"))
source(paste0(j_root, "FILEPATH"))
source(paste0(j_root, "FILEPATH"))
draw_cols <- paste0("draw_", 0:999)

#### Load RRs from meta-regression ####
rr <- fread(paste0(j_root, "FILEPATH"))
rr[,se:=(upper - lower)/(qnorm(0.975,0, 1)*2)]
for(r in rr$time){
  if(r == 0){
    rr_draws <- data.table(time=r, draw=(draw_cols), rr=rlnorm(1000, rr[time==r, rr], rr[time==r, se]))
  } else {
    rr_draws_temp <- data.table(time=r, draw=(draw_cols), rr=rlnorm(1000, rr[time==r, rr], rr[time==r, se]))
    rr_draws <- rbind(rr_draws, rr_draws_temp)
  }
}

### Load persistence estimates from meta-regression ####
persistence_reg <- fread(paste0(j_root, "FILEPATH"))
persistence_reg[,se:=(upper - lower)/(qnorm(0.975,0, 1)*2)]
for(p in persistence_reg$time){
  if(p == 0){
    persistence_draws <- data.table(time=p, draw=(draw_cols), persistence=rlogit(rnorm(1000, persistence_reg[time==p, prevalence], persistence_reg[time==p, se])))
  } else {
    persistence_draws_temp <- data.table(time=p, draw=(draw_cols), persistence=rlogit(rnorm(1000, persistence_reg[time==p, prevalence], persistence_reg[time==p, se])))
    persistence_draws <- rbind(persistence_draws, persistence_draws_temp)
  }
}

#### Pull Exposure ####
if(me_id == 18740){
  exposure <- interpolate(gbd_id_type = "modelable_entity_id", gbd_id = me_id, measure_id = 18, source = "epi", location_id = location, reporting_year_start=min(years), reporting_year_end=max(years), age_group_id = ages, sex_id = c(1,2), status = "best")
} else {
  exposure <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = me_id, measure_id = 18, source = "epi", location_id = location, year_id = years, age_group_id = ages, sex_id = c(1,2), status = "best")
}
#### Assume 1990 prevalence back to 1957 ####
for(y in c(1957:1989)){
  exposure_temp <- exposure[year_id==1990,]
  exposure_temp[, year_id:=y]
  exposure <- rbind(exposure, exposure_temp)
}

#### Make age groups that are easier to loop with ####
age_labels <- get_ids(table="age_group")[age_group_id %in% ages,]
age_labels[,age_group_start := as.numeric(substring(age_group_name, 1, 2))]
age_labels[age_group_start==1, age_group_start := 0] # Not technically correct but helps loop function and is corrected post loop
exposure <- merge(exposure, age_labels[,.(age_group_id, age_group_start)], by="age_group_id")
exposure[,location_id := as.numeric(location_id)]


#### Load population data ####
population <- get_population(age_group_id = ages, location_id=location, year_id=c(1957:2017), sex_id=c(1, 2))
population[,location_id := as.numeric(location_id)]

#### Make estimates by non-GBD age groups ####
exposure <- merge(exposure, population, by=c("age_group_id", "location_id", "year_id", "sex_id"))
exposure <- melt.data.table(exposure, id.vars = names(exposure)[!(names(exposure) %in% draw_cols)], value.name = "prevalence", variable.name="draw")

#### Load estimates of proportion in school and correct estimates to match GBD age groups ####
in_school <- fread(paste0("FILEPATH", education_run_id,"FILEPATH", location, ".csv"))
in_school[,location_id := as.numeric(location_id)]
in_school[, age_group_start := ifelse(age_group_id==6, 6, ifelse(age_group_id==7, 12, 15))]
in_school <- melt.data.table(in_school, id.vars = names(in_school)[!(names(in_school) %in% draw_cols)], value.name = "prevalence", variable.name="draw")
in_school_0to4 <- in_school[age_group_id==6, ]
in_school_0to4[, `:=` (age_group_id = 5, age_group_start = 0, prevalence = 0)]
in_school <- rbind(in_school, in_school_0to4)
in_school <- merge(in_school, population, by=c("age_group_id", "location_id", "year_id", "sex_id"))
in_school_5to9 <- in_school[age_group_start==6, ]
in_school_5to9[, `:=` (age_group_start = 5, prevalence = prevalence * 4/5)] # age_group_id = 6 in ST-GPR model was actually 6-11, not 5-9.
in_school_4below <- rbind(in_school[age_group_start == 0, ], in_school_5to9)
in_school_4below[,total_pop := sum(population), by=c("year_id", "sex_id", "draw")]
in_school_4below[, pop_weight := 2*(population / total_pop)]
in_school_4below <- merge(in_school_4below[age_group_start == 0, ], in_school_4below[age_group_start == 5, .(year_id, sex_id, draw, prevalence_old = prevalence, pop_weight_old = pop_weight)], by=c("year_id", "sex_id", "draw"))
in_school_4to8 <- copy(in_school_4below)
in_school_4to8[, `:=` (age_group_start = 4 , prevalence = prevalence * pop_weight * (1)/5 + prevalence_old * pop_weight_old * (4/5))]
in_school_3to7 <- copy(in_school_4below)
in_school_3to7[, `:=` (age_group_start = 3 , prevalence = prevalence * pop_weight * (2)/5 + prevalence_old * pop_weight_old * (3/5))]
in_school_2to6 <- copy(in_school_4below)
in_school_2to6[, `:=` (age_group_start = 2 , prevalence = prevalence * pop_weight * (3)/5 + prevalence_old * pop_weight_old * (2/5))]
in_school_1to5 <- copy(in_school_4below)
in_school_1to5[, `:=` (age_group_start = 1 , prevalence = 0)]
in_school_6to10 <- in_school[age_group_start==6, ] # age_group_id = 6 in ST-GPR model was actually 6-11, which contains 6-10
in_school_7to11 <- copy(in_school[age_group_start==6, ])
in_school_7to11[, age_group_start := 7] # age_group_id = 6 in ST-GPR model was actually 6-11, which contains 7-11
in_school_8plus <- in_school[age_group_start %in% c(6, 12), ]
in_school_8plus[,total_pop := sum(population), by=c("year_id", "sex_id", "draw")]
in_school_8plus[, pop_weight := 2*(population / total_pop)]
in_school_8plus <- merge(in_school_8plus[age_group_start == 6, ], in_school_8plus[age_group_start == 12, .(year_id, sex_id, draw, prevalence_old = prevalence, pop_weight_old = pop_weight)], by=c("year_id", "sex_id", "draw"))
in_school_8to12 <- copy(in_school_8plus)
in_school_8to12[, `:=` (age_group_start = 8 , prevalence = prevalence * pop_weight * (4)/5 + prevalence_old * pop_weight_old * (1/5))]
in_school_9to13 <- copy(in_school_8plus)
in_school_9to13[, `:=` (age_group_start = 9 , prevalence = prevalence * pop_weight * (3)/5 + prevalence_old * pop_weight_old * (2/5))]
in_school_10to14 <- copy(in_school_8plus)
in_school_10to14[, `:=` (age_group_start = 10 , prevalence = prevalence * pop_weight * (2)/5 + prevalence_old * pop_weight_old * (3/5))]
in_school_11to15 <- in_school[age_group_start %in% c(6, 12, 15),]
in_school_11to15[,total_pop := sum(population), by=c("year_id", "sex_id", "draw")]
in_school_11to15[, pop_weight := 3*(population / total_pop)]
in_school_11to15_temp <- merge(in_school_11to15[age_group_start == 6, ], in_school_11to15[age_group_start == 12, .(year_id, sex_id, draw, prevalence_old = prevalence, pop_weight_old = pop_weight)], by=c("year_id", "sex_id", "draw"))
in_school_11to15 <- merge(in_school_11to15_temp, in_school_11to15[age_group_start == 15, .(year_id, sex_id, draw, prevalence_old_2 = prevalence, pop_weight_old_2 = pop_weight)], by=c("year_id", "sex_id", "draw"))
in_school_11to15[, `:=` (age_group_start = 11, prevalence = prevalence * pop_weight * (1)/5 + prevalence_old * pop_weight_old * (3/5) + prevalence_old_2 * pop_weight_old_2 * (1/5))]
in_school_12plus <- in_school[age_group_start %in% c(12, 15), ]
in_school_12plus <- in_school_12plus[,total_pop := sum(population), by=c("year_id", "sex_id", "draw")]
in_school_12plus <- in_school_12plus[, pop_weight := 2*(population / total_pop)]
in_school_12plus <- merge(in_school_12plus[age_group_start == 12, ], in_school_12plus[age_group_start == 15, .(year_id, sex_id, draw, prevalence_old = prevalence, pop_weight_old = pop_weight)], by=c("year_id", "sex_id", "draw"))
in_school_12to16 <- copy(in_school_12plus)
in_school_12to16[, `:=` (age_group_start = 12 , prevalence = prevalence * pop_weight * (3)/5 + prevalence_old * pop_weight_old * (2/5))]
in_school_13to17 <- copy(in_school_12plus)
in_school_13to17[, `:=` (age_group_start = 13 , prevalence = prevalence * pop_weight * (2)/5 + prevalence_old * pop_weight_old * (3/5))]
in_school_14to18 <- copy(in_school_12plus)
in_school_14to18[, `:=` (age_group_start = 14 , prevalence = prevalence * pop_weight * (1)/5 + prevalence_old * pop_weight_old * (3/5))]
in_school_15to19 <- copy(in_school_12plus)
in_school_15to19[, `:=` (age_group_start = 15 , prevalence = prevalence_old * pop_weight_old * (3/5))]
in_school_16to20 <- copy(in_school_12plus)
in_school_16to20[, `:=` (age_group_start = 16 , prevalence = prevalence_old * pop_weight_old * (2/5))]
in_school_17to21 <- copy(in_school_12plus)
in_school_17to21[, `:=` (age_group_start = 17 , prevalence = prevalence_old * pop_weight_old * (1/5))]
in_school <- rbind(in_school_1to5, in_school_2to6, in_school_3to7, in_school_4to8, in_school_5to9, in_school_6to10, in_school_7to11, in_school_8to12, in_school_9to13, in_school_10to14, in_school_11to15, in_school_12to16, in_school_13to17, in_school_14to18, in_school_15to19, in_school_16to20, in_school_17to21, fill=T)
in_school <- in_school[,.(location_id, year_id, sex_id, age_group_start, draw, prop_in_school = prevalence)]
in_school[prop_in_school>1, prop_in_school := 1] # Cap
print("Finished estimating proportion in school")

######################################################
############## Calculate PAFs by cohort ##############
######################################################

filelist <- list.files(path=paste0(j_root,"FILEPATH"), pattern = "csv", full.names = F)
max_followup <- (in_school[prop_in_school>0, max(age_group_start)+4] - in_school[prop_in_school>0, min(age_group_start)]) + max(rr$time)

for(sex in c(1, 2)){
  for(year in years){
    save_file <- paste0("paf_yld_", location, "_", year, "_", sex, ".csv")
    if(!(save_file %in% filelist)){
    for(age_start in unique(exposure$age_group_start)){
      # Subset required data due to save memory
      exposure_age <- exposure[age_group_start %in% c(age_start:(age_start-max_followup)) & sex_id == sex, ]

      # Interpolate bullying exposure estimates inbetween GBD age groups
      for(age_start_2 in unique(exposure_age$age_group_start)){
        exposure_temp <- exposure_age[age_group_start %in% c(age_start_2, age_start_2+5), ]
        exposure_temp[,total_pop := sum(population), by=c("year_id", "sex_id", "draw")]
        exposure_temp[, pop_weight := 2*(population / total_pop)]
        exposure_temp <- merge(exposure_temp[age_group_start == age_start_2, ], exposure_temp[age_group_start == age_start_2 + 5, .(year_id, sex_id, draw, prevalence_old = prevalence, pop_weight_old = pop_weight)], by=c("year_id", "sex_id", "draw"))
        for(m in c(1:4)){
          exposure_temp_custom <- copy(exposure_temp)
          exposure_temp_custom[, `:=` (age_group_start = age_group_start + m, prevalence = prevalence * pop_weight * (5-m)/5 + prevalence_old * pop_weight_old * (m/5))]
          assign(paste0("exposure_temp_", m), exposure_temp_custom)
        }
        exposure_temp <- rbind(exposure_temp, exposure_temp_1, exposure_temp_2, exposure_temp_3, exposure_temp_4)
        exposure_temp[, `:=` (total_pop=NULL, pop_weight=NULL, prevalence_old=NULL, pop_weight_old=NULL)]
        exposure_age <- rbind(exposure_age, exposure_temp[!(age_group_start %in% unique(exposure_age$age_group_start)),])
      }

      # Adjust prevalence by proportion in school
      exposure_age <- merge(exposure_age, in_school, by=c("location_id", "year_id", "sex_id", "age_group_start", "draw"), all.x=T)
      exposure_age[,prevalence := ifelse(is.na(prop_in_school), 0, prevalence * prop_in_school)]

      # Assign time variable to indicate cohort over time
      for(x in c(0:max_followup)){
        exposure_age[year_id==year - x & sex_id == sex & age_group_start == age_start-x, time := x]
      }
      exposure_age <- exposure_age[!is.na(time),]

      # Adjust prevalence by persistence
      if(exposure_age[, sum(prevalence)] > 0){
        persistence_range <- c(exposure_age[prevalence>0, max(time)] : exposure_age[prevalence>0, min(time)])
        for(y in persistence_range){
          if(y > 0 & exposure_age[, sum(prevalence)] > 0){
            for(p in c(1:11)){
              exposure_y <- merge(exposure_age[time==y , .(prevalence, draw, time)], persistence_draws[time==p, .(persistence, draw)], by="draw")
              exposure_y <- exposure_y[, .(prev_persist = prevalence * persistence, draw)]
              exposure_p <- merge(exposure_age[time==y-p, ], exposure_y, by=c("draw"))
              exposure_p[, `:=` (prevalence = ifelse(prevalence <= 0, prevalence, prevalence - prev_persist))]
              exposure_age <- merge(exposure_age, exposure_p[,.(time, prevalence_adj=prevalence, draw)], by=c("time", "draw"), all.x=T)
              exposure_age[!is.na(prevalence_adj), prevalence := prevalence_adj]
              exposure_age[,prevalence_adj := NULL]
            }
          }
          print(paste0("Finished persistence for follow-up year ", y))
        }
      }

      exposure_age[prevalence<0, prevalence := 0] # can't have negative exposure

      # Merge with RRs from meta-regression
      exposure_age <- merge(exposure_age, rr_draws, by=c("time", "draw"))

      # Calculate Exposure * RRs
      exposure_age[, exp_rr := prevalence * rr]
      exposure_age[, sum_exp_rr := sum(exp_rr), by="draw"]

      # Set TMRED
      exposure_age[, no_exposure := 1-sum(prevalence), by="draw"]
      tmred <- 1 # exposure 100% with rr 1

      # Calculate PAFs
      exposure_age[, paf := (sum_exp_rr + no_exposure - tmred) / (sum_exp_rr + no_exposure)]

      # Change PAFs < 0 to 0
      exposure_age[paf < 0, paf := 0]
      pafs <- unique(exposure_age[,.(age_group_start = age_start, draw, paf)])

      # Merge with other age-specific paf estimates
      if(age_start == unique(exposure$age_group_start)[1]){
        pafs_byage <- pafs
      } else {
        pafs_byage <- rbind(pafs_byage, pafs)
      }
    }
    # Re-assign correct age_group_ids
    pafs_byage <- merge(pafs_byage, age_labels, by="age_group_start")

    # Re-add age_group_ids 2 to 4 and set estimates to 0
    for(infant_age in c(4:2)){
      pafs_infant <- data.table(age_group_id=infant_age, draw=paste0("draw_", 0:999), paf=0)
      pafs_byage <- rbind(pafs_infant,pafs_byage[,.(age_group_id, draw, paf)])
    }

    # Prep and save final file
    pafs_byage[, draw := gsub("draw", "paf", draw)]
    pafs_byage <- dcast(pafs_byage, age_group_id~draw, value.var="paf")
    pafs_byage[, `:=` (rei_id = rei_id, location_id = location, sex_id = sex, year_id=year, cause_id = cause_ids[1], modelable_entity_id = me_id)]
    setcolorder(pafs_byage, c("rei_id", "age_group_id", "location_id", "sex_id", "year_id", "cause_id", "modelable_entity_id", paste0("paf_", 0:999)))

    # add anxiety rows
    pafs_byage_anx <- copy(pafs_byage)
    pafs_byage_anx[,cause_id := cause_ids[2]]
    pafs_byage <- rbind(pafs_byage, pafs_byage_anx)

    write.csv(pafs_byage, paste0(j_root,"FILEPATH/paf_yld_", location, "_", year, "_", sex, ".csv"), row.names=F)
    print(paste0("PAF calculated for location ", location, ", year ", year, ", for sex ", sex))
    }
  }
}






