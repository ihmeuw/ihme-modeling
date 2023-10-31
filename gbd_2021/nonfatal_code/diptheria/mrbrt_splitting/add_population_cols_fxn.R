###
### Using NTDs function to add population to unadj_dt to not drop rows that don't exactly match GBD age groups!
###


add_population_cols <- function(data, gbd_round, decomp_step){

  if (!("sex_id" %in% names(data))) { data[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}

  cat("Returning data with columns of population by sex aggregated over custom age bin for floor of average of year_start and year_end; update to full year for more robustness")

  data[, year_id := floor((year_end + year_start)/2)]

  sexes <- 1:3
  locs  <- unique(data[, location_id])
  years <- unique(data[, year_id])

  age_dt <- fread("/FILEPATH/age_id_dt.csv")

  #fix 1 year old age_group id to align with new 2020 age_group_id so will get results for 1 year old age group when call get_population
  age_dt[age_start==1 , age_group_id:=238]

  ages_over_1 <- age_dt[age_start >=1 , age_group_id]
  under_1     <- age_dt[age_start <1, age_group_id]
  over_95     <- age_dt[age_start >=95 , age_group_id]

  age_dt[ , age_group_name:=NULL]
  age_dt[age_start<1 , age_group_id:=0]
  age_dt[age_start<1 , age_end:=0]
  age_dt[age_start<1 , age_start:=0]
  age_dt<-unique(age_dt)

  # Pull population all but under 1 and over 95

  pop <- get_population(age_group_id = ages_over_1,
                        single_year_age = TRUE,
                        location_id = locs,
                        year_id = years,
                        sex_id = sexes,
                        gbd_round_id = gbd_round,
                        decomp_step = decomp_step,
                        status = "best")

  pop[,run_id:=NULL]

  # Pull pop over 95

  pop_over_95 <- get_population(age_group_id = 235,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")

  pop_over_95[, run_id:=NULL]
  pop_over_95[, population:=population/5]
  pop_over_95_template<-expand.grid(age_group_id=age_dt[age_start>=95,age_group_id],sex_id=sexes,location_id=locs,year_id=years)
  pop_over_95[, age_group_id:=NULL]
  pop_over_95 <- merge(pop_over_95, pop_over_95_template,by=c("sex_id","location_id","year_id"))

  # Pull pop under 1

  pop_under_1 <- get_population(age_group_id = under_1,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")

  pop_under_1 <- pop_under_1[,.(population=sum(population)),by=c("location_id","year_id","sex_id")]
  pop_under_1[,age_group_id:=0]

  pops <- rbindlist(list(pop, pop_under_1, pop_over_95), use.names = T)
  pops <- dcast.data.table(pops, ... ~ sex_id, value.var = "population")
  setnames(pops, c("1", "2", "3"), c("male_population", "female_population", "both_population"))

  # split out data

  data[, split_seq:=1:.N]
  data_ss <- copy(data)
  data_ss[, n := (floor(age_end)+1 - floor(age_start))]
  expanded <- data.table("split_seq" = rep(data_ss[,split_seq], data_ss[,n]))
  data_ss <- merge(expanded, data_ss, by="split_seq", all=T)
  data_ss[,age_rep:= 1:.N - 1, by =.(split_seq)]
  data_ss[,age_start:= floor(age_start)+age_rep]
  data_ss[,age_end:=  age_start]

  # merge on age group ids and pops

  cat("/n at mergeing data tables in pop")

  data_ss <- merge(data_ss, age_dt, by=c("age_start","age_end"))
  ### !!!
  data_ss <- merge(data_ss, pops, by = c("location_id","year_id","age_group_id"),all.x=T)

  data_ss <- data_ss[, .("male_population" = sum(male_population),
                         "female_population" = sum(female_population),
                         "both_population" = sum(both_population)),
                     by = "split_seq"]

  data <- merge(data, data_ss, by="split_seq", all=T)

  cat("/n past all merges")

  data[, split_seq := NULL]

  return(data)

}
