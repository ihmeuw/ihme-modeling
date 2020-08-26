code_general <- 'FILEPATH'
source(paste0(code_general, "/function_lib.R"))
source_shared_functions(c("get_population"))

calc_pop_denom<-function(data,gbd_rnd,decomp_step,pop_col_name="sample_size"){
  
  ds<-decomp_step
  
  # Clear current sample size
  if (pop_col_name=="sample_size") data[,sample_size:=NULL]
  
  # Create year id and sex id
  data[,year_id:=round((year_end+year_start)/2)]
  data[,sex_id:=ifelse(sex=="Male",1,ifelse(sex=="Female",2,3))]
  
  # Pull locations/year/ages/sexes for rows that need population_denominator 
  sex_ids<-unique(data[,sex_id])
  locs<-unique(data[,location_id])
  years<-unique(data[,year_id])
  age_dt<-get_age_map(1)
  ages_over_1<-age_dt[age_start>=1,age_group_id]
  under_1<-age_dt[age_start<1,age_group_id]
  over_95<-age_dt[age_start>=95,age_group_id]
  age_dt[,age_group_name:=NULL]
  age_dt[age_start<1,age_group_id:=0]
  age_dt[age_start<1,age_end:=0]
  age_dt[age_start<1,age_start:=0]
  age_dt<-unique(age_dt)

  # Pull population all but under 1 and over 95
  pop<-get_population(age_group_id = ages_over_1,
                      single_year_age = T,
                      location_id = locs,
                      year_id = years,
                      sex_id = sex_ids,
                      gbd_round_id = gbd_rnd,
                      decomp_step = ds,
                      status = "best")
  pop[,run_id:=NULL]
  
  # Pull population for 95+
  pop_over_95<-get_population(age_group_id = 235,
                              location_id = locs,
                              year_id = years,
                              sex_id = sex_ids,
                              gbd_round_id = gbd_rnd,
                              decomp_step = ds,
                              status = "best")
  pop_over_95[,run_id:=NULL]
  pop_over_95[,population:=population/5]
  pop_over_95_template<-expand.grid(age_group_id=age_dt[age_start>=95,age_group_id],sex_id=sex_ids,location_id=locs,year_id=years)
  pop_over_95[,age_group_id:=NULL]
  pop_over_95<-merge(pop_over_95,pop_over_95_template,by=c("sex_id","location_id","year_id"))

  # Pull population for under 1
  pop_under_1<-get_population(age_group_id = under_1,
                              location_id = locs,
                              year_id = years,
                              sex_id = sex_ids,
                              gbd_round_id = gbd_rnd,
                              decomp_step = ds,
                              status = "best")
  pop_under_1<-pop_under_1[,.(population=sum(population)),by=c("location_id","year_id","sex_id")]
  pop_under_1[,age_group_id:=age_dt[age_start==0&age_end==0,age_group_id]]

  pop<-rbindlist(list(pop,pop_under_1,pop_over_95),use.names = T)
  id_cols<-c("location_id","sex_id","year_id","age_group_id")
  pop<-pop[,(id_cols):=lapply(.SD, as.numeric), .SDcols=id_cols]

  # split out data 
  data[,split_seq:=1:.N]
  data_ss<-copy(data)
  data_ss[,n:=(age_end+1 - age_start)]
  expanded <- data.table("split_seq" = rep(data_ss[,split_seq], data_ss[,n]))
  data_ss <- merge(expanded, data_ss, by="split_seq", all=T)
  data_ss[,age_rep:= 1:.N - 1, by =.(split_seq)]
  data_ss[,age_start:= age_start+age_rep]
  data_ss[,age_end:=  age_start]
  
  # merge on age group ids and pops
  data_ss<-merge(data_ss,age_dt,by=c("age_start","age_end"))
  data_ss<-merge(data_ss,pop,by = c("location_id","sex_id","year_id","age_group_id"),all.x=T)
  
  # sum population for each source
  data_ss[,paste0(pop_col_name):=sum(population),by=split_seq]
  data_ss<-unique(data_ss[,c("split_seq",paste0(pop_col_name)),with=F])
  
  # Merge on ss  
  if(paste0(pop_col_name)%in%names(data)) data[,paste0(pop_col_name):=NULL]
  data<-merge(data,data_ss,by="split_seq",all=T)
  data[,split_seq:=NULL]
  
}
