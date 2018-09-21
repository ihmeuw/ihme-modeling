
library(pscl)

library(R2BayesX)

library(INLA)

reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}

load(PATH)

ihme_code<-read.csv("PATH")

demo<-read.csv("PATH")

vivax_loc_find<-ihme_code$loc_id[(which(substr(ihme_code$ihme_lc_id,1,3) %in% c('ARG','ARM','AZE','CRI','SLV','GEO','IRQ','KGZ','MEX','PRK','PRY','KOR','TUR','TKM','UZB')))]

vivax_env<-all_deaths[ all_deaths$location_id %in% vivax_loc_find,  ]

vivax_env<-vivax_env[ vivax_env$age_group_id %in% c(3:5,sort(unique((demo$age_group_id)))),  ]

vivax_env<-vivax_env[ vivax_env$sex_id %in% c(1,2),  ]

vivax_env<-vivax_env[ vivax_env$year_id>1979,  ]

vivax_env$log_env<-log(vivax_env$mean+1)

loc_row<-rep(vivax_loc_find,each=length(c(3:5,sort(unique((demo$age_group_id)))))*2 * 37)

age_row<-rep(c(3:5,sort(unique((demo$age_group_id)))),length(vivax_loc_find)*2*37)

sex_row<-rep(rep(1:2,each=length(sort(unique((age_row))))),length(vivax_loc_find)*37)

year_row<-rep(rep(1980:2016,each=length(sort(unique((age_row))))),length(vivax_loc_find)*2)

row_to_fill<-data.frame(loc_row,age_row,sex_row,year_row)



cod_vivax <- read.csv("PATH")

location_id<-read.csv("PATH")


# zero infalted model

cod_vivax_model<-cod_vivax[-c(which(is.na(cod_vivax$int_deaths))),c('int_deaths','year','sex','age_group_id','log_env','study_deaths')]


cod_vivax_model$int_deaths_bn<-as.numeric(cod_vivax_model$int_deaths>0)

cod_vivax_model$age_group_id2<-reclassify(cod_vivax_model$age_group_id,c(3:20,30,31,32,235),3:24)

mod_x<-bayesx(study_deaths~year+as.factor(sex)+sx(age_group_id2)+log_env,data=cod_vivax_model,method='MCMC',zipdistopt = "zinb",method='REML',outfile="PATH",predict=T)

# imports betas 1000 #

linear_betas<-read.csv("PATH")[,-1]

non_linear_betas<-read.csv("PATH")[,-1]

###


age_effect<-mod_x$effects$`sx(age_group_id2)`

age_235<-age_effect[21,]

age_235[,1]<-24

age_effect<-rbind(age_effect,age_235)



presence_malaria<- read.csv("PATH")

vivax_country<-presence_malaria[presence_malaria$falciparum==0 & presence_malaria$vivax==1, ]



row_to_fill$age_row2<-reclassify(row_to_fill$age_row,c(3:20,30,31,32,235),3:24)

library(doMC)

registerDoMC(3)

list_estimates<-foreach ( i = 1:dim(row_to_fill)[1]) %dopar% {
  


  line_values<-row_to_fill[i,]
  
  selected_row<-vivax_env[vivax_env$location_id==as.numeric(line_values[1]) & vivax_env$age_group_id==as.numeric(line_values[2]) & vivax_env$sex_id==as.numeric(line_values[3]) & vivax_env$year_id==as.numeric(line_values[4]),]
  

  
  intercept_beta<-linear_betas[,1]
  year_beta<-linear_betas[,2]*as.numeric(selected_row[3])
  as_factor_sex_beta<-linear_betas[,3]*(as.numeric(selected_row[4])-1)
  
  pos_age<-which(age_effect$age_group_id2==as.numeric(line_values[,5]))
  
  age_group_id_beta<-non_linear_betas[,pos_age]
  
  log_env_beta<-linear_betas[,4]*log(as.numeric(selected_row[5])+1)
  
  betas_mat<-rbind(intercept_beta, year_beta,age_group_id_beta,log_env_beta)
  

  estimates_mean<-exp(colSums(betas_mat))
   
  estimates_mean[estimates_mean<0]<-0
  


  return(c(as.numeric(line_values[1,-5]),estimates_mean))
  
  
}

vivax_1000<-do.call(rbind,list_estimates)


vivax_1000_db<-as.data.frame(vivax_1000)

names(vivax_1000_db)<-c('location_id','age_group_id','sex_id','year_id',paste('draw_',0:999,sep=''))

viv_2016<-vivax_1000_db[vivax_1000_db$location_id==130 & vivax_1000_db$year_id==2016,]


save(vivax_1000_db,file="PATH")




