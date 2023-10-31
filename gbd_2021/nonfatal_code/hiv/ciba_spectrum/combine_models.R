################################################################################

rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/")

library(dplyr)
library(parallel)
library(Metrics, lib = paste0("/homes/",user,"/rlibs"))
library(MASS)
library(data.table)
library(mortdb, lib =  "FILEPATH")
source( "FILEPATH")
source(paste0(home.dir, "FILEPATH"))
library(mortdb, lib =  "FILEPATH")

loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all'))
eppasm.locs <- sort(loc.table[epp == 1 & grepl('1', group), ihme_loc_id])
spec.locs <- sort(loc.table[spectrum == 1, ihme_loc_id])
loc.list <- spec.locs[!spec.locs %in% eppasm.locs]
runs = "gbd20_grid"

full = rbindlist(lapply(loc.list,function(loc){
  
    print(loc)
    
    dt = list()
    for(run in runs){
      if(file.exists( "FILEPATH")){
      dx = fread(paste0( "FILEPATH"))
      dx[,iso3 := loc]
      
      if(!"RMSE" %in% colnames(dx)){
        
        dx <- NULL
      
        } 
      
      } else {
      
        dx <- NULL
        
      }
      
      dt = rbind(dt,dx)
    }  
  return(dt)
  
}))


vars = c("rmse_spec_pre","rmse_spec_post","RMSE","trend_fit_post","trend_fit")

best = rbindlist(lapply(vars,function(var){
  
  if(grepl("trend",var)){
    
    lowest = full[,list(var = max(get(var),na.rm=TRUE)), by= "iso3" ]
  
  } else {
    
    lowest = full[,list(var = min(get(var),na.rm=TRUE)), by= "iso3" ]
    
  }
  
  lowest = merge(lowest,full,by = "iso3")
  setnames(lowest,"var",paste0(var,"_best"))
  
  lowest = lowest[get(var) == get(paste0(var,"_best"))]
  lowest = unique(lowest[,.(iso3,var,index)])
  return(lowest)
  
}))

fread( "FILEPATH")[index %in% c("combo_12","combo_14","combo_17","combo_28")]
data = best %>% group_by(index,var) %>% summarise(count = n()) %>% data.table()


ggplot(data[var != "trend_fit_pre"], aes(var, index, fill= count)) + 
  geom_tile() + scale_fill_gradient(low = "yellow", high = "red") +
  theme(axis.text.x = element_text(angle=45,size=12,hjust=1))

ggplot(data[grepl("post",var)], aes(var, index, fill= count)) + 
  geom_tile() + scale_fill_gradient(low = "yellow", high = "red")



##Argentina
run.name = "gbd20_grid"
loc = "ARG"
index = fread( "FILEPATH")


best_post = index[rmse_spec_post == min(rmse_spec_post),.(art_denom,incidence,index,rmse_spec_pre,rmse_spec_post,trend_fit_post)]
best_pre = index[rmse_spec_pre == min(rmse_spec_pre),.(art_denom,incidence,index,rmse_spec_pre,rmse_spec_post,trend_fit_pre)]

#1. pull incidence
inc1 = fread( "FILEPATH")
inc2 = fread( "FILEPATH")

inc1[,run:="pre_art"]
inc2[,run:="post_art"]

both.dt = list(inc1,inc2)

both.dt = rbindlist(lapply(both.dt,function(dt){
  dt = melt(dt, id.vars = c("year","sex","single.age","run"))
  dt = dt[variable == "draw1"]
  return(dt)
}))



ggplot(both.dt[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"],aes(year,value, color=run)) + 
 geom_line() + facet_wrap(~single.age, scales = "free_y")

out_folder = "combo_12"
stage = "stage_1"
spec_data = fread( "FILEPATH")
art_data = copy(spec_data)[age >= 15]
art_data = art_data[,list(pop_art = sum(pop_art)), by = c("year","sex","run_num")]
art_data = art_data[,list(pop_art = mean(pop_art)), by = c("year","sex")]
art_data[, sex := ifelse(sex=="female",2,1)]
art_data[,pre_art := ifelse(pop_art > 0,0,1)]
switch_year = art_data[pre_art == 0, min(year)]

inc1 = fread( "FILEPATH")
inc2 = fread( "FILEPATH")


post_art = both.dt[run=="post_art"]
all = merge(both.dt[run=="pre_art"], post_art, by=c("year","sex","single.age"))
all[,ratio := value.x/value.y]
all[year < 1997, new_val := value.y * ratio]

new = all[,.(year,sex,single.age,variable=variable.y,value=value.y,new_val)]
new = new[year < 1997 & !is.nan(new_val), value := new_val]
new[,new_val := NULL]
new[,run := "scaled_post_art"]

both.dt = rbind(both.dt,new)
ggplot(both.dt[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="female"],aes(year,value, color=run)) + 
  geom_line() + facet_wrap(~single.age, scales = "free_y")


new = dcast(new[,.(year,sex,single.age,variable,value)], year + sex + single.age ~ variable, value.var="value")

test = both.dt[run=="post_art"]
test_old =  both.dt[run=="pre_art"]
weight.key = data.table(year =  c(c(1992:2002), c(1992:2002)),
                        run = c("pre_art","post_art"),
                        weight = c(11:1,1:11))

test2 = rbind(test,test_old)
test2 = merge(test2,weight.key,all.x=TRUE)
weights = test2[!is.na(weight)]
weights = weights[,list(value = weighted.mean(value,weight)), by = c("year","sex","single.age","variable")]

test_all = merge(test,test_old, by= c("sex","year","single.age","variable"))
test_all[year < 1992, value := value.y]
test_all[year > 2002, value := value.x ]
test_all = unique(test_all[,.(sex,year,single.age,variable,value)])
test_all = test_all[!year %in% c(1992:2002)]
test_all = rbind(test_all,weights)

ggplot(new[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"],aes(year,draw1)) + 
  geom_line() + facet_wrap(~single.age, scales = "free_y")

test = both.dt[run=="post_art"]
test_old =  both.dt[run=="pre_art"]
test_all = rbind(test,test_old)

draw.weights = rpois(53,1)
hist(draw.weights)
weight.key = data.table(year =  c(c(1970:2022), c(1970:2022)),
                        run = c("pre_art","post_art"),
                        weight = c(sort(draw.weights,decreasing=TRUE),
                                   sort(draw.weights)))

test_all = merge(test_all,weight.key,all.x=TRUE)
m="male"
a=15
for(m in c("male","female")){
  for(a in unique(test_all$single.age)){
    
        x = test_all[single.age == a & sex == m]
        p = predict(loess(value ~ year, x, span = 0.5))
        test_all[single.age == a & sex == m, predict := p]
  }
}


test_all = test_all[,list(value = weighted.mean(value,weight)), by = c("year","sex","single.age","variable")]
ggplot(new[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"],aes(year,value)) +
  geom_line() + facet_wrap(~single.age, scales = "free_y")
  geom_line(data = new[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"],aes(year,predict1),col="red") +
  

cod_data_merge[,pop_art2 := pop_art^2]

#Negative binomal model for deaths
m1 = glm.nb(draw1 ~ year + sex + bs(year,3) + factor(single.age), 
            init.theta = 0.05, 
            data = new[year <= 1990 | year >= 1997], 
            link = log)
summary(m1)

new[,predict1 := NULL]
new[,predict1  := predict(m1, type = "response", newdata = new)]


ggplot(new[single.age %in% c(15,20,25,30,35,40,45,50)& sex=="male"],aes(year,draw1)) + 
  geom_line() + geom_line(data = new[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"], 
                          aes(year,predict1), color="red") + facet_wrap(~single.age, scales = "free_y")

new[year %in% c(1990:2010),draw1 := predict1]
new[,predict1 := NULL]

new = test_all
new = dcast(new, year + sex + single.age ~ variable, value.var = "value")

dir.create( "FILEPATH")

write.csv(new, "FILEPATH",row.names=FALSE)



test = copy(new)
new_test = test_all[,list(value = smooth(value)), by = c("single.age","sex","variable")]
new_test = cbind(new_test,year = test_all$year)

ggplot(test[single.age %in% c(15,20,25,30,35,40,45,50)& sex=="male"],aes(year,draw1)) + 
  geom_line() + geom_line(data = new[single.age %in% c(15,20,25,30,35,40,45,50) & sex=="male"], 
                          aes(year,predict), color="red") + facet_wrap(~single.age, scales = "free_y")


ggplot(test,aes(year,draw1)) + geom_line() + geom_line(data = test, aes(year,predict), color = "red") 















