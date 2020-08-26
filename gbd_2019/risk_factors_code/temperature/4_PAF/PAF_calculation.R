
#-------------------Header------------------------------------------------
# Author: USERNAME
# Date: 2019
# Purpose: The script calculates low and high temperature PAFs for 12 level 3 causes
#          PAFs are calculated for each pixel day, population-weighted and collapsed for each subnational locations
#          The code is parallelized by year and location
#***************************************************************************


rm(list=ls())

print("start")

# runtime configuration

if (Sys.info()["sysname"] == "Linux") {

  j <- "ADDRESS"
  h <- "/ADDRESS/USERNAME/"

} else {

  j <- "ADDRESS"
  h <- "ADDRESS"

}


print("config")

require(data.table)
require(feather)
library(zoo)


############## function for conditional PAF calculation

fcoldPAF<-function(x,y,z){
  #x=temp
  #y=tmrel
  #z=rr
  if(x<y){
    ifelse(z>=1,(z-1)/z,-((1/z)-1)/(1/z))
  }
  else{NA}
}

fheatPAF<-function(x,y,z){
  #x=temp
  #y=tmrel
  #z=rr
  if(x>y){
    ifelse(z>=1,(z-1)/z,-((1/z)-1)/(1/z))
  }
  else{NA}
}

f.approx<-function(x){
  na.approx(x,na.rm=FALSE)
}

NA2mean <- function(x) replace(x, is.na(x), mean(x, na.rm = TRUE))


###############################################################

ndraws<-999

arg <- commandArgs()[-(1:5)]  # First args are for unix use only
print(arg)

YEAR=arg[1]
print(YEAR)

LOCATION_ID<-arg[2]
print(LOCATION_ID)


##########################################################################


ACAUSE<-c("diabetes","ckd","inj_homicide","inj_mech","inj_trans_road","cvd_ihd","resp_copd",
          "cvd_stroke","cvd_htn","inj_suicide","inj_drowning","lri")


CAUSE_IDS<-c(587,589,724,704,689,493,509,494,498,718,698,322)

MHEAT<-NULL
MCOLD<-NULL

##############################################################################################################################################
### Read in temperature data
##############################################################################################################################################

temp<-as.data.table(read_feather(paste0(j,"FILEPATH",YEAR,"FILEPATH",YEAR,"_",LOCATION_ID,".feather")))

### if temp data for 2019 is not available use 2018
if(nrow(temp)<1 & YEAR==2019){
  temp<-as.data.table(read_feather(paste0(j,"FILEPATH",2018,"FILEPATH",2018,"_",LOCATION_ID,".feather")))

}

temp[,dupli:=duplicated(temp)]
temp<-temp[dupli==FALSE]
TEMP_DRAWS<-paste0("temp_",0:ndraws)
spredtest<-is.na(temp[1,"spread"])

###if spread is missing of if ther is error in spread (spread being temperature values) use spread of fixed value to generate draws
if(spredtest==TRUE | mean(temp$spread,na.rm=TRUE)>10){
  temp[ ,c(paste0("temp_",0:999)):=lapply(.SD,function(x){rnorm(1,temp_C,1.5)}),by=.(x,y,date)]
}

temp[,spread:=lapply(.SD,function(x){replace(x,is.na(x),mean(x,na.rm=TRUE))}),.SDcols=c("spread")]

### if there are missing values interpolate these from surrounding pixels
tryCatch({
  if(any(is.na(temp[,paste0("temp_",0:999)]))==TRUE){
    temp[,paste0("temp_",0:999):=lapply(.SD,f.approx),.SDcols=paste0("temp_",0:999),by="date"]
  }
}, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})


### if there are still missing values replace them with mean
if(any(is.na(temp[,paste0("temp_",0:999)]))==TRUE){
  temp[,paste0("temp_",0:999):=lapply(.SD,NA2mean),.SDcols=paste0("temp_",0:999),by="date"]
}


##############################################################################################################################################
### Read in RR and TMREL
##############################################################################################################################################

for (k in 1:length(ACAUSE)){
  tryCatch({

    rr<-as.data.table(read_feather(paste0(j,"FILEPATH",ACAUSE[k],".feather")))[,1:1002]
    colnames(rr)<-c("annual_temperature","daily_temperature",paste0("draw_",0:999))
    tmrel<-fread(paste0(j,"FILEPATH/tmrel_",LOCATION_ID,".csv"))
    tmrel<-tmrel[year_id==YEAR]

    ####### read in population
    pop<-as.data.table(read_feather(paste0(j,"FILEPATH",YEAR,"FILEPATH",YEAR,"_",LOCATION_ID,".feather")))
    TEMP_DRAWS<-paste0("temp_",0:ndraws)
    TMREL_DRAWS<-paste0("tmrel_",0:ndraws)
    RR_DRAW<-paste0("draw_",0:ndraws)

    PAFS<-NULL

    for (i in 1:(ndraws+1)){
      ### select draws
      temp_draw<-TEMP_DRAWS[i]
      tmrel_draw<-TMREL_DRAWS[i]
      rr_draw<-RR_DRAW[i]
      npop<-nrow(pop)


      TEMP<-temp[,c("date","x","y","meanTempCat","dailyTempCat")]
      colnames(TEMP)<-c("date","x","y","annual_temperature","dailyTempCat")
      TEMP[,temp_draw:=temp[,get(temp_draw)]]
      TEMP[,daily_temperature:=round(temp_draw,digits = 1)]

      RR<-rr[,c("annual_temperature","daily_temperature")]
      RR[,annual_temperature:=as.integer(annual_temperature)]
      RR[,rr_draw_log:=rr[,get(rr_draw)]]
      RR[,rr_draw:=exp(rr_draw_log)]

      TMREL<-tmrel[,c("meanTempCat")]
      colnames(TMREL)<-"annual_temperature"
      TMREL[,meanTempCat:=as.numeric(annual_temperature)]
      TMREL[,tmrel_draw:=tmrel[,get(tmrel_draw)]]
      TMREL[,tmrel_draw:=round(tmrel_draw,digits = 1)]
      TMREL<-TMREL[,-2]

      RISKS<-merge(RR,TMREL,by=c("annual_temperature"),all.x=TRUE)
      RISKS[,daily_temperature:=round(daily_temperature,digits=1)]

      ####
      tmrel_left<-unlist(TMREL[annual_temperature==6][1,2])
      tmrel_right<-unlist(TMREL[annual_temperature==28][1,2])

      RISKS[,tmrel_draw:=ifelse(is.na(tmrel_draw)&annual_temperature<6,tmrel_left,tmrel_draw)]
      RISKS[,tmrel_draw:=ifelse(is.na(tmrel_draw)&annual_temperature>28,tmrel_right,tmrel_draw)]
      RISKS[,":="(rr_draw=round(rr_draw,digits=3),tmrel_draw=round(tmrel_draw,digits=3),daily_temperature=round(daily_temperature,digits=3))]

      shifts<-RISKS[daily_temperature==tmrel_draw][,-c("daily_temperature","rr_draw_log","tmrel_draw")]
      colnames(shifts)[colnames(shifts)=="rr_draw"] <- "rr_shift"

      RISKS<-merge(RISKS,shifts,by=c("annual_temperature"))
      RISKS[,rr_draw_shift:=rr_draw/rr_shift]

      ### merge, temperature data, risks and TMREL
      MATRIX<-as.data.table(merge(TEMP,RISKS,by=c("annual_temperature","daily_temperature"),all.x=TRUE))
      MATRIX[,":="(x=round(x,digits = 2),y=round(y,digits = 2))]
      pop[,":="(x=round(x,digits = 2),y=round(y,digits = 2))]
      MATRIX<-merge(MATRIX,pop,by=c("x","y"),all.x=TRUE)
      #print("line 209")

      if (npop>1&any(is.na(pop$population))==TRUE){MATRIX[,population:=na.approx(population,na.rm=FALSE),by="date"]}
      MATRIX$population[is.na(MATRIX$population)] <- 0
      MATRIX[,population:=ifelse(all(population==0),1,population)]  1

      MATRIX<-MATRIX[!is.na(tmrel_draw)];MATRIX<-MATRIX[!is.na(temp_draw)];MATRIX<-MATRIX[!is.na(rr_draw_shift)]


      ### calculate heat and cold PAFs
      MATRIX[,PAF.cold:=(mapply(fcoldPAF,temp_draw,tmrel_draw,rr_draw_shift))]
      MATRIX[,PAF.heat:=(mapply(fheatPAF,temp_draw,tmrel_draw,rr_draw_shift))]
      MATRIX$PAF.cold[is.na(MATRIX$PAF.cold)] <- 0
      MATRIX$PAF.heat[is.na(MATRIX$PAF.heat)] <- 0


      #### if all population data is missing replace everything with 1
      if(all(is.na(MATRIX$population))==TRUE){
        MATRIX[,":="(population=1)]
      }


      MATRIX[,popsum:=sum(population,na.rm=TRUE),by=.(date)]
      MATRIX[,popfrac:=population/popsum]
      MATRIX[,":="(pwgPAF.heat=PAF.heat*popfrac,pwgPAF.cold=PAF.cold*popfrac)]

      ###### if there is no population data or only one pixel of pop data do not population weigh
      if(npop==0|npop==1|all(MATRIX$population==0)|all(MATRIX$population==1)){
        MATRIX[,":="(pwgPAF.heat=PAF.heat*1,pwgPAF.cold=PAF.cold*1)]
      }

      ### Aggregate daily PAFs to annual PAFs
      popwgtPAF<-MATRIX[,.(heat=sum(pwgPAF.heat,na.rm = TRUE),cold=sum(pwgPAF.cold,na.rm = TRUE)),by="date"]

      ### For PAFs that weren't population weighted
      if(npop==0|npop==1|all(MATRIX$population==0)|all(MATRIX$population==1)){
        popwgtPAF<-MATRIX[,.(heat=mean(pwgPAF.heat,na.rm = TRUE),cold=mean(pwgPAF.cold,na.rm = TRUE)),by="date"]
      }

      ann_popwgtPAF<-popwgtPAF[,.(mean(heat,na.rm = TRUE),mean(cold,na.rm = TRUE))]
      colnames(ann_popwgtPAF)<-c("heatPAF","coldPAF")

      PAFS<-rbind(PAFS,ann_popwgtPAF)
      print(i)

    }


    age_groups<-c(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)

    PAF.1<-data.table(measure_id=4, age_group_id = age_groups, sex_id = 1, year_id=YEAR, location_id=LOCATION_ID, acause=ACAUSE[k], cause_id=CAUSE_IDS[k])
    PAF.2<-data.table(measure_id=4, age_group_id = age_groups, sex_id = 2, year_id=YEAR, location_id=LOCATION_ID, acause=ACAUSE[k], cause_id=CAUSE_IDS[k])

    print("end 147")
    PAF.COLD<-rbind(PAF.1,PAF.2)
    pafCOLD<-data.table(t(c(PAFS[,2])$coldPAF))
    colnames(pafCOLD)<-paste0("draw_",0:ndraws)

    paf.cold.mean<-data.table(draw_mean=mean(c(PAFS[,2])$coldPAF,na.rm = TRUE))
    pafCOLD<-as.data.table(cbind(paf.cold.mean,pafCOLD))
    pafCOLD<-pafCOLD[rep(seq_len(nrow(pafCOLD)), 46), ]
    PAF.COLD<-cbind(PAF.COLD, pafCOLD)
    MCOLD<-rbind(MCOLD,PAF.COLD)
    print("LINE 158")


    print("LINE 164")
    PAF.HEAT<-rbind(PAF.1,PAF.2)
    pafHEAT<-data.table(t(c(PAFS[,1])$heatPAF))
    colnames(pafHEAT)<-paste0("draw_",0:ndraws)
    paf.heat.mean<-data.table(draw_mean=mean(c(PAFS[,1])$heatPAF,na.rm = TRUE))
    pafHEAT<-as.data.table(cbind(paf.heat.mean,pafHEAT))
    pafHEAT<-pafHEAT[rep(seq_len(nrow(pafHEAT)), 46), ]
    PAF.HEAT<-cbind(PAF.HEAT, pafHEAT)
    MHEAT<-rbind(MHEAT,PAF.HEAT)
    print("175")


    print(k)
    print(ACAUSE[k])

  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})

}

print("end loop")

##### restructure and save to most detailed cause

Hlevel3_diabetes<-copy(MHEAT)[cause_id==587]
Hlevel4_diabetes_typ1<-copy(Hlevel3_diabetes)[,":="(acause="diabetes_typ1",cause_id=975)]
Hlevel4_diabetes_typ2<-copy(Hlevel3_diabetes)[,":="(acause="diabetes_typ2",cause_id=976)]
Clevel3_diabetes<-copy(MCOLD)[cause_id==587]
Clevel4_diabetes_typ1<-copy(Clevel3_diabetes)[,":="(acause="diabetes_typ1",cause_id=975)]
Clevel4_diabetes_typ2<-copy(Clevel3_diabetes)[,":="(acause="diabetes_typ2",cause_id=976)]
print("line 285")

Hlevel3_ckd<-MHEAT[cause_id==589]
Hlevel4_ckd_diabetes_typ1<-copy(Hlevel3_ckd)[,":="(acause="ckd_diabetes_typ1",cause_id=997)]
Hlevel4_ckd_diabetes_typ2<-copy(Hlevel3_ckd)[,":="(acause="ckd_diabetes_typ2",cause_id=998)]
Hlevel4_ckd_htn<-copy(Hlevel3_ckd)[,":="(acause="ckd_htn",cause_id=591)]
Hlevel4_ckd_glomerulo<-copy(Hlevel3_ckd)[,":="(acause="ckd_glomerulo",cause_id=592)]
Hlevel4_ckd_other<-copy(Hlevel3_ckd)[,":="(acause="ckd_other",cause_id=593)]
Clevel3_ckd<-MCOLD[cause_id==589]
Clevel4_ckd_diabetes_typ1<-copy(Clevel3_ckd)[,":="(acause="ckd_diabetes_typ1",cause_id=997)]
Clevel4_ckd_diabetes_typ2<-copy(Clevel3_ckd)[,":="(acause="ckd_diabetes_typ2",cause_id=998)]
Clevel4_ckd_htn<-copy(Clevel3_ckd)[,":="(acause="ckd_htn",cause_id=591)]
Clevel4_ckd_glomerulo<-copy(Clevel3_ckd)[,":="(acause="ckd_glomerulo",cause_id=592)]
Clevel4_ckd_other<-copy(Clevel3_ckd)[,":="(acause="ckd_other",cause_id=593)]


Hlevel3_inj_homicide<-copy(MHEAT)[cause_id==724]
Hlevel4_inj_homicide_gun<-copy(Hlevel3_inj_homicide)[,":="(acause="inj_homicide_gun",cause_id=725)]
Hlevel4_inj_homicide_knife<-copy(Hlevel3_inj_homicide)[,":="(acause="inj_homicide_knife",cause_id=726)]
Hlevel4_inj_homicide_sexual<-copy(Hlevel3_inj_homicide)[,":="(acause="inj_homicide_sexual",cause_id=941)]
Hlevel4_inj_homicide_other<-copy(Hlevel3_inj_homicide)[,":="(acause="inj_homicide_other",cause_id=727)]
Clevel3_inj_homicide<-copy(MCOLD)[cause_id==724]
Clevel4_inj_homicide_gun<-copy(Clevel3_inj_homicide)[,":="(acause="inj_homicide_gun",cause_id=725)]
Clevel4_inj_homicide_knife<-copy(Clevel3_inj_homicide)[,":="(acause="inj_homicide_knife",cause_id=726)]
Clevel4_inj_homicide_sexual<-copy(Clevel3_inj_homicide)[,":="(acause="inj_homicide_sexual",cause_id=941)]
Clevel4_inj_homicide_other<-copy(Clevel3_inj_homicide)[,":="(acause="inj_homicide_other",cause_id=727)]


Hlevel3_inj_mech<-copy(MHEAT)[cause_id==704]
Hlevel4_inj_mech_gun<-copy(Hlevel3_inj_mech)[,":="(acause="inj_mech_gun",cause_id=705)]
Hlevel4_inj_mech_other<-copy(Hlevel3_inj_mech)[,":="(acause="inj_mech_other",cause_id=707)]
Clevel3_inj_mech<-copy(MCOLD)[cause_id==704]
Clevel4_inj_mech_gun<-copy(Clevel3_inj_mech)[,":="(acause="inj_mech_gun",cause_id=705)]
Clevel4_inj_mech_other<-copy(Clevel3_inj_mech)[,":="(acause="inj_mech_other",cause_id=707)]

Hlevel3_inj_trans_road<-copy(MHEAT)[cause_id==689]
Hlevel4_inj_trans_road_pedest<-copy(Hlevel3_inj_trans_road)[,":="(acause="inj_trans_road_pedest",cause_id=690)]
Hlevel4_inj_trans_road_pedal<-copy(Hlevel3_inj_trans_road)[,":="(acause="inj_trans_road_pedal",cause_id=691)]
Hlevel4_inj_trans_road_2wheel<-copy(Hlevel3_inj_trans_road)[,":="(acause="inj_trans_road_2wheel",cause_id=692)]
Hlevel4_inj_trans_road_4wheel<-copy(Hlevel3_inj_trans_road)[,":="(acause="inj_trans_road_4wheel",cause_id=693)]
Hlevel4_inj_trans_road_other<-copy(Hlevel3_inj_trans_road)[,":="(acause="inj_trans_road_other",cause_id=694)]
Clevel3_inj_trans_road<-copy(MCOLD)[cause_id==689]
Clevel4_inj_trans_road_pedest<-copy(Clevel3_inj_trans_road)[,":="(acause="inj_trans_road_pedest",cause_id=690)]
Clevel4_inj_trans_road_pedal<-copy(Clevel3_inj_trans_road)[,":="(acause="inj_trans_road_pedal",cause_id=691)]
Clevel4_inj_trans_road_2wheel<-copy(Clevel3_inj_trans_road)[,":="(acause="inj_trans_road_2wheel",cause_id=692)]
Clevel4_inj_trans_road_4wheel<-copy(Clevel3_inj_trans_road)[,":="(acause="inj_trans_road_4wheel",cause_id=693)]
Clevel4_inj_trans_road_other<-copy(Clevel3_inj_trans_road)[,":="(acause="inj_trans_road_other",cause_id=694)]

Hlevel3_cvd_stroke<-copy(MHEAT)[cause_id==494]
Hlevel4_cvd_stroke_isch<-copy(Hlevel3_cvd_stroke)[,":="(acause="cvd_stroke_isch",cause_id=495)]
Hlevel4_cvd_stroke_cerhem<-copy(Hlevel3_cvd_stroke)[,":="(acause="cvd_stroke_cerhem",cause_id=496)]
Hlevel4_cvd_stroke_subhem<-copy(Hlevel3_cvd_stroke)[,":="(acause="cvd_stroke_subhem",cause_id=497)]
Clevel3_cvd_stroke<-copy(MCOLD)[cause_id==494]
Clevel4_cvd_stroke_isch<-copy(Clevel3_cvd_stroke)[,":="(acause="cvd_stroke_isch",cause_id=495)]
Clevel4_cvd_stroke_cerhem<-copy(Clevel3_cvd_stroke)[,":="(acause="cvd_stroke_cerhem",cause_id=496)]
Clevel4_cvd_stroke_subhem<-copy(Clevel3_cvd_stroke)[,":="(acause="cvd_stroke_subhem",cause_id=497)]


Hlevel3_inj_suicide<-copy(MHEAT)[cause_id==718]
Hlevel4_inj_suicide_firearm<-copy(Hlevel3_inj_suicide)[,":="(acause="inj_suicide_firearm",cause_id=721)]
Hlevel4_inj_suicide_other<-copy(Hlevel3_inj_suicide)[,":="(acause="inj_suicide_other",cause_id=723)]
Clevel3_inj_suicide<-copy(MCOLD)[cause_id==718]
Clevel4_inj_suicide_firearm<-copy(Clevel3_inj_suicide)[,":="(acause="inj_suicide_firearm",cause_id=721)]
Clevel4_inj_suicide_other<-copy(Clevel3_inj_suicide)[,":="(acause="inj_suicide_other",cause_id=723)]

print("line 351")

MHEAT_new<-MHEAT[cause_id!=587]
MHEAT_new<-MHEAT_new[cause_id!=589]
MHEAT_new<-MHEAT_new[cause_id!=724]
MHEAT_new<-MHEAT_new[cause_id!=704]
MHEAT_new<-MHEAT_new[cause_id!=689]
MHEAT_new<-MHEAT_new[cause_id!=494]
MHEAT_new<-MHEAT_new[cause_id!=718]

MCOLD_new<-MCOLD[cause_id!=587]
MCOLD_new<-MCOLD_new[cause_id!=589]
MCOLD_new<-MCOLD_new[cause_id!=724]
MCOLD_new<-MCOLD_new[cause_id!=704]
MCOLD_new<-MCOLD_new[cause_id!=689]
MCOLD_new<-MCOLD_new[cause_id!=494]
MCOLD_new<-MCOLD_new[cause_id!=718]

print("line 369")

HLEVEL4<-rbind(Hlevel4_diabetes_typ1,Hlevel4_diabetes_typ2,
               Hlevel4_ckd_diabetes_typ1,Hlevel4_ckd_diabetes_typ2, Hlevel4_ckd_htn, Hlevel4_ckd_glomerulo, Hlevel4_ckd_other,
               Hlevel4_inj_homicide_gun,Hlevel4_inj_homicide_knife,Hlevel4_inj_homicide_sexual,Hlevel4_inj_homicide_other,
               Hlevel4_inj_mech_gun,Hlevel4_inj_mech_other,
               Hlevel4_inj_trans_road_pedest,Hlevel4_inj_trans_road_pedal,Hlevel4_inj_trans_road_2wheel,Hlevel4_inj_trans_road_4wheel,Hlevel4_inj_trans_road_other,
               Hlevel4_cvd_stroke_isch,Hlevel4_cvd_stroke_cerhem, Hlevel4_cvd_stroke_subhem,
               Hlevel4_inj_suicide_firearm,Hlevel4_inj_suicide_other)

CLEVEL4<-rbind(Clevel4_diabetes_typ1,Clevel4_diabetes_typ2,
               Clevel4_ckd_diabetes_typ1,Clevel4_ckd_diabetes_typ2, Clevel4_ckd_htn, Clevel4_ckd_glomerulo, Clevel4_ckd_other,
               Clevel4_inj_homicide_gun,Clevel4_inj_homicide_knife,Clevel4_inj_homicide_sexual,Clevel4_inj_homicide_other,
               Clevel4_inj_mech_gun,Clevel4_inj_mech_other,
               Clevel4_inj_trans_road_pedest,Clevel4_inj_trans_road_pedal,Clevel4_inj_trans_road_2wheel,Clevel4_inj_trans_road_4wheel,Clevel4_inj_trans_road_other,
               Clevel4_cvd_stroke_isch,Clevel4_cvd_stroke_cerhem, Clevel4_cvd_stroke_subhem,
               Clevel4_inj_suicide_firearm,Clevel4_inj_suicide_other)

print("line 387")

MMHEAT<-as.data.table(rbind(MHEAT_new,HLEVEL4))
MMCOLD<-as.data.table(rbind(MCOLD_new,CLEVEL4))
print("line 391")


if(any(is.na(MMHEAT[,paste0(c("draw_mean","draw_",0:ndraws))]))==TRUE){
  file[,c("draw_mean",paste0("draw_",0:ndraws)):=lapply(.SD,function(x){replace(x,is.na(x),rowMeans(file[,paste0("draw_",0:ndraws)],na.rm = TRUE))}),.SDcols=c("draw_mean",paste0("draw_",0:ndraws))]
}

if(any(is.na(MMCOLD[,c("draw_mean",paste0("draw_",0:ndraws))]))==TRUE){
  file[,c("draw_mean",paste0("draw_",0:ndraws)):=lapply(.SD,function(x){replace(x,is.na(x),rowMeans(file[,paste0("draw_",0:ndraws)],na.rm = TRUE))}),.SDcols=c("draw_mean",paste0("draw_",0:ndraws))]
}



write.csv(MMHEAT,paste0(j,"FILEPATH/heat/",LOCATION_ID,"_",YEAR,".csv"), row.names = FALSE)
write.csv(MMCOLD,paste0(j,"FILEPATH/cold/",LOCATION_ID,"_",YEAR,".csv"), row.names = FALSE)

print("end")

