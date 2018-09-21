
  rm(list=ls()); library(foreign)
  if (Sys.info()[1] == 'Windows') {
    username <- "USER"
    root <- "FILEPATH"
    workdir <-  paste("FILEPATH",username,"FILEPATH",sep="")
    source("FILEPATH/get_locations.r")
  } else {
    username <- Sys.getenv("USER")
    root <- "/home/j/"
    workdir <- paste("FILEPATH",username,"FILEPATH",sep="")
    source("FILEPATH/get_locations.r")
  }
  
  print(paste0(workdir))
  test <- F      
  start <- 8     
  use_ctemp <- 1    
  err_out <- T

  ## get locations
  codes <- get_locations(level="estimate", gbd_year=2016)
  

  source(paste0(root, "FILEPATH/check_loc_results.r"))

  ## get age-sex combinations
  sex_input <- c("male", "female")
  age_input <- c("enn", "lnn", "pnn", "inf", "ch")
  
  ## errors
  errout <- paste0("-o FILEPATH",username,
                   "/output -e FILEPATH",username,"/errors")


  if (!test) {
    setwd("FILEPATH")
    if (start<=1 & fin >=1)   for (ff in dir("FILEPATH")) if(ff!="archive" & ff!= "addtional_sources") file.remove(paste("FILEPATH/", ff, sep=""))
    if (start<=2 & fin >=2) for (ff in dir("FILEPATH")) if(ff!="archive") file.remove(paste("FILEPATH/", ff, sep=""))

    setwd("FILEPATH")
    if (start<=2 & fin >=2) for (ff in dir("FILEPATH")) if(ff!="archive") file.remove(paste("FILEPATH/", ff, sep=""))
    if (start<=3 & fin >=3) for (ff in dir("FILEPATH")) if(ff!="archive") file.remove(paste("FILEPATH/", ff, sep=""))
  }

  setwd(workdir)

  
  
locs_for_stata <- get_locations(level="estimate")
write.csv(locs_for_stata, paste0(root, "FILEPATH/as_locs_for_stata.csv"), row.names=F)
  
subnats <- get_locations(level="subnational")
write.csv(subnats, paste0(root, "FILEPATH/as_subnats_for_stata.csv"), row.names=F)

cplus_locs <- get_locations()
write.csv(cplus_locs, paste0(root, "FILEPATH/as_cplus_for_stata.csv"), row.names=F)

############
## Submit jobs
############

  
## Step 1: Compile Data [

if (start<=1 & fin >=1) {
  jname <- "as01"
  slots <- 4
  mem <- slots*2
  holds <- "m10"
  sys.sub <- paste0("qsub ",ifelse(err_out,paste0(errout),paste0(""))," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",holds," ")
  args <- paste0(username)
  shell <- "stata_shell.sh"
  script <- "01_compile_data.do"
  print(paste(sys.sub, shell, script, "\"", args, "\""))
  system(paste(sys.sub, shell, script, "\"", args, "\""))
} 
  
## Step 2: Fit the age and sex models 

if (start<=2 & fin >=2) {
  slots <- 3
  mem <- slots*2
    jname <- paste0("as02_fit")
    holds <- "as01"
    sys.sub <- paste0("qsub ",ifelse(err_out,paste0(errout),paste0(""))," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",holds," ")
    args <- paste(workdir,use_ctemp,sep=" ")
    shell <- "stata_shell.sh"
    script <- "02_fit_models.do"
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    system(paste(sys.sub, shell, script, "\"", args, "\""))
}
  
## Step 3: Predicting the Sex Model, stage 1 linear regression
if (start<=3 & fin >=3) {
  step3_dir <- "FILEPATH"
  setwd(step3_dir)
  for (ff in dir(step3_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  step3_cluster_dir <- "FILEPATH"
  setwd(step3_cluster_dir)
  for (ff in dir(step3_cluster_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  setwd(workdir)
  slots <- 2
  mem <- slots*2
  holds <- paste("-hold_jid \"",paste(c("as02_fit"),collapse=","),"\"",sep="")
  jobids <- NULL
  
  for (loc in unique(codes$ihme_loc_id)) {
    jname <- paste0("as03_",loc)
    sys.sub <- paste0("qsub ",ifelse(err_out,paste0(errout),paste0(""))," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
    args <- paste0(workdir," ",loc," ",use_ctemp," ",codes$location_id[codes$ihme_loc_id == loc])
    shell <- "stata_shell.sh"
    script <- "03_predict_sex_model_stage1.do"
    system(paste(sys.sub, shell, script, "\"", args, "\""))  
    jobids <- c(jobids,jname)
  }
}

## Step 4: Predicting Stage 2 Sex Model (Space-Time)
if (start<=4 & fin >=4) {
  step4_dir <- "FILEPATH"
  setwd(step4_dir)
  for (ff in dir(step4_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  step4_cluster_dir <- "FILEPATH"
  setwd(step4_cluster_dir)
  for (ff in dir(step4_cluster_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  setwd(workdir)
  if (start > 3) jobids <- "03fakejob"
  jname <- "as04_sexmod2"
  slots <- 3
  mem <- slots*2
  args <- paste0("no")
  holds <- paste("-hold_jid \"",paste(jobids,collapse=","),"\"",sep="")
  sys.sub <- paste0("qsub ",ifelse(err_out,paste0(errout),paste0(""))," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
  shell <- "r_shell.sh"
  script <- "04_predict_sex_model_stage2.R"
  system(paste(sys.sub, shell, script, "\"", args, "\""))
}


## Step 5: Predicting Stage 3 Sex Model (GPR)
if (start <=5 & fin >=5) {  
  step5_dir <- "FILEPATH"
  setwd(step5_dir)
  for (ff in dir(step5_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  step5_cluster_dir <- "FILEPATH"
  setwd(step5_cluster_dir)
  for (ff in dir(step5_cluster_dir, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  setwd(workdir)
  jobids <- c()
  for (loc in unique(codes$ihme_loc_id)) {
    reg <- gsub(" ","_",codes$region_name[codes$ihme_loc_id==loc])
    holds <- "as04_sexmod2"
    jname <- paste0("gpr_",loc)
    slots <- 6
    mem <- slots*2
    args <- paste0(reg," ",loc," ",username)
    sys.sub <- paste0("qsub ",paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",holds)
    shell <- "python_shell.sh"
    script <- "05_predict_sex_model_gpr.py"
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    system(paste(sys.sub, shell, script, "\"", args, "\""))
    jobids <- c(jobids, jname)
  }
}
  
## Step 6: Predicting Stages 1 and 2 of the age model
if(start<=6 & fin>=6){
  
  step6_dira <- "FILEPATH"
  setwd(step6_dira)
  for (ff in dir(step6_dira, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  step6_dirb <- "FILEPATH"
  setwd(step6_dirb)
  for (ff in dir(step6_dirb, full.names=T)) if(ff!="archive" & !test) file.remove(paste0(ff))
  
  step6_dir_cluster1 <- "FILEPATH"
  setwd(step6_dir_cluster1)
  for (ff in dir(step6_dir_cluster1, full.names=T)) if(ff!=paste0(step6_dir_cluster1, "/archive") & !test) file.remove(paste0(ff))
  
  step6_dir_cluster2 <- "FILEPATH"
  setwd(step6_dir_cluster2)
  for (ff in dir(step6_dir_cluster2, full.names=T)) if(ff!=paste0(step6_dir_cluster2, "/archive") & !test) file.remove(paste0(ff))
  
  setwd(workdir)
  
  if (start > 2) {
    jobids <- "03fakejob"
  }

for(sex in sex_input){
    jname <- paste0("as06_agemod",sex)
    slots <- 3
    mem <- slots*2
    args <- paste0(sex)
    holds <- paste("-hold_jid \"",paste(jobids,collapse=","),"\"",sep="")
     sys.sub <- paste0("qsub ",ifelse(err_out,paste0(errout),paste0(""))," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
    shell <- "r_shell.sh"
    script <- "06_predict_age_model_stages1_2.R"
    system(paste(sys.sub, shell, script, "\"", args, "\""))
    }
}
  

## Step 7: Predicting GPR (Stage 3) of the age model
  if (start <=7 & fin >=7) {  
    for(age in age_input){
      for(sex in sex_input){
        step7_dir <- paste0("FILEPATH", sex, "_", age)
        system(paste0("perl -e 'unlink <", step7_dir, "/gpr_*.txt>' "))
      }
    }
    
    ifelse(start<7, holds <- c("as06_agemodmale", "as06_agemodfemale"), holds <- "fakejob")
    step7_jobs <- c()
    for (loc in unique(codes$ihme_loc_id)) {
      reg <- gsub(" ","_",codes$region_name[codes$ihme_loc_id==loc])
      for(sex in sex_input){
        for(age in age_input){
          dir.create(paste0("FILEPATH", sex ,"_",age))
          jname <- paste0("gpr_",loc, sex, age)
          step7_jobs <- c(step7_jobs, jname)
          slots <- 6
          mem <- slots*2
          args <- paste(reg, loc ,username, sex, age)
          if(loc=="PRK"){
              sys.sub <- paste0("qsub ",paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",paste(holds,collapse=","))
          } else {
            sys.sub <- paste0("qsub "," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",paste(holds,collapse=","))
          }
          shell <- "python_shell.sh"
          script <- "07_predict_age_model_gpr.py"
          system(paste(sys.sub, shell, script, "\"", args, "\""))
          }
        }
      }
  }

  ## Step 8: Saving and formating for scaling
  if (start <=8 & fin >=8){
    
    step8_dir <- "FILEPATH"
    system(paste0("perl -e 'unlink <", step8_dir, "/scaling_input_*.txt>' "))
    
    data_dir <- "FILEPATH"
    
    for(sex in sex_input){
      for(age in age_input){
        print(paste0(sex, "_", age))
        draw_dir <- paste0(data_dir,"/",sex,"_",age)
        post <- paste0("_",sex,"_",age,".txt")
        check_loc_results(unique(codes$ihme_loc_id),draw_dir,prefix="gpr_",postfix=post)
      }
    }
    
    holds <- "fakejob"
    for (loc in unique(codes$ihme_loc_id)) {
      jname <- paste0("as08_format_",loc)
      slots <- 2
      mem <- slots*2
      args <- paste0(loc)
   
     sys.sub <- paste0("qsub ",paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",paste(holds,collapse=","))
      shell <- "r_shell.sh"
      script <- "08_prep_age_model_for_scaling.R"
      system(paste(sys.sub, shell, script, "\"", args, "\""))
    }
    
  }
  
  ## Step 9: Scaling Age-Sex Results
  
  if (start<=9 & fin >=9) {
    slots <- 4
    mem <- slots*2
    step9_jobs <- c()
    for (loc in unique(codes$ihme_loc_id)) {
      ifelse(start<9, holds <- paste0("as08_format_", loc), holds <- "fakejob")
      jname <- paste0("as09_",loc)
      sys.sub <- paste0("qsub " ,paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",paste(holds,collapse=","))       
      args <- paste0(loc)
      shell <- "stata_shell.sh"
      script <- "09_scale_age_sex.do"
      system(paste(sys.sub, shell, script, "\"", args, "\""))  
      step9_jobs <- c(step9_jobs,jname)
    }
  }
  
  ## Step 10: Compilling age-sex results
  
  if (start<=10 & fin >=10) {
    ifelse(start<10, holds <- step9_jobs, holds <- "fakejob")
    jname <- "as10_compile"
    slots <- 3
    mem <- slots*2
    sys.sub <- paste0("qsub " ,paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",paste(holds,collapse=","))
    args <- paste0(workdir," ",use_ctemp)
    shell <- "stata_shell.sh"
    script <- "10_compile_estimates.do"
    system(paste(sys.sub, shell, script, "\"", args, "\""))
  }