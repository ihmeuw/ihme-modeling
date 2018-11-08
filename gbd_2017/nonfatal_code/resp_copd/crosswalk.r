
## CROSSWALK FUNCTION

crosswalk <- function(dt, acause, ref, alts, bundle_xwalk){
    
    library(plyr)
    library(data.table)

    ## PREP MATCHED X-WALK DATA ------------------------------------------------------------
  
    ## Input data
    xwalk_data <- as.data.table(get_epi_data(bundle_id = bundle_xwalk))
    xwalk_data <- xwalk_data[,c("case_definition","nid","age_start","age_end","sex","location_id","location_name","sample_size","mean")]

    ## rename mean to mean_value to keep R straight ("mean" has meaning in R already)
    setnames(xwalk_data,"mean","mean_value")

    ## logit transform mean_value
    xwalk_data[,mean_value:=log(mean_value/(1-mean_value))]

    ## Reshape data wide by case definition
    xwalk_data <- dcast.data.table(xwalk_data, nid + age_start + age_end + sex +location_id + location_name + sample_size ~ case_definition, value.var = "mean_value")

    ## Generate age_mid variable (independent var in regression)
    xwalk_data[, age_mid := ((age_start+age_end)/2)]

    ## LOAD & PREP PRE-CROSSWALK BUNDLE -----------------------------------------------------

    dt <- as.data.table(dt)
    n <- nrow(dt)
    dt$id <- seq(1,n,1)
    pre_data <- as.data.table(dt)
    pre_data <-pre_data[measure=="prevalence",] ## just crosswalk prevalence data

    ## rename mean to mean_value to keep R straight ("mean" has meaning in R already)
    setnames(pre_data,"mean","mean_value")

    ## Add original unadjusted mean to note_modeler column for reference
    pre_data[case_diagnostics %in% alts & !is.na(note_modeler), note_modeler := paste0(note_modeler, " | unadjusted mean : ", mean_value)]
    pre_data[case_diagnostics %in% alts & is.na(note_modeler), note_modeler := paste0("unadjusted mean : ", mean_value)]

    ## Generate age_mid variable
    pre_data[, age_mid := (age_start+age_end)/2]

    ## save pre_data as post_data to preserve pre_data as unadjusted
    post_data <- copy(pre_data)

    ## logit transform mean_value
    post_data[,mean_value:=log(mean_value/(1-mean_value))]

    ## LOOP OVER ALT DEFINITION -------------------------------------------------------------

    for(alt in alts){
  
      #Subset xwalk data to rows with column entries for both ref and alt
      xwalk_subset <- xwalk_data[!is.na(get(ref)) & !is.na(get(alt)),]
  
      if(nrow(xwalk_subset)>5){
    
        print(paste0("Crosswalking ", alt))
    
        # Perform regression and save as a function
        regression <- summary(lm(get(ref)~get(alt)+age_mid, data=xwalk_subset))
        print(regression)
        B0 <- regression$coefficients[1,1]
        B1 <- regression$coefficients[2,1]
        B2 <- regression$coefficients[3,1]
        crosswalk <- function(mean, age){
          return(B0+B1*mean+B2*age)
        }
    
        # Apply regression to data
        post_data[case_diagnostics==alt, mean_value_new:=crosswalk(mean_value, age_mid)]
        post_data[case_diagnostics==alt, case_diagnostics:=paste0(alt,", crosswalked")]
    
      } else {
        print(paste0("There are insuffcient observations to derive a crosswalk between ", alt, " and ", ref))
      }
      
    }

    post_data[!is.na(mean_value_new), mean_value:=mean_value_new]
    post_data[,c("mean_value_new"):=NULL]

    # Reverse the logit transformation
    post_data[,mean_value:=exp(mean_value)/(exp(mean_value)+1)]

    post_data[mean_value>0,c("lower","upper", "uncertainty_type_value"):=NA]
    post_data[standard_error>=1, standard_error:=0.999]
    post_data[mean_value==0, standard_error:=NA]
    post_data <- post_data[mean_value!=0 | upper!=0,]

    ## Rename mean_value to mean
    setnames(post_data,"mean_value","mean")
    
    crosswalked_ids <- unique(post_data$id)
    not_crosswalked_rows <- dt[(id %in% crosswalked_ids)==F,]
    dt <- rbind.fill(post_data,not_crosswalked_rows)
    dt <- as.data.table(dt)
    dt[,id:=NULL]
    
    return(dt)

}