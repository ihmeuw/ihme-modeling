
string_passed <- as.character(commandArgs())
print(string_passed)
write_dir <- string_passed[4]
data <- string_passed[5]
type <- string_passed[6]


print(data)
path <- paste0(write_dir)
df_in <- read.csv(paste0(path, data, "_", type, "_survey_in.csv"))

if(min(df_in$pweight)<=0){
  df_in$pweight=1
}

library(survey, lib = 'FILEPATH')
survey_function<- function(str_val, df){


  print(str_val)
  print(head(df))

  df$age_start <- as.factor(df$age_start)
  df$age_end <- as.factor(df$age_end)
  df$sex <- as.factor(df$sex)
  df$pweight <- as.numeric(df$pweight)
  df[,str_val] <- as.numeric(as.character(df[,str_val]))
  try(design <- svydesign(id=~psu, strata=~strata, weights=~pweight,data=df), silent=TRUE)
  if (!exists(("design"))){
    try(design <- svydesign(id=~psu, strata=~strata, weights=~pweight,data=df, nest=TRUE))
  }

  try(final <- as.data.frame(svyby(as.formula(paste0("~", str_val)), ~age_start+age_end+sex,design,  svymean, na.rm=TRUE)))


  if(!exists("final")){
    if (exists(("design"))){
      rm(design)
    }
    df$psu <- 1:nrow(df)
    df$strata <- 1

    try(design <- svydesign(id=~psu, strata=~strata, weights=~pweight,data=df), silent=TRUE)
    if (!exists(("design"))){
      try(design <- svydesign(id=~psu, strata=~strata, weights=~pweight,data=df, nest=TRUE))
    }


    final <- as.data.frame(svyby(as.formula(paste0("~", str_val)), ~age_start+age_end+sex,design,  svymean, na.rm=TRUE))
  }
  rownames(final) <- NULL
  colnames(final)[4] <- "mean"
  return(final)
}

df_util <- df_in[!is.na(df_in[,"util_var"]),]
system.time(util <- survey_function(str_val="util_var", df= df_util))
write.csv(util, paste0(path, data, "_", type, "_util_var.csv"))


if(sum(is.na(df_in$num_visit))!=nrow(df_in)){

  df_visit <- df_in[!is.na(df_in[,'num_visit']),]
  visit <- survey_function(str_val="num_visit", df= df_visit)
  write.csv(visit, paste0(path, data, "_", type, "_num_visit.csv"))

  df_visit_ever <- df_visit[df_visit$num_visit>0,]
  visit_ever <- survey_function(str_val="num_visit", df= df_visit_ever)
  write.csv(visit_ever, paste0(path, data,"_", type, "_num_visit_ever.csv"))

}

