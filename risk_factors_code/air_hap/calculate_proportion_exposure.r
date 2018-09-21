###################
### Setting up ####
###################
require(haven)
require(data.table)
require(dplyr)
require(survey)

###################################################################
# Collapse Blocks
###################################################################


#######################################################################################################################################

collapse_list <- function(df, vars) {

  ## Detect meta
  meta <- setdiff(names(df), vars)
  
  ## Binary for whether or not variable exists and is not completely missing
  out.vars <- sapply(vars, function(x) ifelse(x %in% names(df) & nrow(df[!is.na(x)]) > 0, 1, 0)) %>% t %>% data.table

  return(cbind(out.meta, out.vars))
}


#######################################################################################################################################

setup_design <- function(df, var) {

## Set options

## conservative adjustment recommended by Thomas Lumley for single-PSU strata.  Centers the data for the single-PSU stratum around the sample grand mean rather than the stratum mean
options(survey.lonely.psu = 'adjust')

## conservative adjustment recommended by Thomas Lumley for single-PSU within subpopulations. 
options(survey.adjust.domain.lonely = TRUE)

## Check for survey design vars
check_list <- c("strata", "psu", "pweight")
for (i in check_list) {
  ## Assign to *_formula the variable if it exists and nonmissing, else NULL
  assign(paste0(i, "_formula"),
    ifelse(i %in% names(df) & nrow(df[!is.na(i)]) > 0, paste("~", i), NULL) %>% as.formula
  ) 
}
#browser()
## Set svydesign
return(svydesign(id = psu_formula, weight = pweight_formula, strat = strata_formula, data = df[!is.na(var)], nest = TRUE))

}

#######################################################################################################################################

if(0 == 1) { #test
  aggregate_under5 = TRUE
  tabulate_pregnant = FALSE
  by_vars <- c("nid", "ihme_loc_id", "year_start", "year_end", "sex", "age_group_id", "pregnant")
  var <- "hemoglobin"
  }

collapse_by <- function(df, var, by_vars, calc.sd = FALSE) {
  #browser()
  ## Subset to frame where data isn't missing
  df.c <- copy(df[!is.na(get(var)) & !is.na(strata) & !is.na(psu) & !is.na(pweight)])
    
  ## Setup design
  design <- setup_design(df.c, var)

  ## Setup by the by call as a formula
  by_formula <- as.formula(paste0("~", paste(by_vars, collapse = "+")))

  ## Calculate mean and standard error by by_var(s).  Design effect is dependent on the scaling of the sampling weights
  est = svyby(~get(var), by_formula, svymean, design = design, deff = "replace", na.rm = TRUE, drop.empty.groups = TRUE, keep.names = FALSE, multicore=TRUE)
  setnames(est, c("get(var)", "DEff.get(var)"), c("mean", "deff"))
  
  ## Calculate number of observations, number of clusters, strata
  meta <- df.c[, list(sample_size = length(which(!is.na(get(var)))), 
                      nclust = length(unique(psu)), 
                      nstrata= length(unique(strata)),
                      var = var
                      ), by = by_vars]
     
  
  ## Combine meta with est
  out <- merge(est, meta, by=by_vars)
  setnames(out, c("se", "deff"), c("standard_error", "design_effect"))
  
  # Option of calculating standard deviation. Will do if specified, and detected continuous as not binary  
  if(calc.sd & nrow(df.c[!(get(var) %in% c(0,1,NA))]) > 0) {
    ##Calculate variance & se(variance) then convert to stdev and SE(stdev) 
    stdev = svyby(~get(var), by_formula, svyvar, design = design, deff = "replace", na.rm = TRUE, drop.empty.groups = TRUE, keep.names = FALSE, multicore=TRUE) %>% as.data.table
    setnames(stdev, "get(var)", "variance")
    stdev[, standard_deviation := sqrt(variance)]
      #Calculating standard error of stdev (not variance) as per http://stats.stackexchange.com/questions/156518/what-is-the-standard-error-of-the-sample-standard-deviation
      stdev[, standard_deviation_se := 1 / (2*standard_deviation) * se] 
    stdev[,variance:=NULL]
    stdev[,se:=NULL]

    out <- merge(out, stdev, by=by_vars) %>% as.data.table
   
    }
  
  return(out)                      
  
}

#######################################################################################################################################

