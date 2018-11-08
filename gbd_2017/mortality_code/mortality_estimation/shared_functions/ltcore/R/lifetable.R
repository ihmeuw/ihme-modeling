#' Generate a full life-table, given mx, ax, and qx
#'
#' Given a data.table with variables mx, ax, and qx, compute a full life table
#'
#' @param data data.frame or data.table with variables: qx, mx, and ax. Must include age or age_group_id, sex or sex_id, and year or year_id. Finally, \strong{data must include a variable named "id" that contains the concatenated values of all other variables that, combined with age, sex, and year, must uniquely identify the data}. For example, id may contain location_id or draw #, or a concatenation of the two (e.g. "{location_id}_{draw}"). 
## make sure id, sex, year, and age uniquely identify observations
#' @param preserve_u5 numeric (0 for no, 1 for yes), whether to preserve under-5 qx estimates rather than reclaculating them based on mx and ax
#' @param cap_qx numeric (0 for no, 1 for yes), whether to cap qx values at 1 
#' @param force_cap logical, whether to enforce qx capping even if qx > 1.5 values exist (potentially indicative of a significant issue in the input data)
#' @param qx_diag_threshold numeric, optional. Whether to return a data table of qx values over a given threshold when encountered while performing lifetable calculations by age, year_id, sex_id.
#' 
#' @return data.frame or data.table with additional variables px, lx, Tx, nLx, if qx_diag_thresholds is TRUE, returns a list with the life table as the first element, and the diagnostic file as the second.
#' @import data.table
#' @import mortdb
#' @export
#' @examples
#' \dontrun{
#' test_lt <- data.table(age = c(0,1,seq(5,110,5)), mx = .05, ax = 2.5, qx = .05, year = 1990, sex = "male", id = 1)
#' test_lt[age == 20, mx := .2]
#' full_lt <- lifetable(test_lt)
#' test2 <- ltcore::lifetable(test_lt)
#' setorder(full_lt, age)
#' all.equal(full_lt, test2)
#' # Timing:
#' large_lt <- rbindlist(lapply(1:20000, function(x) {
#'    test2 <- copy(test_lt)
#'    test2[, id := x]
#'  }))
#' system.time(full_lt <- lifetable(large_lt))
#' system.time(test2 <- ltcore::lifetable(large_lt))
#' }

lifetable <- function(data,  preserve_u5=0, cap_qx=0, force_cap=F, qx_diag_threshold=NULL) {

  ## checking if data table or data frame to return object of same format
  dframe <- F
  if(!is.data.table(data)){
    dframe <- T
    data <- data.table(data)
  }
  
  ## make flexible with regard to age vs age_group_id, check to make sure all the correct age groups are present
  age_group_id <- F
  lt_ages <- data.table(get_age_map(type="lifetable"))
  if("age_group_id" %in% names(data)){
    age_group_id <- T
    if("age" %in% names(data)) stop ("You have more than one age variable, delete one and try again")
    lt_ages <- lt_ages[,list(age_group_id, age_group_name_short)]
    if(!isTRUE(all.equal(unique(sort(data$age_group_id)), unique(lt_ages$age_group_id)))) stop("You don't have the correct age groups")
    data <- merge(lt_ages, data, by=c("age_group_id"), all.y=T)
    setnames(data, "age_group_name_short", "age")
  } 
  
  ## make flexible with regard to sex and sex_id
  sex_id<-F
  if("sex_id" %in% names(data)){
    sex_id <- T
    if("sex" %in% names(data)) stop ("You have more than one sex identifier, delete one and try again")
    data[sex_id==1, sex:="male"]
    data[sex_id==2, sex:="female"]
    data[sex_id==3, sex:="both"]
    data[,"sex_id":=NULL]
  }
  
  ## make flexible with regard to year and year_id
  year_id <- F
  if(!"year" %in% names(data)){
    year_id <- T
    setnames(data, "year_id", "year")
  }
  
  ## order data just in case
  data <- data[order(id, sex, year, age)]
  if("qx" %in% names(data)){
    data[,qx:=as.numeric(qx)]
  }
  data[,mx:=as.numeric(mx)]
  data[,ax:=as.numeric(ax)]
  data[,age:=as.numeric(age)]
  if(typeof(data$sex) != "character") data[,sex:=as.character(sex)]
  
  ## can set up more assurances here (certain things uniquely identify, etc.)
  setkeyv(data,c("id", "sex", "year", "age"))
  num_duplicates <- length(duplicated(data)[duplicated(data)==T])
  if(num_duplicates>0) stop(paste0("You have ",num_duplicates," duplicates of your data over id_vars"))
  
  ## get length of intervals
  # data[,n:=unlist(tapply(age, list(id, sex, year), function(x) c(x[-1], max(x))-x))]
  gen_age_length(data)
  setnames(data, "age_length", "n")
  
  ## qx
  if (preserve_u5 == 1) {
    data[age>1, qx:=(n*mx)/(1+(n-ax)*mx)]
  }
  if (preserve_u5 == 0){
    data[,qx:=(n*mx)/(1+(n-ax)*mx)]
  } 
  
  ## setting qx to be 1 for the terminal age group  
  data[age==max(age), qx:=1]
  
  if (cap_qx == 1) {
    if (!is.null(qx_diag_threshold)) {
      qx_over_threshold <- data[qx > qx_diag_threshold]
    }
    if (nrow(data[qx > 1.5]) > 0 & force_cap==F) {
      stop(paste0("Are you sure you want to cap_qx? Some values of qx are greater than 1.5"))
    } else {
      data[qx>1, qx:=0.9999]
    }
  } else {
    if (nrow(data[qx > 1]) > 0) stop(paste0("Probabilities of death over 1, re-examine data, or use cap option. Max qx value is"), max(data$qx))
  }
  
  ## px
  data[,px:=1-qx]
  
  ## lx
  data[,lx:=0]
  data[age==0, lx:=100000]
  for (i in 1:length(unique(data$age))) {
    temp <- NULL
    temp <- data$lx*data$px
    temp <- c(0,temp[-length(temp)])
    data[,lx:=0]
    data[,lx:=lx+temp]
    data[age==0, lx:=100000]
  }
  
  ## dx
  setkey(data,id,sex,year,age)
  setnames(data, "n", "age_length")
  lx_to_dx(data)
  
  ## nLx
  gen_nLx(data)
  
  ## Tx
  gen_Tx(data, id_vars = c("id", "sex", "year", "age"))
  
  ## ex
  gen_ex(data)

  setnames(data, "age_length", "n")
  
  ## returning in same format
  if(sex_id==T){
    data[sex=="male", sex_id:=1]
    data[sex=="female", sex_id:=2]
    data[sex=="both", sex_id:=3]
    data[,sex:=NULL]
  }

  if(age_group_id==T){
    #     setnames(data, "age_group_name_short", "age")
    #     data <- merge(data, lt_ages, by=c("age_group_name_short"), all.x=T)
    data[,age:=NULL]
  }
  
  if(year_id==T){
    setnames(data, "year", "year_id")
  }

  if(dframe==T){
    data <- as.data.frame(data)
  }
  
  if (!is.null(qx_diag_threshold)) {
    return(list(data, qx_over_threshold))
  } else {
    return(data)
  }
  
}

