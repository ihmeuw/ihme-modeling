## Generate lambda values through an nLx value produced from lifetable 
##      (nLx produced using ID variables directly from get_life_table output)

## Input:
## source(paste(get_path("j"), <FILE PATH>)
## age_groups <- c(5:20, 28, 30:33, 44:45, 148)
## mx_ax_qx <- setDT(get_life_table(location_set_id = 35, life_table_parameter_id = c(1:3), age_group_id = age_groups, gbd_round_id = 5, with_shock = 1, with_hiv = 1, run_id = 4))


## Note: Since nLx represents person-years lived between age x and x + n, nLx age groups can be added to calculate the total nLx

##############################################
### Load External Functions
##############################################
# Load libraries from the cancer team
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, '/r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(get_path('cdb_utils_r'))

# Load libraries from the mortality team
source(paste0(get_path("shared_r_libraries"), "/get_life_table.R"))
mortality_funcs <- paste0(get_path("j"), "/Project/Mortality/shared/functions")
source(paste0(mortality_funcs, '/db_init.R'))
source(paste0(mortality_funcs,"/get_age_map.r"))
source(paste0(mortality_funcs, "/gen_age_length.R"))
source(paste0(mortality_funcs, "/check_key.R"))
source(paste0(mortality_funcs, "/qx_to_lx.R"))
source(paste0(mortality_funcs, "/lx_to_dx.R"))
source(paste0(mortality_funcs, "/gen_nLx.R"))
source(paste0(mortality_funcs, "/gen_Tx.R"))
source(paste0(mortality_funcs, "/gen_ex.R"))

# Load other reqired libraries
library(data.table)
library(foreign)

##############################################
### Define Internal Functions
##############################################
get_uid_cols <- function() {
  # Returns a list of unique identifiers
  return(c('location_id', 'year_id', 'age_group_id', 'sex_id'))
}

convert_lifetable <- function(dt, id_vars = get_uid_cols()) {
    dt <- copy(dt)
    print('converting lifetable...')
    req_age_groups <- c(5:20, 28, 30:33, 44:45, 148)
    dt_age_groups <- sort(unique(dt$age_group_id))
    missing_age_groups <- req_age_groups[!req_age_groups %in% dt_age_groups]
    if(length(missing_age_groups) > 0) {
        stop(paste0("Missing the following age_group_ids: ", 
                paste(missing_age_groups, collapse = ", ")))
    }
    ## Reshape wide from life table parameter IDs 1:3
    dt <- dt[life_table_parameter_id %in% c(1:3)]
    dt[life_table_parameter_id == 1, lt_parameter := "mx"]
    dt[life_table_parameter_id == 2, lt_parameter := "ax"]
    dt[life_table_parameter_id == 3, lt_parameter := "qx"]
    dt[, life_table_parameter_id := NULL]
    dt[, run_id := NULL]
    reshape_formula <- as.formula(paste0(paste(id_vars, collapse = " + "), " ~ lt_parameter"))
    dt <- data.table::dcast(data = dt, formula = reshape_formula, value.var = "mean")

    ## Merge on age map
    age_map <- setDT(get_age_map(type = "lifetable"))
    dt <- merge(dt, age_map[, .(age_group_id, age_group_years_start)], by = "age_group_id")
    setnames(dt, "age_group_years_start", "age")

    ## Setup for computation
    dt[, age_group_id := NULL]
    setkeyv(dt, c(id_vars[id_vars != "age_group_id"], "age"))

    ## Run through lifetable functions. NOTE: this will convert dt to a list
    gen_age_length(dt)
    qx_to_lx(dt)
    lx_to_dx(dt)

    gen_nLx(dt)
    ##    Tx - average person-years lived above age x in age interval
    ##    ex - average life expectancy at age x 
    gen_Tx(dt, id_vars = c(id_vars[id_vars != "age_group_id"], "age"))
    gen_ex(dt)

    ## Output table
    return(dt)
}

generate_nLx <- function(gbd_round_num) {
    print(paste("generating nLx data for gbd round", gbd_round_num, "..."))
    age_groups <- c(5:20, 28, 30:33, 44:45, 148)
    mx_ax_qx <- get_life_table(location_set_id = 35, status = 'best',
                    life_table_parameter_id = c(1:3), age_group_id = age_groups, 
                    gbd_round_id = gbd_round_num, with_shock = 1, with_hiv = 1, decomp_step='step1')
    if (!is.data.table(mx_ax_qx)) {
        mx_ax_qx <- as.data.table(mx_ax_qx)
    }
    full_lifetable <- convert_lifetable(mx_ax_qx)
    uid_cols <- get_uid_cols()
    # Add age_group_id
    full_lifetable$age_group_id <- (full_lifetable$age / 5)+5
    full_lifetable[full_lifetable$age == 0, age_group_id := 28] 
    full_lifetable[full_lifetable$age == 1, age_group_id := 5] 
    full_lifetable[full_lifetable$age >= 80 & full_lifetable$age <100 , 
                    age_group_id := age_group_id + 9]
    full_lifetable[full_lifetable$age >= 100 & full_lifetable$age <110 , 
                    age_group_id := age_group_id + 19]
    full_lifetable[full_lifetable$age == 110, age_group_id := 148] 
    nlx_table <- subset(full_lifetable,  ,c(uid_cols, 'nLx', 'age', 'age_length'))
    return(nlx_table)
}

calc_lambda <- function(age_id, calc_dt, age_col){
    calc_dt <- calc_dt[calc_dt[,get(age_col)] %in% c(age_id, age_id-1),]
    calc_dt[calc_dt[,get(age_col)]== age_id, 'lambda'] = 
        log(calc_dt[calc_dt[,get(age_col)] == age_id, "nLx"] / 
            calc_dt[calc_dt[,get(age_col)] == age_id-1, 'nLx'] 
        )
    output <- calc_dt[calc_dt[,get(age_col)] == age_id,]
    output$lambda = output$lambda/output$age_length
    return(output)
}


replace_young_ages <- function(dt) { 
    '%ni%' <- Negate('%in%')
    tmp <- dt[age_group_id==7,]
    replace_ages = c(6,5,4,3,2,1,28)
    dt_sub <- dt[age_group_id %ni% replace_ages]
    replace_df <- data.table() 
    for (i in replace_ages) { 
        tmp[,age_group_id:=i]
        replace_df <- rbind(tmp, replace_df)
    }
    return(rbind(dt_sub,replace_df))
}

generate_lambda_values <- function(dt) {
    print("calculating lambda values...")
    # Calculate lambda
    age_tbl = data.table("age" = unique(dt$age))
    age_tbl$lcalc_age <-rank(age_tbl$age, ties.method = "min") 
    lambda_dt <- merge(dt, age_tbl)
    min_age <- min(lambda_dt$lcalc_age)
    lambda_ages <- unique(lambda_dt$lcalc_age)[unique(lambda_dt$lcalc_age) != min_age]
    lambda_dt <- do.call(rbind, lapply(lambda_ages, calc_lambda, 
                        calc_dt=lambda_dt, age_col='lcalc_age'))
    # Combine more-specific age groups to create the broader age group '95+'
    uid_cols <- get_uid_cols()
    old_age_groups = c(33, 44, 45, 148)
    old = lambda_dt[lambda_dt$age_group_id %in% old_age_groups,]
    lambda_dt_235 <- as.data.table(aggregate(lambda ~ location_id + year_id + sex_id, 
                data=old, FUN=sum, na.rm=TRUE))
    lambda_dt_235$age_group_id <- 235
    lambda_dt <- rbind(lambda_dt, lambda_dt_235, fill=TRUE)
    # There is no easy way to calculate the infant age groups, so the '1-4' 
    #   age group value is used as a substitute
    young_age <- lambda_dt[lambda_dt$age_group_id == 5]
    for (n in c(1,2,3,4,28)) {
      young_age$age_group_id <- n 
      lambda_dt <- rbind(lambda_dt, young_age, fill=TRUE)
    }
    return(lambda_dt)
}


##############################################
### Run
##############################################
# Set arguments
gbd_round_id <- get_gbd_parameter("current_gbd_round")
d_step <- get_gbd_parameter('current_decomp_step')
today <- Sys.Date()
output_file <- get_path("lambda_values", process="nonfatal_model")
output_file <- gsub("<date>", today, output_file)
# Generate Lambdas
nLx <-generate_nLx(gbd_round_id)
lambda_values <- generate_lambda_values(nLx)

# re-assign age_group_id 7 for younger age groups 
lambda_values <- replace_young_ages(lambda_values) 
write.csv(as.data.frame(lambda_values), output_file)
print("lambda version updated")
# Add entry in cancer database
upload_notes <- paste("current 'best' version for gbd_round_id", gbd_round_id, 'decomp_step ', d_step)
cdb.append_to_table(table_name='cnf_lambda_version',
                    new_data=data.frame(date_generated=c(today), 
                    notes=c(upload_notes)))
