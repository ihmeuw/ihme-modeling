## Kannisto-Thatcher qx extension (helper functions, followed by main function)
## AUTHOR
## October 2019

# all these packages are needed for disrEx... find a better way to get all of them
# libr <- "/ihme/scratch/users/krpaul/packages"
# library(startupmsg, lib.loc = libr)
# library(sfsmisc, lib.loc = libr)
# library(SweaveListingUtils, lib.loc = libr)
# library(distr, lib.loc = libr)
# library(distrEx, lib.loc = libr)



#' Kannisto-Thatcher model, in force of mortality (mu) space
#' mux = [a * exp(bx)] / [1 + a * exp(bx)]
#'
#' @param x age (numeric)
#' @param params vector of K-T parameters (a, b)
#'
#' @return returns mux output of K-T model for age x and parameters a, b (numeric)
#' @export
kt.mu <- function(x, params){
  a <- params[1]
  b <- params[2]
  pred <- (a*exp(b*x))/(1 + a*exp(b*x))
  return(pred)
}



#' Kannisto-Thatcher model, in 5qx space
#' 5qx = 1 - ([a * exp(bx) + 1]/[a * exp(b(x+5)) + 1])^(1/b)
#'
#' @param x age (numeric)
#' @param params vector of K-T parameters (a, b)
#'
#' @return returns 5qx output of K-T model for age x and parameters a, b (numeric)
#' @export
kt.qx <- function(x, params){
  a <- params[1]
  b <- params[2]
  pred <- 1 - ((a*exp(b*x) + 1)/(a*exp(b*(x+5)) + 1))^(1/b)
  return(pred)
}



#' Kannisto-Thatcher model, in 5ax space
#' See pg 69 Preston Demography textbook for equation for nax from mu
#'
#' @param x age (numeric)
#' @param params vector of K-T parameters (a, b)
#'
#' @return returns 5ax output of K-T model for age x and parameters a, b (numeric)
#' @export
#' 
#' @import distr
#' @import distrEx
kt.ax <- function(x, params){
  
  a <- params[1]
  b <- params[2]

  num   <- function(a0, x0, params) return(exp(-1 * distrEx::distrExIntegrate(kt.mu, x0, a0, params = params)) * kt.mu(a0, params) * (a0 - x0))
  denom <- function(a0, x0, params) return(exp(-1 * distrEx::distrExIntegrate(kt.mu, x0, a0, params = params)) * kt.mu(a0, params))
  numV   <- function(A, x0, params) return(sapply(A, num, x0, params)) ## vectorized
  denomV <- function(A, x0, params) return(sapply(A, denom, x0, params)) ## vectorized
  
  pred <- distrEx::distrExIntegrate(numV, x, x + 5, x0 = x, params = params) /
          distrEx::distrExIntegrate(denomV, x, x + 5, x0 = x, params = params)
  return(pred)
}



#' Optimization function for K-T model
#' SSE = sum of (qx - qx_pred)^2
#'
#' @param params vector of K-T parameters (a, b)
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx
#'
#' @return returns sum of square errors (numeric)
#' @export
sse <- function(params, data){
  data <- setDT(data)
  data$pred <- unlist(lapply(data$age, kt.qx, params = params))
  data[, se := (qx - pred)^2]
  return(sum(data$se))
}



#' Calculate starting parameters a and b for input to optim (one life table)
#' Based on logit(mx) ~ ln(a) + b*age
#'
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx
#'
#' @return returns list of K-T parameters (a, b)
#' @export
start.kt <- function(data){
  data <- setDT(data)
  data[, y := logit(mx)]
  fit <- glm(y ~ age, data = data)
  a0 <- unname(exp(coef(fit)[1]))
  b0 <- unname(coef(fit)[2])
  return(list(a0 = a0, b0 = b0))
}



#' Fit K-T model (one life table)
#'
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx
#' @param age_width numeric, default = 20, inclusive width of interval of data to include in years.
#'                  example: data that goes to age 90-94 would be fit starting with data from 70-74 when age_width=20.
#'
#' @return returns vector of K-T parameters (a, b)
#' @export
fit.kt <- function(data, age_width = 20){
  data <- setDT(data)
  age_start <- max(data$age) - age_width
  start <- start.kt(data[age >= age_start])
  fit <- optim(par = c(a = start$a0, b = start$b0),
               fn = sse,
               data = data[age >= age_start],
               method = "L-BFGS-B",
               control = list(maxit=10000),
               lower = c(a = 0.00000001, b = 0.00000001),
               upper = c(a = 5, b = 5))
  params <- fit$par
  return(params)
}



#' Expand ages and predict K-T (one life table)
#'
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx
#' @param params vector of parameters for K-T model (a, b)
#' @param byvars character vector containing the variable names of all variables that uniquely identify the observations (except for age)
#'
#' @return returns data.table with qx extended to age group 105-109 for one life table
#' @export
pred.kt <- function(data, params, byvars){
  data <- setDT(data)
  start_age <- max(data$age) + 5
  if(start_age < 105){
    add_ages  <- seq(start_age, 105, 5)
    for(aa in add_ages){
      add_row <- data[age == start_age - 5]
      add_row[, age := aa]
      add_row <- add_row[, .SD, .SDcols = c(byvars, "age")]
      data <- rbind(data, add_row, fill = T)
    }
  }
  data$pred <- unlist(lapply(data$age, kt.qx, params = params))
  data$pred_ax <- unlist(lapply(data$age, kt.ax, params = params))
  # predict starting from 80 if start_age is 105
  if(start_age < 105){
    data[age >= start_age, qx := pred]
    data[age >= start_age, ax := pred_ax]
  } else {
    data[age >= 80, qx := pred]
    data[age >= 80, ax := pred_ax]
  }
  data[, pred := NULL]
  return(data)
}



#' Fit Kannisto-Thatcher model and predict (one life table)
#'
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx
#' @param byvars character vector containing the variable names of all variables that uniquely identify the observations (except for age)
#' @param age_width numeric, default = 20, inclusive width of interval of data to include in years.
#'                  example: data that goes to age 90-94 would be fit starting with data from 70-74 when age_width=20.
#'
#' @return returns data.table with qx extended to age group 105-109 for one life table
#' @export
fit.predict.kt <- function(data, byvars, age_width = 20){
  data   <- setDT(data)
  params <- fit.kt(data, age_width = age_width)
  data   <- pred.kt(data, params, byvars)
  return(data)
}



#' Extend qx values up to age group 105-109 using Kannisto-Thatcher model
#'
#' @param data data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, qx, ax
#' @param byvars character vector containing the variable names of all variables that uniquely identify the observations (except for age)
#' @param age_width numeric, default = 20, inclusive width of interval of data to include in years.
#'                  example: data that goes to age 90-94 would be fit starting with data from 70-74 when age_width=20.
#'
#' @return returns data.table with qx and ax extended to age group 105-109
#' 
#'  Initial parameter values selected using methodology from
#'  Feehan, D.M. Demography (2018) 55: 2025. https://doi.org/10.1007/s13524-018-0728-x
#' 
#'  Additional reference: HMD methods
#'  https://www.mortality.org/Public/Docs/MethodsProtocol.pdf
#' 
#' @export
#'
#' @import data.table
#' @import stats
#' @import dplyr
#' @import plyr
extend_qx_kt <- function(data, byvars, age_width = 20){
  
  ## Remove any old age groups with qx > 1
  data[age >= 65 & qx > 1, age_qx_over_1 := min(age), by = byvars]
  data[is.na(age_qx_over_1), age_qx_over_1 := 999]
  data[, min_age_qx_over_1 := min(age_qx_over_1, na.rm=T), by = byvars]
  data <- data[age < min_age_qx_over_1]
  
  ## Drop old age groups after qx starts to decrease, if this happens
  data[age >= 65, qx_change := shift(qx, 1, type = "lead") - qx, by = byvars]
  data[age >= 65 & qx_change < 0, min_age_qx_change_neg := min(age), by= byvars]
  change_neg <- data[age == min_age_qx_change_neg, c("age", "min_age_qx_change_neg", byvars), with = F]
  change_neg <- change_neg[, c("age") := NULL]
  data[, min_age_qx_change_neg := NULL]
  if(nrow(change_neg) > 0){
    data <- merge(data, change_neg, by = byvars, all.x=T)
    data <- data[is.na(min_age_qx_change_neg) | age < min_age_qx_change_neg]
  }
  
  ## Fit & Predict KT model to get qx and ax
  data <- setDT(data)
  data[, grp := .GRP, by = byvars]
  data <- ddply(data, .(grp), fit.predict.kt, byvars = byvars, age_width = age_width) %>% setDT
  data <- data[, .SD, .SDcols = c(byvars, "age", "mx", "ax", "qx", "lx", "dx")]
  setkeyv(data, c(byvars, "age"))
  return(data)
}