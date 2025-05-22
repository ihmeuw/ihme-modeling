#' A general function for converting pv prevalence to incidence.
#' 
#' This function converts prevalence (2-10 year olds) in the range (0, 1), 
#'   to clinical incidence (all ages, per person per year) in the range 
#'   (0, 0.619). The relationship is non-linear and monotonic. Given a
#'   region number 1-12 it will use the appropriate model, and use the 
#'   pooled model otherwise.
#'   
#'
#' @param prev Prevalence (2-10) vector.
#' @param region The region code as given in the paper (1-12). Either a vector
#'   with length match prev, or a single scalar.
#' @param force Force the model to return output even though prevalences are in inappropriate ranges?
#' @details To force the function to use the pooled model, set region to 0.
#' 
#' @examples 
#' PrevIncConversion_pv(runif(100, 0, 0.05), 3)
#' PrevIncConversion_pv(rep(0.02, 100), sample(1:12, 100, replace = TRUE))
#' 
#' # Force to use the pooled model
#' PrevIncConversion_pv(0.02, 0)
#' 
#' # Display all models.
#' d <- data.frame(prev = rep(seq(0, 0.15, 0.001), each = 12),
#'                 region = 1:12)
#' d$inc <- PrevIncConversion_pv(d$prev, d$region, force = TRUE)
#' 
#' library(ggplot2)
#' ggplot(d, aes(prev, inc, colour = factor(region))) + geom_line()
#' ggplot(d, aes(prev, inc, colour = factor(region))) +
#'   geom_line() +
#'   facet_wrap(~ factor(region), scales = 'free_y')
#' 
#' # Sort of replicate Figure 7 but with new relationship
#' 
#' ggplot(d, aes(100*prev, 1000*inc, colour = factor(region))) +
#'   geom_line() +
#'   facet_wrap(~ factor(region), scales = 'free_y') +
#'   scale_y_log10() +
#'   scale_x_log10() +
#'   coord_cartesian(xlim = c(0.2, 20))
#' 
#' 
#' # individual functions.
#' PrevIncConversion_pv_pooled(0.03)
#' PrevIncConversion_pv_12(0.1)
#'     
#' @export

PrevIncConversion_pv <- function(prev, region, force = FALSE){
  
  ind_model_regions <- c(2, 3, 8, 10, 11, 12)
  region_row <- sapply(region, function(r) 
    if(r %in% ind_model_regions){
      which(ind_model_regions == r)
    } else {
      7
    })
  
  
  max_prev <- IncPrevConversion_pv(0.7, c(ind_model_regions, 0))
  if(any(prev > max_prev[region_row])){
    if(!force){
      stop('Prevalences are above appropriate ranges. Conversion will give silly output')
    } else {
      warning('Prevalences are above appropriate ranges. Conversion will give silly output')
    }
  }
  coefs <- data.frame(b0 = c(-9.981607, -9.981610, -9.981608, -9.981601, -9.981608, -9.981608, -9.981608), 
                      slope = c(1.130463, 1.139376, 1.130203, 1.265105, 1.107091, 1.205450, 1.116050))
  linpred <- -log((1 / prev) - 1)
  biglog0 <- (linpred - coefs$b0[region_row]) / coefs$slope[region_row]
  inc <- biglog02inc(biglog0)
  
  inc[inc < 0] <- 0
  
  return(inc)
  
}



#' @export
#'@rdname PrevIncConversion_pv

PrevIncConversion_pv_pooled <- function(prev){
  
  region <- 0
  PrevIncConversion_pv(prev, region)  
  
}



#'@export
#'@rdname PrevIncConversion_pv

PrevIncConversion_pv_2 <- function(prev){
  
  region <- 2
  PrevIncConversion_pv(prev, region)  
  
}




#'@export
#'@rdname PrevIncConversion_pv


PrevIncConversion_pv_3 <- function(prev){
  
  region <- 3
  PrevIncConversion_pv(prev, region)  
  
}





#'@export
#'@rdname PrevIncConversion_pv


PrevIncConversion_pv_8 <- function(prev){
  
  region <- 8
  PrevIncConversion_pv(prev, region)  
  
}





#'@export
#'@rdname PrevIncConversion_pv


PrevIncConversion_pv_10 <- function(prev){
  
  region <- 10
  PrevIncConversion_pv(prev, region)  
  
}





#'@export
#'@rdname PrevIncConversion_pv


PrevIncConversion_pv_11 <- function(prev){
  
  region <- 11
  PrevIncConversion_pv(prev, region)  
  
}



#'@export
#'@rdname PrevIncConversion_pv


PrevIncConversion_pv_12 <- function(prev){
  
  region <- 12
  PrevIncConversion_pv(prev, region)  
}




######################################################################


#' A general function for converting pv incidence to prevalence
#' 
#' This function converts clinical incidence (all ages, per person per year) in 
#'   the range (0, infinty) to prevalence (1-99 year olds) in the range 
#'   (0, 1). The relationship is non-linear and monotonic. Given a region
#'   number 1-12 it will use the appropriate model, and use the 
#'   pooled model otherwise.
#'   
#'   Note the models in region 12 and 3 go well above incidence of 1. 
#'     Also note that the raw equations from the paper give prevance > 1
#'     for various incidence values. truncate the function 
#'     at prevalence = 1. As in the pf model though, they are therefore
#'     not true inverse functions at very high incidence.
#'
#' @param inc Incidence (all age) vector.
#' @param region The region code as given in the paper (1-12). Either a vector
#'   with length match prev, or a single scalar.
#' @details To force the function to use the pooled model, set region to 0.
#' @examples 
#' IncPrevConversion_pv(runif(100), 3)
#' IncPrevConversion_pv(runif(100), sample(1:12, 100, replace = TRUE))
#' 
#' # Force to use the pooled model
#' IncPrevConversion_pv(runif(100), 0)
#' 
#' # Display all models.
#' d <- data.frame(inc = rep(seq(0, 1, 0.001), each = 12),
#'                 region = 1:12)
#' d$prev <- IncPrevConversion_pv(d$inc, d$region)
#' 
#' library(ggplot2)
#' ggplot(d, aes(prev, inc, colour = factor(region))) + geom_line()
#' ggplot(d, aes(prev, inc, colour = factor(region))) +
#'   geom_line() +
#'   facet_wrap(~ factor(region), scales = 'free_y')
#' 
#' 
#' # individual functions.
#' IncPrevConversion_pv_pooled(0.1)
#' IncPrevConversion_pv_12(0.5)
#' 
#' # The two functions are inverses within certain bounds.
#' d <- data.frame(prev = rep(seq(0.01, 1, 0.0001), each = 12),
#'                 region = 1:12)
#' d$inc <- PrevIncConversion_pv(d$prev, d$region, force = TRUE)
#' d$prev2 <- IncPrevConversion_pv(d$inc, d$region)
#' any(abs(d$prev - d$prev2) > 1e-13)
#' 
#' # At very low prevalence values, the equations give negative incidence. 
#' # So some hard limits are implemented.
#' # Which means the functions aren't inverses
#' d <- data.frame(prev = rep(c(0, 1e-9), each = 12),
#'                 region = 1:12)
#' d$inc <- PrevIncConversion_pv(d$prev, d$region, force = TRUE)
#' d$prev2 <- IncPrevConversion_pv(d$inc, d$region)
#' all(abs(d$prev - d$prev2) > 1e-7)
#' @export

IncPrevConversion_pv <- function(inc, region){
  
  ind_model_regions <- c(2, 3, 8, 10, 11, 12)
  region_row <- sapply(region, function(r) 
    if(r %in% ind_model_regions){
      which(ind_model_regions == r)
    } else {
      7
    })
  
  coefs <- data.frame(b0 = c(-9.981607, -9.981610, -9.981608, -9.981601, -9.981608, -9.981608, -9.981608), 
                      slope = c(1.130463, 1.139376, 1.130203, 1.265105, 1.107091, 1.205450, 1.116050))
  biglog0 <- inc2biglog0(inc)
  prev <- stats::plogis(coefs$b0[region_row] + coefs$slope[region_row] * biglog0)
  
  return(prev)
  
}




# Convert incidence to a logged value with inc = 0 -> biglog0 = 0
inc2biglog0 <- function(inc) log10(inc + 4.38914e-07 / 2) - -6.658651
biglog02inc <- function(biglog0) 10 ^ (biglog0 + -6.658651) - 4.38914e-07 / 2

#' @export
#'@rdname IncPrevConversion_pv

IncPrevConversion_pv_pooled <- function(inc){
  
  region <- 0
  IncPrevConversion_pv(inc, region)  
  
}



#'@export
#'@rdname IncPrevConversion_pv

IncPrevConversion_pv_2 <- function(inc){
  
  region <- 2
  IncPrevConversion_pv(inc, region)  
  
}




#'@export
#'@rdname IncPrevConversion_pv


IncPrevConversion_pv_3 <- function(inc){
  
  region <- 3
  IncPrevConversion_pv(inc, region)  
  
}





#'@export
#'@rdname IncPrevConversion_pv


IncPrevConversion_pv_8 <- function(inc){
  
  region <- 8
  IncPrevConversion_pv(inc, region)  
  
}





#'@export
#'@rdname IncPrevConversion_pv


IncPrevConversion_pv_10 <- function(inc){
  
  region <- 10
  IncPrevConversion_pv(inc, region)  
  
}





#'@export
#'@rdname IncPrevConversion_pv


IncPrevConversion_pv_11 <- function(inc){
  
  region <- 11
  IncPrevConversion_pv(inc, region)  
  
}



#'@export
#'@rdname IncPrevConversion_pv


IncPrevConversion_pv_12 <- function(inc){
  
  region <- 12
  IncPrevConversion_pv(inc, region)  
}


