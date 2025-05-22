# dlist is the universe of distribution families as defined in the above code
#  classA is the majority of families
#  classB is scaled beta
#  classM is the mirror family of distributions
library(pracma, lib.loc = "FILEPATH")
source("./ensemble/pdf_families.R")
dlist <- c(classA, classB, classM)

# returns a list of x values, fx - the density, along with XMIN and XMAX
get_edensity <- function(weights, mean, sd, .min, .max) {

    W_ <- as.data.table(weights)
    M_ <- mean
    S_ <- sd

    if (M_ == 0) {
        XMIN <- 0
        XMAX <- qnorm(0.999, M_, S_)
    } else {
        mu <- log(M_/sqrt(1 + (S_^2/(M_^2))))
        sdlog <- sqrt(log(1 + (S_^2/M_^2)))
        XMIN <- qlnorm(0.001, mu, sdlog)
        XMAX <- qlnorm(0.999, mu, sdlog)
    }
    x_min <- ifelse(missing(.min), XMIN, max(c(.min, XMIN)))
    x_max <- ifelse(missing(.max), XMAX, min(c(.max, XMAX)))
    xx <- seq(x_min, x_max, length = 1000)

    fx <- 0 * xx

    # remove 0s
    W_ <- W_[1, W_ != 0, with=FALSE]

    buildDENlist <- function(jjj) {
        distn = names(W_)[[jjj]]
        EST <- NULL
        LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
        if (LENGTH == 4) {
            EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2),
                                                               XMIN = XMIN,
                                                               XMAX = XMAX)),
                       silent = T)
        } else {
            EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2))),
                       silent = T)
        }
        d.dist <- NULL
        d.dist <- try(dlist[paste0(distn)][[1]]$dF(xx, EST), silent = T)
        if (class(EST) == "numeric" & class(d.dist) == "numeric") {
            dEST <- EST
            dDEN <- d.dist
            weight <- W_[[jjj]]
        } else {
            dEST <- 0
            dDEN <- 0
            weight <- 0
        }
        dDEN[!is.finite(dDEN)] <- 0
        return(list(dDEN = dDEN, weight = weight))
    }
    denOUT <- lapply(1:length(W_), buildDENlist)

    ## re-scale weights
    TW <- unlist(lapply(denOUT, function(x) (x$weight)))
    TW <- TW/sum(TW, na.rm = T)
    ## build ensemble density
    fx <- Reduce("+", lapply(1:length(TW), function(jjj) denOUT[[jjj]]$dDEN * TW[jjj]))
    fx[!is.finite(fx)] <- 0
    fx[c(length(fx),1)] <- 0
    area_under_curve <- pracma::trapz(xx, fx)
    if (area_under_curve != 0) fx <- fx / area_under_curve

    return(list(fx = fx, x = xx, XMIN = x_min, XMAX = x_max))

}

get_edensity <- compiler::cmpfun(get_edensity)
