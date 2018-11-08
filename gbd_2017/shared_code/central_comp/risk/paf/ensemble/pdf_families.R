# The Probability Distribution Function Families
# Fit and select a probability distribution function (PDF).
# class A - support is non-negative real numbers
# class B - support is non-negative real numbers, shifted and transformed
library(actuar)
library(zipfR)
EULERS_CONSTANT = 0.577215664901533
nullF = function() {
    stop("Function not implemented")
}

# gamma ------------------------------------------------------------------------
# the gamma family of PDFs, class A, standard R distribution
gammaOBJ = list(name = "gamma",
                initF = nullF,
                mv2par = function(mn, vr) {
                    list(shape = mn^2/vr, rate = mn/vr)
                }, dF = function(x, p) {
                    dgamma(x, shape = p[1], rate = p[2])
                }, tailF = function(p, tau, lt) {
                    pgamma(tau, shape = p[1], rate = p[2], lower.tail = lt)
                }, plotF = function(x, p, clr) {
                    lines(x, dgamma(x, shape = p[1], rate = p[2]), col = clr)
                })

# mgamma -----------------------------------------------------------------------
# the mirrored gamma family of PDFs class M
dmgamma = function(x, shape, rate, XMIN, XMAX) {
    dgamma(XMAX - x, shape = shape, rate = rate)
}
pmgamma = function(x, shape, rate, lower.tail = TRUE) {
    # NOTE: with mirroring, take the other tail
    pgamma(XMAX - x, shape, rate, lower.tail = ifelse(lower.tail, FALSE, TRUE))
}
qmgamma = function(x, shape, rate) {
    qgamma(XMAX - x, shape = shape, rate = rate)
}
mgammaOBJ = list(name = "mgamma",
                 initF = function(D) {
                     list(shape = (XMAX - mean(D))^2/var(D), rate = (XMAX - mean(D))/var(D))
                 }, mv2par = function(mn, vr, XMIN, XMAX) {
                     list(shape = (XMAX - mn)^2/vr, rate = (XMAX - mn)/vr, XMIN = XMIN, XMAX = XMAX)
                 }, dF = function(x, p, XMIN, XMAX) {
                     dmgamma(x, shape = p[1], rate = p[2], XMIN = p[3], XMAX = p[4])
                 }, tailF = function(p, tau, lt) {
                     pmgamma(tau, shape = p[1], rate = p[2], lower.tail = lt)
                 }, plotF = function(x, p, clr) {
                     lines(x, dmgamma(x, shape = p[1], rate = p[2], XMIN = p[3], XMAX = p[4]), col = clr)
                 })

# invgamma ---------------------------------------------------------------------
# the invgamma family of PDFs, class A class A, standard R distribution
xinvgamma = function(x, shape, scale) {
    x * dinvgamma(x, shape = shape, scale = scale)
}
x2invgamma = function(x, shape, scale) {
    x^2 * dinvgamma(x, shape = shape, scale = scale)
}
invgamma_mv2p = function(mn, vr) {
    try({
        F = function(x, mn, vr) {
            names(x) <- c("a", "b")
            list2env(as.list(abs(x)), envir = .GlobalEnv)
            mn1 = b/(a - 1)
            vr1 = b^2/(a - 1)^2/(a - 2)
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn, mn * sqrt(vr))
        est = NULL
        est = abs(optim(xi, F, mn = mn, vr = vr)$par)
        list(shape = est[1], scale = est[2])
    })
}
invgammaOBJ = list(name = "invgamma",
                   mv2par = invgamma_mv2p,
                   initF = nullF,
                   dF = function(x, p) {
                       dinvgamma(x, shape = p[1], scale = p[2])
                   }, tailF = function(p, tau, lt) {
                       pinvgamma(tau, shape = p[1], scale = p[2], lower.tail = lt)
                   }, plotF = function(x, p, clr) {
                       lines(x, dinvgamma(x, shape = p[1], scale = p[2]), col = clr)
                   })

# normal -----------------------------------------------------------------------
# the normal family of PDFs, class A class A, standard R distribution
norm_mv2p = function(mn, vr) {
    list(mean = mn, sd = sqrt(vr))
}
normOBJ = list(name = "norm",
               initF = nullF,
               mv2par = norm_mv2p,
               dF = function(x, p) {
                   dnorm(x, mean = p[1], sd = p[2])
               }, tailF = function(p, tau, lt) {
                   pnorm(tau, mean = p[1], sd = p[2], lower.tail = lt)
               }, plotF = function(x, p, clr) {
                   lines(x, dnorm(x, mean = p[1], sd = p[2]), col = clr)
               })

# lnorm ------------------------------------------------------------------------
# the log-normal family of PDFs class A, standard R distribution
lnorm_mv2p = function(mn, vr) {
    ## alternate param
    sd <- sqrt(vr)
    mu <- log(mn/sqrt(1 + (sd^2/(mn^2))))
    sdlog <- sqrt(log(1 + (sd^2/mn^2)))
    list(meanlog = mu[1], sdlog = sdlog[1])
}
lnormOBJ = list(name = "lnorm",
                initF = nullF,
                mv2par = lnorm_mv2p,
                dF = function(x, p) {
                    dlnorm(x, meanlog = p[1], sdlog = p[2])
                }, tailF = function(p, tau, lt) {
                    plnorm(tau, meanlog = p[1], sdlog = p[2], lower.tail = lt)
                }, plotF = function(x, p, clr) {
                    lines(x, dlnorm(x, meanlog = p[1], sdlog = p[2]), col = clr)
                })

# exp --------------------------------------------------------------------------
# the exponential family of PDFs, class A class A, standard R distribution
expOBJ = list(name = "exp",
              initF = nullF,
              mv2par = function(mn, vr) {
                  list(rate = 1/mn)
              }, dF = function(x, p) {
                  dexp(x, rate = p)
              }, tailF = function(p, tau, lt) {
                  pexp(tau, rate = p, lower.tail = lt)
              }, plotF = function(x, p, clr) {
                  lines(x, dexp(x, rate = p), col = clr)
              })

# weibull ----------------------------------------------------------------------
# the weibull family of PDFs, class A class A, standard R distribution
xweibull = function(x, shape, scale) {
    x * dweibull(x, shape = shape, scale = scale)
}
x2weibull = function(x, shape, scale) {
    x^2 * dweibull(x, shape = shape, scale = scale)
}
weibull_mv2p = function(mn, vr) {
    try({
        F = function(x, mn, vr) {
            names(x) <- c("sh", "sc")
            list2env(as.list(abs(x)), envir = .GlobalEnv)
            mn1 = sc * gamma(1 + 1/sh)
            vr1 = sc^2 * (gamma(1 + 2/sh) - mn1^2)
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn, mn/sqrt(vr))
        est = NULL
        est = abs(optim(xi, F, mn = mn, vr = vr)$par)
        list(shape = est[1], scale = est[2])
    })
}
weibullOBJ = list(name = "weibull",
                  initF = nullF,
                  mv2par = weibull_mv2p,
                  dF = function(x, p) {
                      dweibull(x, shape = p[1], scale = p[2])
                  }, tailF = function(p, tau, lt) {
                      pweibull(tau, shape = p[1], scale = p[2], lower.tail = lt)
                  }, plotF = function(x, p, clr) {
                      lines(x, dweibull(x, shape = p[1], scale = p[2]), col = clr)
                  })

# invweibull -------------------------------------------------------------------
# the invweibull family of PDFs, class A class A, standard R distribution
xinvweibull = function(x, shape, scale) {
    x * dinvweibull(x, shape = shape, scale = scale)
}
x2invweibull = function(x, shape, scale) {
    x^2 * dinvweibull(x, shape = shape, scale = scale)
}
invweibull_mv2p = function(mn, vr) {
    try({
        F = function(x, mn, vr) {
            names(x) <- c("sh", "sc")
            list2env(as.list(abs(x)), envir = .GlobalEnv)
            mn1 = sc * gamma(1 - 1/sh)
            vr1 = sc^2 * gamma(1 - 2/sh) - mn1^2
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(max(2.2, sqrt(vr)/mn), mn)
        est = NULL
        est = abs(optim(xi, F, mn = mn, vr = vr)$par)
        list(shape = est[1], scale = est[2])
    })
}
invweibullOBJ = list(name = "invweibull",
                     initF = nullF,
                     mv2par = invweibull_mv2p,
                     dF = function(x, p) {
                         dinvweibull(x, shape = p[1], scale = p[2])
                     }, tailF = function(p, tau, lt) {
                         pinvweibull(tau, shape = p[1], scale = p[2], lower.tail = lt)
                     }, plotF = function(x, p, clr) {
                         lines(x, dinvweibull(x, shape = p[1], scale = p[2]), col = clr)
                     })

# llogis -----------------------------------------------------------------------
# the llogis family of PDFs, class A class A, standard R distribution
xllogis = function(x, shape, scale) {
    x * dllogis(x, shape = shape, scale = scale)
}
x2llogis = function(x, shape, scale) {
    x^2 * dllogis(x, shape = shape, scale = scale)
}
llogis_mv2p = function(mn, vr) {
    try({
        F = function(x, mn, vr) {
            names(x) <- c("a", "b")
            list2env(as.list(x), envir = .GlobalEnv)
            b = pi/b
            mn1 = a * b/sin(b)
            vr1 = a^2 * (2 * b/sin(2 * b) - b^2/(sin(b)^2))
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn, max(2, mn))
        est = NULL
        est = abs(optim(xi, F, mn = mn, vr = vr)$par)
        list(shape = est[2], scale = est[1])
    })
}
llogisOBJ = list(name = "llogis",
                 initF = nullF,
                 mv2par = llogis_mv2p,
                 dF = function(x, p) {
                     dllogis(x, shape = p[1], scale = p[2])
                 }, tailF = function(p, tau, lt) {
                     pllogis(tau, shape = p[1], scale = p[2], lower.tail = lt)
                 }, plotF = function(x, p, clr) {
                     lines(x, dllogis(x, shape = p[1], scale = p[2]), col = clr)
                 })

# gumbel -----------------------------------------------------------------------
# the gumbel family of PDFs, class A class A, defined below
gumbel_mv2p = function(mn, vr) {
        list(alpha = mn - EULERS_CONSTANT * sqrt(vr) * sqrt(6)/pi,
             scale = sqrt(vr) * sqrt(6)/pi)
}
gumbelOBJ = list(name = "gumbel",
                 mv2par = gumbel_mv2p,
                 initF = function(D) {
                     list(alpha = mean(D) - EULERS_CONSTANT * sd(D) * sqrt(6)/pi,
                          scale = sd(D) * sqrt(6)/pi)
                 }, dF = function(x, p) {
                     dgumbel(x, alpha = p[1], scale = p[2])
                 }, tailF = function(p, tau, lt) {
                     pgumbel(tau, alpha = p[1], scale = p[2], lower.tail = lt)
                 }, plotF = function(x, p, clr) {
                     lines(x, dgumbel(x, alpha = p[1], scale = p[2]), col = clr)
                 })

# mgumbel ----------------------------------------------------------------------
# the mirrored gumbel family of PDFs, class A class M, defined below NOTE:
# XMAX should be defined as a global parameter
dmgumbel = function(x, alpha, scale) {
    dgumbel(XMAX - x, alpha, scale)
}
pmgumbel = function(q, alpha, scale, lower.tail) {
    # NOTE: with mirroring, take the other tail
    pgumbel(XMAX - q, alpha, scale, lower.tail = ifelse(lower.tail, FALSE, TRUE))
}
qmgumbel = function(p, alpha, scale) {
    qgumbel(XMAX - z, alpha, scale)
}
rmgumbel = function(n, alpha, scale) {
    mn = alpha + scale * EULERS_CONSTANT
    rgumbel(n, alpha + XMAX - 2 * mn, scale)
}
mgumbel_mv2p = function(mn, vr) {
    list(alpha = XMAX - mn - EULERS_CONSTANT * sqrt(vr) * sqrt(6)/pi,
         scale = sqrt(vr) * sqrt(6)/pi)
}
mgumbelOBJ = list(name = "mgumbel",
                  initF = function(D) {
                      inits = list(alpha = XMAX - mean(D) + EULERS_CONSTANT * sd(D) * sqrt(6)/pi,
                                   scale = sd(D) * sqrt(6)/pi)
                      fitdist(XMAX - D, "gumbel", start = inits)$estimate
                  }, mv2par = mgumbel_mv2p, dF = function(x, p) {
                      dmgumbel(x, alpha = p[1], scale = p[2])
                  }, tailF = function(p, tau, lt) {
                      # NOTE: take the other tail
                      pmgumbel(tau, alpha = p[1], scale = p[2], lower.tail = lt)
                  }, plotF = function(x, p, clr) {
                      lines(x, dmgumbel(x, alpha = p[1], scale = p[2]), col = clr)
                  })

# betasr -----------------------------------------------------------------------
# the beta distribution, over a shifted range class B, defined below
dbetasr = function(x, shape1, shape2, XMIN, XMAX) {
    dbeta((x - XMIN)/(XMAX - XMIN), shape1, shape2)/(XMAX - XMIN)
}
pbetasr = function(q, shape1, shape2, lt) {
    pbeta((q - XMIN)/(XMAX - XMIN), shape1, shape2, lower.tail = lt)
}
qbetasr = function(p, shape1, shape2) {
    qbeta((p - XMIN)/XMAX, shape1, shape2)/(XMAX - XMIN)
}
rbetasr = function(n, shape1, shape2) {
    XMIN + rbeta(n, shape1, shape2) * (XMAX - XMIN)
}
betasr_mv2p = function(mn, vr, XMIN, XMAX) {
    try({
        m1 = (mn - XMIN)/(XMAX - XMIN)
        v1 = vr/(XMAX - XMIN)^2
        list(shape1 = m1^2 * (1 - m1)/v1 - m1,
             shape2 = m1 * (1 - m1)^2/v1 - 1 + m1,
             XMIN = XMIN, XMAX = XMAX)
    })
}
betasrOBJ = list(name = "betasr",
                 mv2par = betasr_mv2p,
                 initF = function(D) {
                     betasr_mv2p(mean(D), var(D), XMIN(D), XMAX(D))
                 }, dF = function(x, p) {
                     dbetasr(x, shape1 = p[1], shape2 = p[2], XMIN = p[3], XMAX = p[4])
                 }, tailF = function(p, tau, lt) {
                     pbetasr(tau, shape1 = p[1], shape2 = p[2], lt)
                 }, plotF = function(x, p, clr) {
                     lines(x, dbetasr(x, shape1 = p[1], shape2 = p[2], XMIN = p[3], XMAX = p[4]), col = clr)
                 })


# put them all in a list -------------------------------------------------------
classA1 = list(exp = expOBJ)
classA2 = list(gamma = gammaOBJ,
               invgamma = invgammaOBJ,
               llogis = llogisOBJ,
               gumbel = gumbelOBJ,
               weibull = weibullOBJ,
               invweibull = invweibullOBJ,
               lnorm = lnormOBJ,
               norm = normOBJ)
classA = c(classA1, classA2)
classM = list(mgamma = mgammaOBJ, mgumbel = mgumbelOBJ)
classB = list(betasr = betasrOBJ)
