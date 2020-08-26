# Fit and select a probability distribution function (PDF).

EULERS_CONSTANT = 0.577215664901533
library(actuar)  # includes the inverse weibull
library(zipfR)  # defines the incomplete gamma function Igamma
varE = function(D) {
    n = length(D)
    var(D) * (n - 1)/n
}
skew = function(D) {
    mn = mean(D)
    mean((mn - D)^3)
}
nullF = function(D) {
    NULL
}

# gamma ------------------------------------------------------------------------
# the gamma family of PDFs, class A class A, standard R distribution
gammaOBJ = list(name = "gamma", initF = nullF, mv2par = function(mn, vr) {
    try(list(shape = mn^2/vr, rate = mn/vr), silent = T)
}, dF = function(x, p) {
    try(dgamma(x, shape = p[1], rate = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pgamma(tau, shape = p[1], rate = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dgamma(x, shape = p[1], rate = p[2]), col = clr)
})

# mgamma  ------------------------------------------------------------------------
# the mirrored gamma family of PDFs class M
dmgamma = function(x, shape, rate, XMIN, XMAX) {
    try(dgamma(XMAX - x, shape = shape, rate = rate), silent = T)
}
pmgamma = function(q, shape, rate, lower.tail = TRUE) {
    # NOTE: with mirroring, take the other tail
    pgamma(XMAX - q, shape, rate, lower.tail = ifelse(lower.tail, FALSE, TRUE))
}
qmgamma = function(p, shape, rate) {
    qgamma(XMAX - z, shape = shape, rate = rate)
}
mgammaOBJ = list(name = "mgamma", initF = function(D) {
    try(list(shape = (XMAX - mean(D))^2/var(D), rate = (XMAX - mean(D))/var(D)), silent = T)
}, mv2par = function(mn, vr, XMIN, XMAX) {
    try(list(shape = (XMAX - mn)^2/vr, rate = (XMAX - mn)/vr, XMIN = XMIN, XMAX = XMAX), silent = T)
}, dF = function(x, p, XMIN, XMAX) {
    try(dmgamma(x, shape = p[1], rate = p[2], XMIN = p[3], XMAX = p[4]), silent = T)
}, tailF = function(p, tau, lt) {
    pmgamma(tau, shape = p[1], rate = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dmgamma(x, shape = p[1], rate = p[2], XMIN = p[3], XMAX = p[4]), col = clr)
})

# invgamma  ------------------------------------------------------------------------
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
            a = x[1]
            b = x[2]
            mn1 = NULL
            mn1 = b/(a - 1)
            vr1 = NULL
            vr1 = b^2/(a - 1)^2/(a - 2)
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn, mn * sqrt(vr))
        F(xi, mn, vr)
        est = NULL
        est = abs(optim(xi, F, mn = mn, vr = vr)$par)
        list(shape = est[1], scale = est[2])
    })
}
invgammaOBJ = list(name = "invgamma", mv2par = invgamma_mv2p, initF = nullF, dF = function(x, p) {
    try(dinvgamma(x, shape = p[1], scale = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pinvgamma(tau, shape = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dinvgamma(x, shape = p[1], scale = p[2]), col = clr)
})

# normal  ------------------------------------------------------------------------
# the normal family of PDFs, class A class A, standard R distribution
norm_mv2p = function(mn, vr) {
    try(list(mean = mn, sd = sqrt(vr)), silent = T)
}
normOBJ = list(name = "norm", initF = nullF, mv2par = norm_mv2p, dF = function(x, p) {
    try(dnorm(x, mean = p[1], sd = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pnorm(tau, mean = p[1], sd = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dnorm(x, mean = p[1], sd = p[2]), col = clr)
})

# gnorm  ------------------------------------------------------------------------
# generalized normal distribution, classA class B, TBD
dgnorm = function(x, mu, alpha, beta) {
    try(beta * exp(-(abs(x - mu)/alpha)^beta)/(2 * alpha * gamma(1/beta)), silent = T)
}
pgnorm = function(x, mu, alpha, beta, lower.tail) {
    p = 1/2 + sign(x - mu) * Igamma(1/beta, (abs(x - mu)/alpha)^beta)/(2 * gamma(1/beta))
    if (lower.tail == FALSE)
        1 - p
}

# lnorm  ------------------------------------------------------------------------
# the log-normal family of PDFs class A, standard R distribution
lnorm_mv2p = function(mn, vr) {
    try({
        ## alternate param
        sd <- sqrt(vr)
        mu <- log(mn/sqrt(1 + (sd^2/(mn^2))))
        sdlog <- sqrt(log(1 + (sd^2/mn^2)))

        # list(meanlog = est[1], sdlog = est[2])
        list(meanlog = mu[1], sdlog = sdlog[1])
    })
}
lnormOBJ = list(name = "lnorm", initF = nullF, mv2par = lnorm_mv2p, dF = function(x, p) {
    try(dlnorm(x, meanlog = p[1], sdlog = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    plnorm(tau, meanlog = p[1], sdlog = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dlnorm(x, meanlog = p[1], sdlog = p[2]), col = clr)
})

# lgnorm  ------------------------------------------------------------------------
# generalized log normal distribution, classA class A, TBD
lgnorm_mv2p = function(mn, vr) {
    try({
        F = function(x, mn, vr) {
            x = abs(x)
            mu = x[1]
            alpha = x[2]
            beta = mu^2/alpha
            mn1 = mu
            vr1 = (mn - mn1)^2 + (vr - vr1)^2
        }
        xi = c(log(mn), sqrt(vr))
        est = try(abs(optim(xi, F, mn = mn, vr = vr)$par))
        list(meanlog = est[1], sdlog = est[2])
    })
}
dglnorm = function(x, mu, alpha, beta) {
    try(beta * exp(-(abs(log(x) - mu)/alpha)^beta)/(2 * alpha * gamma(1/beta))/x, silent = T)
}
pglnorm = function(x, mu, alpha, beta, lower.tail) {
    p = 1/2 + sign(log(x) - mu) * Igamma(1/beta, (abs(log(x) - mu)/alpha)^beta)/(2 * gamma(1/beta))
    if (lower.tail == FALSE)
        1 - p
    p
}
glnormOBJ = list(name = "glnorm", initF = function(D) {
    list(mu = mean(log(D)), alpha = sd(log(D)), beta = 2)
}, dF = function(x, p) {
    try(dglnorm(x, mu = p[1], alpha = p[2], beta = p[3]), silent = T)
}, tailF = function(p, tau, lt) {
    pglnorm(tau, mu = p[1], alpha = p[2], beta = p[3], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dglnorm(x, mu = p[1], alpha = p[2], beta = p[3]), col = clr)
})

# exp  ------------------------------------------------------------------------
# the exponential family of PDFs, class A class A, standard R distribution
expOBJ = list(name = "exp", initF = nullF, mv2par = function(mn, vr) {
    try(list(rate = 1/mn), silent = T)
}, dF = function(x, p) {
    try(dexp(x, rate = p), silent = T)
}, tailF = function(p, tau, lt) {
    pexp(tau, rate = p, lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dexp(x, rate = p), col = clr)
})

# weibull  ------------------------------------------------------------------------
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
            l = x[1]  #scale
            k = x[2]  #shape
            mn1 = l * gamma(1 + 1/k)
            vr1 = l^2 * (gamma(1 + 2/k) - (gamma(1 + 1/k))^2)
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn/sqrt(vr), mn)
        F(xi, mn, vr)
        est = NULL
        est = try(abs(optim(xi, F, mn = mn, vr = vr)$par))
        list(shape = est[2], scale = est[1])
    })
}
weibullOBJ = list(name = "weibull", initF = nullF, mv2par = weibull_mv2p, dF = function(x, p) {
    try(dweibull(x, shape = p[1], scale = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pweibull(tau, shape = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dweibull(x, shape = p[1], scale = p[2]), col = clr)
})

# invweibull  ------------------------------------------------------------------------
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
            x = abs(x)
            mn1 = NULL
            mn1 = integrate(xinvweibull, 0, Inf, shape = x[1], scale = x[2],
                            rel.tol = 0.1, abs.tol = 0.1)$value
            m2 = NULL
            m2 = integrate(x2invweibull, 0, Inf, shape = x[1], scale = x[2],
                           rel.tol = 0.1, abs.tol = 0.1)$value
            vr1 = m2 - mn1^2
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(max(2.2, sqrt(vr)/mn), mn)
        F(xi, mn, vr)
        est = NULL
        est = try(abs(optim(xi, F, mn = mn, vr = vr)$par))
        list(shape = est[1], scale = est[2])
    })
}
invweibullOBJ = list(name = "invweibull", initF = nullF, mv2par = invweibull_mv2p, dF = function(x, p) {
    try(dinvweibull(x, shape = p[1], scale = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pinvweibull(tau, shape = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dinvweibull(x, shape = p[1], scale = p[2]), col = clr)
})

# llogis  ------------------------------------------------------------------------
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
            x = abs(x)
            a = x[1]
            b = pi/x[2]
            mn1 = NULL
            mn1 = a * b/sin(b)
            vr1 = NULL
            vr1 = a^2 * (2 * b/sin(2 * b) - b^2/(sin(b)^2))
            (mn1 - mn)^2 + (vr1 - vr)^2
        }
        xi = c(mn, max(2, mn))
        F(xi, mn, vr)
        est = try(abs(optim(xi, F, mn = mn, vr = vr)$par))
        list(shape = est[2], scale = est[1])
    })
}
llogisOBJ = list(name = "llogis", initF = nullF, mv2par = llogis_mv2p, dF = function(x, p) {
    try(dllogis(x, shape = p[1], scale = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pllogis(tau, shape = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dllogis(x, shape = p[1], scale = p[2]), col = clr)
})

# gumbel  ------------------------------------------------------------------------
# the gumbel family of PDFs, class A class A, defined below
gumbel_mv2p = function(mn, vr) {
    try({
        list(alpha = mn - EULERS_CONSTANT * sqrt(vr) * sqrt(6)/pi, scale = sqrt(vr) * sqrt(6)/pi)
    })
}
gumbelOBJ = list(name = "gumbel", mv2par = gumbel_mv2p, initF = function(D) {
    list(alpha = mean(D) - EULERS_CONSTANT * sd(D) * sqrt(6)/pi, scale = sd(D) * sqrt(6)/pi)
}, dF = function(x, p) {
    try(dgumbel(x, alpha = p[1], scale = p[2]), silent = T)
}, tailF = function(p, tau, lt) {
    pgumbel(tau, alpha = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dgumbel(x, alpha = p[1], scale = p[2]), col = clr)
})

# mgumbel  ------------------------------------------------------------------------
# the mirrored gumbel family of PDFs, class A class M, defined below NOTE: XMAX should be defined as a global parameter
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
    list(alpha = XMAX - mn - EULERS_CONSTANT * sqrt(vr) * sqrt(6)/pi, scale = sqrt(vr) * sqrt(6)/pi)
}
mgumbelOBJ = list(name = "mgumbel", initF = function(D) {
    inits = list(alpha = XMAX - mean(D) + EULERS_CONSTANT * sd(D) * sqrt(6)/pi, scale = sd(D) * sqrt(6)/pi)
    fitdist(XMAX - D, "gumbel", start = inits)$estimate
}, mv2par = mgumbel_mv2p, dF = function(x, p) {
    dmgumbel(x, alpha = p[1], scale = p[2])
}, tailF = function(p, tau, lt) {
    # NOTE: take the other tail
    pmgumbel(tau, alpha = p[1], scale = p[2], lower.tail = lt)
}, plotF = function(x, p, clr) {
    lines(x, dmgumbel(x, alpha = p[1], scale = p[2]), col = clr)
})

# betasr  ------------------------------------------------------------------------
# the beta distribution, over a shifted range class B, defined below
dbetasr = function(x, shape1, shape2, XMIN, XMAX) {
    try(dbeta((x - XMIN)/(XMAX - XMIN), shape1, shape2)/(XMAX - XMIN), silent = T)
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

        list(shape1 = m1^2 * (1 - m1)/v1 - m1, shape2 = m1 * (1 - m1)^2/v1 - 1 + m1, XMIN = XMIN, XMAX = XMAX)
    })
}
betasrOBJ = list(name = "betasr", mv2par = betasr_mv2p, initF = function(D) {
    betasr_mv2p(mean(D), var(D), XMIN(D), XMAX(D))
}, dF = function(x, p) {
    try(dbetasr(x, shape1 = p[1], shape2 = p[2], XMIN = p[3], XMAX = p[4]), silent = T)
}, tailF = function(p, tau, lt) {
    pbetasr(tau, shape1 = p[1], shape2 = p[2], lt)
}, plotF = function(x, p, clr) {
    lines(x, dbetasr(x, shape1 = p[1], shape2 = p[2], XMIN = p[3], XMAX = p[4]), col = clr)
})


# classA distlist
classA1 = list(exp = expOBJ)
classA2 = list(gamma = gammaOBJ, invgamma = invgammaOBJ, llogis = llogisOBJ,
               gumbel = gumbelOBJ, invweibull = invweibullOBJ, weibull = weibullOBJ,
               lnorm = lnormOBJ, norm = normOBJ)
classA3 = list(glnorm = glnormOBJ)
classA = c(classA1, classA2, classA3)
# classM distlist  ------------------------------------------------------------------------
# Mirrored classA distributions XMAX must be defined as a global variable
classM = list(mgamma = mgammaOBJ, mgumbel = mgumbelOBJ)
classB = list(betasr = betasrOBJ)
