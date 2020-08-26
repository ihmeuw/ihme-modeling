simmod <- function(fp, ...) UseMethod("simmod")
simfit <- function(fit, ...) UseMethod("simfit")

prev <- function(mod, ...) UseMethod("prev")
fnPregPrev <- function(mod, fp, ...) UseMethod("fnPregPrev")
incid <- function(mod, ...) UseMethod("incid")

incid_sexratio <- function(mod, ...) UseMethod("incid_sexratio")

agemx <- function(mod, ...) UseMethod("agemx")
natagemx <- function(mod, ...) UseMethod("natagemx")
calc_nqx <- function(mod, ...) UseMethod("calc_nqx")

pop15to49 <- function(mod, ...) UseMethod("pop15to49")
artpop15to49 <- function(mod, ...) UseMethod("artpop15to49")
artpop15plus <- function(mod, ...) UseMethod("artpop15plus")
artcov15to49 <- function(mod, ...) UseMethod("artcov15to49")
artcov15plus <- function(mod, ...) UseMethod("artcov15plus")
age15pop <- function(mod, ...) UseMethod("age15pop")
