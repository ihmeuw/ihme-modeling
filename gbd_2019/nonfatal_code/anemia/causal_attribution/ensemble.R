
EULERS_CONSTANT = 0.57721566490153286060651209008240243104215933593992

#############################################################
# gamma :: the gamma family of PDFs, class A
#          class A, standard R distribution
#############################################################

gamma_mv2p <-  function(mn, vr) {
  list(shape = mn^2/vr,
       rate = mn/vr)
}

#############################################################
# mgumbel :: the mirrored gumbel family of PDFs, class A
#        class M, defined below
#
# NOTE: XMAX should be defined as a global parameter
#
#############################################################

pmgumbel <- function(q, alpha, scale, lower.tail) {
  # NOTE: with mirroring, take the other tail
  return(pgumbel(q = XMAX-q,
                 alpha = alpha,
                 scale = scale,
                 lower.tail = ifelse(lower.tail,FALSE,TRUE)))
}

mgumbel_mv2p <- function(mn, vr) {
  list(
    alpha = XMAX - mn - EULERS_CONSTANT*sqrt(vr)*sqrt(6)/pi,
    scale = sqrt(vr)*sqrt(6)/pi
  )
}
