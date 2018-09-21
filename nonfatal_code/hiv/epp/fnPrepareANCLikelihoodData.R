fnPrepareANCLikelihoodData <- function (anc.prev, anc.n, anc.used = TRUE, anchor.year = 1970L,
    return.data = TRUE) {
    anc.prev <- anc.prev[anc.used, , drop = F]
    anc.n <- anc.n[anc.used, , drop = F]
    anc.prev <- anc.prev[apply(!is.na(anc.prev), 1, sum) > 0, , drop = F]
    anc.n <- anc.n[apply(!is.na(anc.n), 1, sum) > 0, , drop = F]
    ancobs.idx <- mapply(intersect, lapply(as.data.frame(t(!is.na(anc.prev))),
        which), lapply(as.data.frame(t(!is.na(anc.n))), which),
        SIMPLIFY = FALSE)
    anc.years.lst <- lapply(ancobs.idx, function(i) as.integer(colnames(anc.prev)[i]))
    anc.prev.lst <- setNames(lapply(1:length(ancobs.idx), function(i) as.numeric(anc.prev[i,
        ancobs.idx[[i]]])), rownames(anc.prev))
    anc.n.lst <- setNames(lapply(1:length(ancobs.idx), function(i) as.numeric(anc.n[i,
        ancobs.idx[[i]]])), rownames(anc.n))
    x.lst <- mapply(function(p, n) (p * n + 0.5)/(n + 1), anc.prev.lst,
        anc.n.lst, SIMPLIFY = FALSE)
    W.lst <- lapply(x.lst, qnorm)
    v.lst <- mapply(function(W, x, n) 2 * pi * exp(W^2) * x *
        (1 - x)/n, W.lst, x.lst, anc.n.lst, SIMPLIFY = FALSE)
    anc.idx.lst <- lapply(anc.years.lst, "-", anchor.year - 1)
    anclik.dat <- list(W.lst = W.lst, v.lst = v.lst, n.lst = anc.n.lst,
        anc.idx.lst = anc.idx.lst)
    if (return.data) {
        anclik.dat$anc.prev <- anc.prev
        anclik.dat$anc.n <- anc.n
    }
    return(anclik.dat)
}
