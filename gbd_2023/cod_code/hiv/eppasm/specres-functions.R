incid.specres <- function(x){
  nyr <- ncol(x$newinf.m)
  infections <- colSums(x$newinf.m[4:10,-1]+x$newinf.f[4:10,-1])
  hivn <- colSums(x$totpop.m[4:10,-nyr]+x$totpop.f[4:10,-nyr]-(x$hivnum.m[4:10,-nyr]+x$hivnum.f[4:10,-nyr]))
  c(0, infections / hivn)
}
    

prev.specres <- function(x) colSums(x$hivnum.m[4:10,]+x$hivnum.f[4:10,])/colSums(x$totpop.m[4:10,]+x$totpop.f[4:10,])

aidsdeaths.specres <- function(x) colSums(x$aidsdeaths.m[-(1:3),]+x$aidsdeaths.f[-(1:3),])

agemx.specres <- function(specres, nonhiv=FALSE){

  if(nonhiv)
    deaths <- specres$natdeaths
  else
    deaths <- with(specres, natdeaths+hivdeaths)
  pop <- with(specres, (totpop[,,-1]+totpop[,,-dim(totpop)[3]])/2)

  mx <- array(0, dim=dim(deaths), dimnames(deaths))
  mx[,,-1] <- deaths[,,-1] / pop

  return(mx)
}

natagemx.specres <- function(specres){

  deaths <- specres$natdeaths
  pop <- with(specres, (totpop[,,-1]+totpop[,,-dim(totpop)[3]])/2)

  mx <- array(0, dim=dim(deaths), dimnames(deaths))
  mx[,,-1] <- deaths[,,-1] / pop

  return(mx)
}

calc_nqx.specres <- function(specres, n=45, x=15, nonhiv=FALSE){
  if(nonhiv)
    mx <- natagemx(specres)
  else
    mx <- agemx(specres)
  1-exp(-colSums(mx[as.character(x+0:(n-1)),,]))
}


aggr_specres <- function(specreslist){
  out <- lapply(do.call(mapply, c(FUN=list, specreslist, SIMPLIFY=FALSE)), Reduce, f="+")
  class(out) <- "specres"
  return(out)
}

pop15to49.specres <- function(specres){colSums(specres$totpop[as.character(15:49),,],,2)}
artpop15to49.specres <- function(specres){colSums(specres$artnum.m[4:10,]+specres$artnum.f[4:10,])}
artpop15plus.specres <- function(specres){colSums(specres$artnum.m[4:17,]+specres$artnum.f[4:17,])}
artcov15to49.specres <- function(specres){colSums(specres$artnum.m[4:10,]+specres$artnum.f[4:10,]) / colSums(specres$hivnum.m[4:10,]+specres$hivnum.f[4:10,])}
artcov15plus.specres <- function(specres){colSums(specres$artnum.m[4:17,]+specres$artnum.f[4:17,]) / colSums(specres$hivnum.m[4:17,]+specres$hivnum.f[4:17,])}
age15pop.specres <- function(specres){colSums(specres$totpop["15",,])}

ageprev.specres <- function(specres, aidx=NULL, sidx=NULL, yidx=NULL, agspan=5, arridx=NULL){

  if(is.null(arridx)){
    if(length(agspan)==1)
      agspan <- rep(agspan, length(aidx))
    
    dims <- dim(mod)
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    arridx <- idx$aidx + (idx$sidx-1)*dims[1] + (idx$yidx-1)*dims[1]*dims[2]
    agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else if(length(agspan)==1)
    agspan <- rep(agspan, length(arridx))
  
  agidx <- rep(arridx, agspan)
  allidx <- agidx + unlist(sapply(agspan, seq_len))-1
  
  hivn <- fastmatch::ctapply(mod[,,1,][allidx], agidx, sum)
  hivp <- fastmatch::ctapply(mod[,,2,][allidx], agidx, sum)
  
  prev <- hivp/(hivn+hivp)
  if(!is.null(aidx))
    prev <- array(prev, c(length(aidx), length(sidx), length(yidx)))
  return(prev)
}


## MAYBE NEED TO CORRECT THIS FUNCTION FOR SUSCEPTIBLES YEAR EARLIER
incid_sexratio.specres <- function(x){
  incid.m <- colSums(x$newinf.m[4:10,]) / colSums(x$totpop.m[4:10,] - x$hivnum.m[4:10,])
  incid.f <- colSums(x$newinf.f[4:10,]) / colSums(x$totpop.f[4:10,] - x$hivnum.f[4:10,])
  return(incid.f / incid.m)
}


fnPregPrev.specres <- function(specres){
  specres$hivpregwomen / specres$births
}
