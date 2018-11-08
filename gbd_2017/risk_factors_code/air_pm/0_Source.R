joint.samp.inla.downscaling <- function(dat,
                                        N=1000,
                                        samp = NULL,
                                        INLAOut,
                                        A = A,
                                        prefix = 'pred_',
                                        spat.slope,
                                        keep = NULL){
  # Creating Samples if none specified
  if (is.null(samp)){
    samp <- inla.posterior.sample(n = N, INLAOut$INLAObj)
  }
  # Number of samples
  #N <- length(samp)
  # Fixed Effects
  fixed <- rownames(INLAOut$INLAObj$summary.fixed)
  # Random Effects
  random <- names(INLAOut$INLAObj$summary.random)
  random <- random[!grepl('index',random)]
  # Looping for each fixed effect
  for (i in 1:N){
    # Empty column for prediction
    dat[,paste(prefix, i, sep = '')] <- as.numeric(0)
    for (j in 1:length(fixed)){
      # Intercept
      if (fixed[j] %in% c('(Intercept)','intercept')) {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] + samp[[i]]$latent[fixed[j],]
      }
      # Interaction terms
      else if (grepl(':',fixed[j])){
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
          samp[[i]]$latent[fixed[j],] * dat[,strsplit(fixed[j],':')[[1]][1]] * dat[,strsplit(fixed[j],':')[[1]][2]]
      }
      # Other variables
      else {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +  samp[[i]]$latent[fixed[j],] * dat[,fixed[j]]
      }
    }
    check <- rep(0, length(random))
    # Looping for each random effect
    for (k in 1:length(random)){
      tmp <- data.frame(Val = rep(0,length(unique(dat[,random[k]]))))
      rownames(tmp) <- unique(dat[,random[k]])
	  if (grepl('Country3',random[k])){
	        tmp <- data.frame(Val = rep(0,237))
	        rownames(tmp) <- as.character(1:237)
	      }
      tmp[substr(rownames(samp[[i]]$latent)[grep(random[k],rownames(samp[[i]]$latent))],nchar(random[k]) + 2,1000),'Val'] <-
        samp[[i]]$latent[grep(random[k],rownames(samp[[i]]$latent))]
      if (grepl('Country3',random[k])) {tmp[c("1","2","10","14","16","18","20","28","41","42","44","46","50","51","54","66","67",
                                                "68","69","70","77","78","81","91","97","100","104","106","119","120","122","124","131",
                                                "139","140","141","143","148","154","161","164","165","171","172","173","175","182",
                                                "187","190","191","192","194","197","207","209","212","214","215","219","231"),'Val'] <- 0}
      for (l in 1:length(fixed)){
        if(grepl(l, random[k])){
          if (fixed[l] %in% c('(Intercept)','intercept')){
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
          }
          else {
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],] * dat[,fixed[l]]
          }
          check[k]<-1
          break
        }
      }
      if (check[k] == 0){
        dat[,paste(prefix, i, sep = '')] <-
          dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
        check[k]<-1
      }
      # Removing unecessary columns
      rm(tmp)
    }
    # Adding spatial random effects
    dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
      as.numeric(A %*% samp[[i]]$latent[grep('index1:',rownames(samp[[i]]$latent)),]) + # Spatial Intercept
      as.numeric(A %*% samp[[i]]$latent[grep('index2:',rownames(samp[[i]]$latent)),]) * dat[,deparse(substitute(spat.slope))] # Spatial slope for CTM
    # Printing index
    if (i %% 10 == 0 | N <= 10){print(paste('Predictions done:', i))}
  }
  return(dat[c(keep,paste(prefix, 1:N, sep = ''))])
}