#############################################################
# 
#  ihmeDistSelect
#  Compare, predict and select distributions 
#  Version 0.4
#  Jan 4, 2017
# 
#  Fit and select a probability distribution function (PDF). 
#############################################################

SQ = function(x){x^2}

if(!exists("ListsExist")) source ("ihmeDistList.R")

if(!exists("ihmeDistFit")) source ("ihmeDistFit.R")

if(!exists("NOISY")) NOISY = FALSE

if(!exists("runTest")) runTest = TRUE 

ihmeDistSelect = function(DataSets, pdfOBJ, threshold, whichTail, testP=0.2){

  N = length(DataSets)

  L = length(pdfOBJ) 

  aicRanks        = data.frame(matrix(0,N,L)) 
  names(aicRanks) = names(pdfOBJ)

  mleErrRanks        = data.frame(matrix(0,N,L)) 
  names(mleErrRanks) = names(pdfOBJ)

  mleErrs            = data.frame(matrix(0,N,L)) 
  names(mleErrs)     = names(pdfOBJ)

  mlePred            = data.frame(matrix(0,N,L)) 
  names(mlePred)     = names(pdfOBJ)

  MoMerrRanks        = data.frame(matrix(0,N,L)) 
  names(MoMerrRanks) = names(pdfOBJ)

  MoMpred            = data.frame(matrix(0,N,L)) 
  names(MoMpred)     = names(pdfOBJ)

  MoMerrs            = data.frame(matrix(0,N,L)) 
  names(MoMerrs)     = names(pdfOBJ)

  obs 	          = rep(0,N) 


  for(i in 1:N){

    if(NOISY) print(c(i=i))

    tmp = ihmeDistFit(DataSets[[i]], pdfOBJ, threshold, whichTail)

    aicRanks[i,] = tmp$dAIC$ranks

    mleErrRanks[i,] = tmp$Tails[[1]]$MLE_tails$MLE_ranks

    mleErrs[i,] = tmp$Tails[[1]]$MLE_tails$MLE_errors

    mlePred[i,] = tmp$Tails[[1]]$MLE_tails$MLE_predicted

    MoMerrRanks[i,] = tmp$Tails[[1]]$MoM_tails$MoM_ranks

    MoMerrs[i,] = tmp$Tails[[1]]$MoM_tails$MoM_errors

    MoMpred[i,] = tmp$Tails[[1]]$MoM_tails$MoM_predicted

    obs[i] = tmp$Tails[[1]]$obs
  }
  
  aicAVG = colSums(aicRanks)/N
  
  mlePredAVG = colSums(mleErrRanks)/N
  
  MoMpredAVG = colSums(MoMerrRanks)/N
  
  results = data.frame(avg_aic_rank = aicAVG, avg_mle_rank = mlePredAVG, avg_MoM_rank = MoMpredAVG) 

  rownames(results) = names(pdfOBJ) 

  #######################################################
  # Split DataSets into a Training Set and a Testing Set
  #######################################################
  
  ixTrain = sample(1:N, (1-testP)*N)
  
  ixTest = c(1:N)[-ixTrain]
  
  #browser() 
  
  mleTrainOBJ= ensembleTrain(obs[ixTrain], as.matrix(mlePred[ixTrain,])) 
   
  mleTrainOBJ = ensembleTest(obs[ixTest], as.matrix(mlePred[ixTest,]), mleTrainOBJ)

  MoMtrainOBJ = ensembleTrain(obs[ixTrain], as.matrix(MoMpred[ixTrain,])) 

  MoMtrainOBJ = ensembleTest(obs[ixTest], as.matrix(MoMpred[ixTest,]), MoMtrainOBJ)

  return(
    list(N=N,L=L,
       aicRanks     = aicRanks, 
       mlePredRanks    = mleErrRanks, 
       MoMpredRanks    = MoMerrRanks, 
       mlePredictions  = mlePred, 
       mleErrors       = mleErrs,
       MoMpredictions  = MoMpred, 
       MoMerrors       = MoMerrs,
       observations = obs, 
       ixTrain      = ixTrain,
       ixTest       = ixTest,
       MLEcompare   = mleTrainOBJ,
       MoMcompare   = MoMtrainOBJ,
       results      = results
     )
  ) 
}

getWts = function(observed, predicted, wts.i, NRandomStarts = 5){

  i = length(wts.i)
  
  sum.wtd.errs = function(wts, obs, prd){

    wts = wts/sum(wts) 

    sum(SQ(obs-prd%*%wts))

  }

  out = optim(wts.i, sum.wtd.errs, obs=observed, prd =predicted)

  wts = out$par

  vals = out$value

  wts = c(wts/sum(wts))

  for(j in 1:NRandomStarts){

    wts.i = runif(i,-1,1)

    wts1 = optim(wts.i, sum.wtd.errs, obs=observed, prd =predicted, method = "SANN")$par

    out = optim(wts1, sum.wtd.errs, obs=observed, prd =predicted)

    wts1 = out$par

    wts = rbind(wts, wts1/sum(wts1)) 

    vals = c(vals, out$value)
  }
  
  bestWTS = wts[which.min(vals),]

  return(bestWTS/sum(bestWTS))  
}

ensembleTest = function(observed, predicted, trainOBJ){

  N = length(observed)

  test.errs = colSums(SQ(predicted-observed))/N

  J = length(trainOBJ$ensms.prd)

  all.predictions = matrix(0, N, J)
  
  for(i in 1:J){

   ix = trainOBJ$ensms.prd[[i]]$ix 

   wts = unlist(trainOBJ$ensms.prd[[i]]$wts)

   prd = predicted[,ix]%*%wts

   all.predictions[,i] = prd

   test.errs = c(test.errs, sum(SQ(observed-prd))/N) 

  }

  trainOBJ$results$test.errs=test.errs

  trainOBJ$results$test.ranks=rank(test.errs)
  
  return(trainOBJ)  
}

ensembleTrain = function(observed, predicted){
  
  J = dim(predicted)[2]

  N = dim(predicted)[1]
  
  tot.err = data.frame(err = colSums(SQ(predicted-observed))/N)
  
  ranks = data.frame(rank(tot.err))

  nms = rownames(tot.err) 

  rownames(ranks) = nms 

  ensms.predict = list()
  
  wts.i = c(1,0)
 
  all.weights = data.frame(diag(J)) 

  names(all.weights) = nms

  zeros = rep(0,J) 
  
  for(i in 2:J){

     ix = which(ranks<=i)

     wts = getWts(observed, predicted[,ix], wts.i)

     nms = c(nms, paste("best",i,sep="")) 

     mod.weights = zeros

     mod.weights[ix] = wts
     
     all.weights = rbind(all.weights, mod.weights)  
     
     wtd.pred = predicted[,ix]%*%wts 

     wts.i = c(wts, 0.01)
     
     wts = data.frame(wts)
     
     errs = wtd.pred-observed  
     
     tot.err = rbind(tot.err, sum(SQ(errs))/N)

     ensms.predict[[i-1]] = list(N=i,wtd.pred=wtd.pred,errs=errs,ix=ix, wts=wts) 
  }
  
  rownames(all.weights) = nms 
 
  results = tot.err
  
  results$ranks = rank(tot.err)

  rownames(results) = nms
  
  ix = which(results$ranks == 1)
  
  lng = length(results$ranks)
  
  return(list(ensms.prd = ensms.predict, best=all.weights[ix,], full = all.weights[lng,], weights=all.weights, results = results))  
}


plotDistSelect = function(selectOBJ){with(selectOBJ,{

  par(mfrow= c(2,2))

  plot(MLEcompare$results$ranks, MLEcompare$results$test.ranks, xlab = "Training Rank", ylab = "Test Rank", xlim = c(1,2*L-1), ylim = c(1,2*L-1), main = "Testing Set")
  segments(1,1,2*L-1,2*L-1)
  
  mnx = min(sqrt(MLEcompare$results$test.errs))
  
  plot(sqrt(MLEcompare$results$test.errs), type = "h", ylim = c(0,mnx*8), xlab = "Model Index", ylab = "Prediction Errors", yaxt = "n")
  
  axis(2, c(0, 0.01, 0.02, 0.03), c("0", "1%", "2%", "3%"))
  
  segments(L+0.5,0,L+0.5,0.02, col = "blue", lty = 2) 

  text(L+1, mnx*4, "Ensemble Models", pos = 4) 
  
  J = length(MLEcompare$ensms.prd)
  
  prd = as.matrix(mlePredictions[ixTest,])
  
  wts = unlist(MLEcompare$ensms.prd[[J]]$wts)
  
  obs = observations[ixTest]
  
  ens.prd = prd%*%wts
  rng = range(c(0,ens.prd, obs))
  plot(obs, ens.prd, pch = 15, xlim = rng, ylim = rng)
  segments(rng[1], rng[1], rng[2], rng[2])
  
  K = dim(mlePredictions)[2]
  for(i in 1:K){
    points(obs, prd[,i], col = i+5)
  } 
  
})}

densembleMLE = function(x, wts, Data, pdfOBJ, threshold, whichTail){
  
  fits = ihmeDistFit(Data, pdfOBJ, threshold, whichTail) 
  
  fx = 0
  
  for(i in 1:length(pdfOBJ))
    fx = fx + wts[i]*pdfOBJ[[i]]$dF(x,fits$MLE_Fits[[i]]$estimate) 
  
  return(fx) 
} 

densembleMoM = function(x, wts, mn, vr, pdfOBJ, threshold, whichTail){
 
  fx=0 
  for(i in 1:length(pdfOBJ)){ 
    est = unlist(pdfOBJ[[i]]$mv2par(mn,vr))
    fx = fx + wts[i]*pdfOBJ[[i]]$dF(x,est) 
  } 

  return(fx) 
} 

pensembleMLE = function(p, wts, Data, pdfOBJ, whichTail){
  
  fits = ihmeDistFit(Data, pdfOBJ, p, whichTail) 
  
  px = 0
  
  for(i in 1:length(pdfOBJ))
    px = c(px, pdfOBJ[[i]]$tailF(fits$MLE_Fits[[i]]$estimate, p, whichTail))

  return(sum(px[-1]*wts)) 
} 

pensembleMoM = function(p, wts, mn, vr, pdfOBJ, whichTail){
  
  px = 0
  for(i in 1:length(pdfOBJ)){ 
    est = unlist(pdfOBJ[[i]]$mv2par(mn,vr)) 
    px = c(px, pdfOBJ[[i]]$tailF(est, p, whichTail))
  } 
  return(sum(px[-1]*wts)) 
} 

