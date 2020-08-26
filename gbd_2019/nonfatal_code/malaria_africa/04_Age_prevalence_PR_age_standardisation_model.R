

ssize.tmean = c( 0.03916203, 0.04464933, 0.0464907, 0.04703639,
0.04468962, 0.04430289, 0.04088593, 0.04048751, 0.0385978,
0.03586601, 0.03665516, 0.02850493, 0.03038655, 0.02493861,
0.02169105, 0.01506840, 0.01481482, 0.01519726, 0.01474023,
0.01553814, 0.01193805, 0.01220409, 0.01202119, 0.01202534,
0.01283806, 0.01083728, 0.01095784, 0.01092874, 0.01067932,
0.01137331, 0.01035001, 0.01035001, 0.01035001, 0.01035001,
0.01035001, 0.008342426, 0.008342426, 0.008342426, 0.008342426,
0.008342426, 0.00660059, 0.00660059, 0.00660059, 0.00660059,
0.00660059, 0.004597861, 0.004597861, 0.004597861, 0.004597861,
0.004597861, 0.003960774, 0.003960774, 0.003960774, 0.003960774,
0.003960774, 0.003045687, 0.003045687, 0.003045687, 0.003045687,
0.003045687, 0.002761077, 0.002761077, 0.002761077, 0.002761077,
0.002761077, 0.001275367, 0.001275367, 0.001275367, 0.001275367,
0.001275367, 0.001275367, 0.001275367, 0.001275367, 0.001275367,
0.001275367, 0.001275367, 0.001275367, 0.001275367, 0.001275367,
0.001275367, 0.001275367, 0.001275367, 0.001275367, 0.001275367,
0.001275367) 

a.best = 9.422033
s.best = 0.446326
c.best = 0.070376 
r.best = 1.807675
 
std.curve=function(x,age, 
    aa=a.best, sens=s.best, c2=c.best, r=r.best)
{
  ss = exp(-sens)  
  sl = r
  decl=1-ss + ss*pmin(1,exp(-c2*(age-aa)))
  x*(1-exp(-sl*age))*decl
}

std.curve.wtdavg=function(x,mn,mx,WTS=ssize.tmean,
  aa=a.best, sens=s.best, c2=c.best, r=r.best)
{
  age=c(mn:(mx-1))+0.5
  pr=std.curve(x,age,aa=aa,sens=sens,c2=c2,r=r)
  ix=ceiling(age)
  prwt=weighted.mean(pr,WTS[ix])
  prwt 
}

invert.std.curve=function(PR,mn,mx,WTS=ssize.tmean,
  aa=a.best, sens=s.best, c2=c.best, r=r.best)
{
  ddf=function(x,PR,mn,mx, WTS=WTS, aa=aa, sens=sens, c2=c2, r=r)
  {
    wtavg=std.curve.wtdavg(x,mn,mx,WTS=WTS,aa=aa,sens=sens,c2=c2,r=r) 
    (PR - wtavg)^2
  }
  #### this is the bottle neck
  newPR=optimize(ddf,c(0,1),PR=PR,mn=mn,mx=mx,WTS=WTS, 
    aa=aa,sens=sens,c2=c2,r=r)$min 
  newPR
}

invert.std.curve.i = function(i,PR,mn,mx, L, U,WTS=ssize.tmean,
  aa=a.best, sens=s.best, c2=c.best, r=r.best)
{
  xx = invert.std.curve(PR[i], mn[i], mx[i], WTS=WTS, aa=aa,sens=sens,c2=c2,r=r) 
  pPR = std.curve.wtdavg(xx, L, U, WTS=WTS, aa=aa, sens=sens, c2=c2,r=r)
  pPR
}

convert = function(DT, L=2, U=9)
{
  lng = dim(DT)[1]  
  U = min(U,85) 
  PR.LU = sapply(1:lng, invert.std.curve.i, 
	PR=DT$PR, mn=DT$mn, mx=DT$mx, L=L, U=U) 
}

convert_1_99 = function(DT, L=1, U=99)
{
  lng = dim(DT)[1]  
  U = min(U,85) 
  PR.LU = sapply(1:lng, invert.std.curve.i, 
	PR=DT$PR, mn=DT$mn, mx=DT$mx, L=L, U=U) 
}

convert_0_5 = function(DT, L=0, U=5)
{
  lng = dim(DT)[1]  
  U = min(U,85) 
  PR.LU = sapply(1:lng, invert.std.curve.i, 
	PR=DT$PR, mn=DT$mn, mx=DT$mx, L=L, U=U) 
}

convert_26 = function(DT, L=26, U=26)
{
  lng = dim(DT)[1]  
  U = min(U,85) 
  PR.LU = sapply(1:lng, invert.std.curve.i, 
	PR=DT$PR, mn=DT$mn, mx=DT$mx, L=L, U=U) 
}


#mn.test = c(1,2,3,4,5)
#mx.test = c(10,20,30,40,50)
#PR.test = c(0.1, 0.2, 0.3, 0.4, 0.5) 

#DT.test = data.frame(mn=mn.test, mx=mx.test, PR=PR.test)

#pPR = convert(DT.test)
#pPR.all = convert(DT.test,L=0,U=85)
