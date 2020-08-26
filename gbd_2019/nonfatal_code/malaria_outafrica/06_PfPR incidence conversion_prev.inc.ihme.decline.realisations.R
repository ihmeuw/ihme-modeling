### R script to generate a full suite of burden estimates and maps: PfPR realization version

## load R libraries
library(raster)
library(compiler)

library(doMC)
registerDoMC(64)
load("j.num.list")

load("post.ensemble.fin.dat")

## define prev.inc function 
build.prev.inc <- function(age.group=c("infants","children","adults")[1],j.nums) {

    if (age.group=="infants") {
        
        prev.inc.means <- function(prevalence,seasonality=0,decline=0.5) {

            load("pcurve.dat")
            decline <- 1/approxfun(seq(0,1,length.out=100),pcurve)(prevalence)
            decline[decline > 1] <- 1
            decline[decline < 0] <- 0

            prev <- seq(0,1,length.out=100)
            inc.low.dec <- post.ensemble$lowseason.lowtreatment.90dec$infants[,j.nums[1]]
            inc.low.nodec <- post.ensemble$lowseason.lowtreatment$infants[,j.nums[2]]
            inc.high.dec <- post.ensemble$highseason.lowtreatment.90dec$infants[,j.nums[3]]
            inc.high.nodec <- post.ensemble$highseason.lowtreatment$infants[,j.nums[4]]
            inc.low.nodec[61:100] <- inc.low.nodec[60]
            inc.high.nodec[61:100] <- inc.high.nodec[60]
            inc.low.dec[61:100] <- inc.low.dec[60]
            inc.high.dec[61:100] <- inc.high.dec[60]
            f.low.nodec <- approxfun(prev,inc.low.nodec)
            f.high.nodec <- approxfun(prev,inc.high.nodec)
            f.low.dec <- approxfun(prev,inc.low.dec)
            f.high.dec <- approxfun(prev,inc.high.dec)
            return(decline*(f.low.dec(prevalence)*(1-seasonality)+f.high.dec(prevalence)*seasonality)+(1-decline)*(f.low.nodec(prevalence)*(1-seasonality)+f.high.nodec(prevalence)*seasonality))
        }
        return(prev.inc.means)
    }     

    if (age.group=="children") {
        
        prev.inc.means <- function(prevalence,seasonality=0,decline=0.5) {

        load("pcurve.dat")
            decline <- 1/approxfun(seq(0,1,length.out=100),pcurve)(prevalence)
            decline[decline > 1] <- 1
            decline[decline < 0] <- 0

            prev <- seq(0,1,length.out=100)
            inc.low.dec <- post.ensemble$lowseason.lowtreatment.90dec$children[,j.nums[5]]
            inc.low.nodec <- post.ensemble$lowseason.lowtreatment$children[,j.nums[6]]
            inc.high.dec <- post.ensemble$highseason.lowtreatment.90dec$children[,j.nums[7]]
            inc.high.nodec <- post.ensemble$highseason.lowtreatment$children[,j.nums[8]]
            inc.low.nodec[61:100] <- inc.low.nodec[60]
            inc.high.nodec[61:100] <- inc.high.nodec[60]
            inc.low.dec[61:100] <- inc.low.dec[60]
            inc.high.dec[61:100] <- inc.high.dec[60]
            f.low.nodec <- approxfun(prev,inc.low.nodec)
            f.high.nodec <- approxfun(prev,inc.high.nodec)
            f.low.dec <- approxfun(prev,inc.low.dec)
            f.high.dec <- approxfun(prev,inc.high.dec)
            return(decline*(f.low.dec(prevalence)*(1-seasonality)+f.high.dec(prevalence)*seasonality)+(1-decline)*(f.low.nodec(prevalence)*(1-seasonality)+f.high.nodec(prevalence)*seasonality))

        }
        return(prev.inc.means)
    }

    if (age.group=="adults") {
        
        prev.inc.means <- function(prevalence,seasonality=0,decline=0.5) {

         load("pcurve.dat")
            decline <- 1/approxfun(seq(0,1,length.out=100),pcurve)(prevalence)
            decline[decline > 1] <- 1
            decline[decline < 0] <- 0

            prev <- seq(0,1,length.out=100)
            inc.low.dec <- post.ensemble$lowseason.lowtreatment.90dec$adults[,j.nums[9]]
            inc.low.nodec <- post.ensemble$lowseason.lowtreatment$adults[,j.nums[10]]
            inc.high.dec <- post.ensemble$highseason.lowtreatment.90dec$adults[,j.nums[11]]
            inc.high.nodec <- post.ensemble$highseason.lowtreatment$adults[,j.nums[12]]
            inc.low.nodec[61:100] <- inc.low.nodec[60]
            inc.high.nodec[61:100] <- inc.high.nodec[60]
            inc.low.dec[61:100] <- inc.low.dec[60]
            inc.high.dec[61:100] <- inc.high.dec[60]
            f.low.nodec <- approxfun(prev,inc.low.nodec)
            f.high.nodec <- approxfun(prev,inc.high.nodec)
            f.low.dec <- approxfun(prev,inc.low.dec)
            f.high.dec <- approxfun(prev,inc.high.dec)
            return(decline*(f.low.dec(prevalence)*(1-seasonality)+f.high.dec(prevalence)*seasonality)+(1-decline)*(f.low.nodec(prevalence)*(1-seasonality)+f.high.nodec(prevalence)*seasonality))
            
         }
        return(prev.inc.means)
    }
      
}

## load seasonality map
seasonality <- raster("seasonality.tif")

output.list <- foreach (run.number=1:100) %dopar% {

output.list <- list()

gmap <- raster("african_cn5km_2013_no_disputes.tif")
gmapv <- getValues(gmap)
seasonalityv <- getValues(seasonality)

        infants.prev.inc <- build.prev.inc(age.group="infants",j.num.list[((run.number-1)*12+1):((run.number*12))])
        children.prev.inc <- build.prev.inc(age.group="children",j.num.list[((run.number-1)*12+1):((run.number*12))])
        adults.prev.inc <- build.prev.inc(age.group="adults",j.num.list[((run.number-1)*12+1):((run.number*12))])
 
for (year in 1970:2015) {

     cat(year,"\n")

     ## load prevalence maps
    eval(parse(text=paste("prev.",year," <- raster(\"FILEPATH",year,".",(run.number-1)%%100+1,".PR.tif\")",sep="")))
    eval(parse(text=paste("prev.dec.",year," <- raster(\"FILEPATH",max(year,1971),".",(run.number-1)%%100+1,".5yr.decline.PR.tif\")",sep="")))

## compile burden estimates
       
        eval(parse(text=paste("prevv <- getValues(prev.",year,")",sep="")))
        eval(parse(text=paste("declinev <- getValues(prev.dec.",year,")",sep=""))
)

        eval(parse(text=paste("na.pixels <- which(prevv == -9999 | is.na(prevv) | gmapv == -9999 | seasonalityv == -9999)",sep="")))
        valid.pixels <- which(!(1:length(gmapv) %in% na.pixels))
        eval(parse(text=paste("inc.map.",year," <- gmap",sep="")))
        eval(parse(text=paste("inc.map.",year,"v <- gmapv",sep="")))
        eval(parse(text=paste("inc.map.",year,"v[valid.pixels] <- 0",sep="")))
        eval(parse(text=paste("inc.map.",year,"v[na.pixels] <- 0",sep="")))

        eval(parse(text=paste("inc.map.",year,"v.infants <- inc.map.",year,"v",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.children <- inc.map.",year,"v",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.adults <- inc.map.",year,"v",sep="")))
        
        eval(parse(text=paste("inc.map.",year,"v.infants[valid.pixels] <- infants.prev.inc(prevv[valid.pixels],seasonality=seasonalityv[valid.pixels],decline=declinev[valid.pixels])",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.children[valid.pixels] <- children.prev.inc(prevv[valid.pixels],seasonality=seasonalityv[valid.pixels],decline=declinev[valid.pixels])",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.adults[valid.pixels] <- adults.prev.inc(prevv[valid.pixels],seasonality=seasonalityv[valid.pixels],decline=declinev[valid.pixels])",sep="")))

        eval(parse(text=paste("inc.map.",year,"v.infants[na.pixels] <- 0",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.children[na.pixels] <- 0",sep="")))
        eval(parse(text=paste("inc.map.",year,"v.adults[na.pixels] <- 0",sep="")))

        eval(parse(text=paste("values(inc.map.",year,") <- inc.map.",year,"v.infants",sep="")))
        eval(parse(text=paste("writeRaster(inc.map.",year,",\"FILEPATH",year,".",run.number,".inc.rate.infants.full.tif\",overwrite=T,format=\"GTiff\")",sep="")))

        eval(parse(text=paste("values(inc.map.",year,") <- inc.map.",year,"v.children",sep="")))
        eval(parse(text=paste("writeRaster(inc.map.",year,",\"FILEPATH",year,".",run.number,".inc.rate.children.full.tif\",overwrite=T,format=\"GTiff\")",sep="")))

        eval(parse(text=paste("values(inc.map.",year,") <- inc.map.",year,"v.adults",sep="")))
        eval(parse(text=paste("writeRaster(inc.map.",year,",\"FILEPATH",year,".",run.number,".inc.rate.adults.full.tif\",overwrite=T,format=\"GTiff\")",sep="")))

    }
}

