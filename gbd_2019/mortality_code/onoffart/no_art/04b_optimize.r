## NAME
## February 2014
## Optimization of mortality and CD4 progression parameters in UNAIDS comparmental model.

library(stats)
library(utils)

if (Sys.info()[1] == "Linux") {
  root <- "ADDRESS"
  user <- Sys.getenv("USER")
  code_dir <- paste0("FILEPATH")
  print(commandArgs())
  age <- commandArgs()[3]
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
  age <- "45_100"
}

comp_dir <- paste0(root,"FILEPATH")

# Source optimization functions
source(paste0(code_dir,"/04a_define_comp_model_functions.r"))

potential_ages <- c("15_25","25_35","35_45","45_100")
potential_seeds <- c(1337,5336,600,1990)

selected_seed <- potential_seeds[potential_ages==age]

set.seed(selected_seed)


##################################################################
## SET UP COMPARTMENTS AND INITIAL PARAMETERS
##################################################################
# Number of iterations in the model
num.steps <- 121
pred.steps <- 321

# Time interval of each step (years)
dt <- 0.1

# CD4 bins are vectors whose value at index t takes represents the number of living people with a CD4 count in the bin's range at time t.
c1 <- rep(0,num.steps)
c2 <- rep(0,num.steps)
c3 <- rep(0,num.steps)
c4 <- rep(0,num.steps)
c5 <- rep(0,num.steps)
c6 <- rep(0,num.steps)
c7 <- rep(0,num.steps)

# CD4 bins
cd4.upper <- rep(0,7)
cd4.lower <- rep(0,7)
cd4.upper[1] <- 750
cd4.lower[1] <- 500
cd4.upper[2] <- 500
cd4.lower[2] <- 350
cd4.upper[3] <- 350
cd4.lower[3] <- 250
cd4.upper[4] <- 250
cd4.lower[4] <- 200
cd4.upper[5] <- 200
cd4.lower[5] <- 100
cd4.upper[6] <- 100
cd4.lower[6] <- 50
cd4.upper[7] <- 50
cd4.lower[7] <- 0

#Width of CD4 bins and midpoint CD4
bin.width <- rep(0,7)
midpoint.cd4 <- rep(0,7)

for(i in 1:length(bin.width)) {
    bin.width[i] <- cd4.upper[i]-cd4.lower[i]
    midpoint.cd4[i] <- (cd4.upper[i]+cd4.lower[i])/2
}

# Death bins are vectors whose value at index t takes represents the cumulative number of people who have died by time t who have died with a CD4 count within the range of the corresponding CD4 bin.

d1 <- rep(0,num.steps)
d2 <- rep(0,num.steps)
d3 <- rep(0,num.steps)
d4 <- rep(0,num.steps)
d5 <- rep(0,num.steps)
d6 <- rep(0,num.steps)
d7 <- rep(0,num.steps)

# Seed infections into CD4 categories based off of 1000 infections.  This uses UNAIDS defaults.
s1 <- c(643, 607, 585, 552)
s2 <- c(357, 393, 415, 448)
# New: Haidong prior (forget why this is here)
# s1 <- c(925, 925, 925, 925)
# s2 <- c(75, 75, 75, 75)
s.tot <-s1 + s2

# f.1 vectors - one for each age group.  F.1 is the rate at which infected individuals progress through the first CD4 category (CD4 > 500).  This looks like it is based off of some paper by Nikos Pantazis
f.1 <- rep(0, 4)
f.1[1] <- 0.117
f.1[2] <- 0.15
f.1[3] <- 0.183
f.1[4] <- 0.213

# UNAIDS optimized mortality and progression parameters - these will be the starting points of our model.  Vectors of length 4 because there are 4 age groups.
# Mortality: m = a*exp(b*i)
# Slope of CD4 Decline: r = c + d*i

a.unaids <- c(0.002176, 0.00131211802888206, 0.001866, 0.001980)
b.unaids <- c(0.832, 0.99, 0.986, 0.926)
c.unaids <- c(41.432488, 17.5186200122399, 43.631226, 70.392792)
d.unaids <- c(-4, 9.24044329247385, 4.8, 5)

# Bring in regression draw output
filepath <- age
df <- read.csv(paste0(comp_dir,"/input/", filepath, ".csv", sep = ""))


# Convert age groups to categories recognized by the model
if(age == "15_25") {
  group <- 1
} else if (age == "25_35") {
  group <- 2
} else if(age == "35_45") {
  group <- 3
} else if(age == "45_100") {
  group <- 4
}

# Subset data frame to just survival probabilities
data <- df[, grep("surv_prob", colnames(df))]

# Set up optimization vectors to store parameters
a.opt     <- rep(NaN, nrow(data))
b.opt     <- rep(NaN, nrow(data))
c.opt     <- rep(NaN, nrow(data))
d.opt     <- rep(NaN, nrow(data))
conv      <- rep(NaN, nrow(data))

scalar_dist <- seq(0.8, 1.2, by=0.01)
scalars <- rep(NaN, nrow(data))

for(i in 1:length(scalars)) {
  scalars[i] <- sample(scalar_dist, 1)
}
errors <- c()
preds <- matrix(NaN, nrow(data), floor(pred.steps/10))
##################################################################
## OPTIMIZE FOR EACH DRAW OF REGRESSION OUTPUT
##################################################################
for(i in 1:nrow(data)) {

  # Sample from scalars
  scalar <- scalars[i]

#   print(i)
#   print(scalar)

  # Initial UNAIDS parameters
  par.unaids <- c(a.unaids[group], b.unaids[group], c.unaids[group], d.unaids[group])

  #Subset data to just the survival probabilities
  x <- as.numeric(as.vector(data[i,]))

  # Add 1 for initial survival
  draw <- append(1, x)

  # Optimize
  opt <- try(optim(par.unaids, cmp.calculate.loss, draw=draw, age.group=group, scalar=scalar, method=c("BFGS")))

#   print(opt)

  if (!inherits(opt, "try-error")) {
    # Store optimization parameters
    #print(opt$convergence)

    a.opt[i] <- opt$par[1]
    b.opt[i] <- opt$par[2]
    c.opt[i] <- opt$par[3]
    d.opt[i] <- opt$par[4]
    conv[i] <- opt$convergence

    # Return survival parameters
    preds[i,] <- cmp.pred.surv(opt$par, draw, group, scalar)

  } else {          # If optimization fails, return NAs and continue
    a.opt[i] <- NaN
    b.opt[i] <- NaN
    c.opt[i] <- NaN
    d.opt[i] <- NaN
    conv[i] <- 2
  }

}

##################################################################
## SAVE OPTIMAL PARAMETERS
##################################################################
cats <- seq(1,7,1)
mort <- matrix(NaN, nrow(data), length(cats))
prog <- matrix(NaN, nrow(data), length(cats)-1)
convergence <- matrix(conv, nrow(data), 1) # 0 = converges, 1 = no convergence, 2 = optimization failure

for(j in 1:length(cats)) {
  for(i in 1:nrow(data)) {


    mort[i,j] <- mort.risk(a.opt[i], b.opt[i], cats[j])

    if(j<length(cats)) {

      scalar <- scalars[i]

      prog[i,j] <- prog.risk(c.opt[i], d.opt[i], cats[j], group, scalar)

    }

  }
}

# Column names so variables will be recognizable upon output
colnames(mort) <- c("mort1", "mort2", "mort3", "mort4", "mort5", "mort6", "mort7")
colnames(prog) <- c("prog1", "prog2", "prog3", "prog4", "prog5", "prog6")
colnames(convergence) <- c("no_converge")

# Combine convergence to mortality and progression parameters
mort <- cbind(mort, convergence)
prog <- cbind(prog, convergence)

## Save mortality and progression rate parameters as CSVs
output.path <- paste(age, "sample", sep = "_")

write.table(mort, file = paste0(comp_dir,"/output/", output.path, "_mortality", ".csv"), sep = ",", row.names=FALSE)
write.table(prog, file = paste0(comp_dir,"/output/", output.path, "_progression", ".csv"), sep = ",", row.names=FALSE)

## Save survival predictions as CSVs
write.table(preds, file = paste0(comp_dir,"/output/", output.path, "_survival_predictions", ".csv"), sep = ",", row.names=FALSE)

