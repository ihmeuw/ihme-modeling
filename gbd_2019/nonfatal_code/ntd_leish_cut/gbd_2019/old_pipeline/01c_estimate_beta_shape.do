// Program that allows the non-linear optimizer ("nl") to estimate the shape
// parameters of a beta distribution, given the upper and lower confidence bounds.
  capture program drop nlfaq
  program nlfaq  
    syntax varlist(min=1 max=1) [if], at(name)

    tempname alpha beta
    scalar `alpha' = `at'[1, 1]
    scalar `beta' = `at'[1, 2]

    tempvar yh
    generate double `yh' = ibeta(`alpha', `beta', prop_lo) - 0.025 + 1 in 1  // predicted value for lower bound (plus one), given alpha and beta
    replace `yh' = ibeta(`alpha', `beta', prop_hi) - 0.975 in 2  // predicted value for upper bound, given alpha and beta

    replace `varlist' = `yh'
  end
  