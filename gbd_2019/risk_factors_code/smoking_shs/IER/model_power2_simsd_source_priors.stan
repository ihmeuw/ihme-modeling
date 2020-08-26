//----HEADER-------------------------------------------------------------------------------------------------------------
// Project: RF: air_pm
// Purpose: Adding informative priors on the model
//***********************************************************************************************************************
  
// Create the stan model object 
// stan model
// specify the input data
data {
  // // observations
  int<lower=1> N;
  
  // log(RR) and sd(log(RR))
  vector[N] log_rr;
  vector[N] log_rr_sd;
  
  // sources
  int<lower=1> S;
  int<lower=1,upper=S> source[N];
  
   // observed exposure level
  vector[N] exposure;
  
  // counterfactual exposure level
  vector[N] cf_exposure;
  
  // test exposure values
  int<lower=1> T;             // number of test points
  vector[T] test_exposure;    // values at which to predict RR
  
  // theoretical minimum
  real tmrel;
}

// specify model parameters
parameters {
  // we're assuming that these values have to be positive
  // lower=1e-9 forces positive values in case we use a prior distribution that allows for negatives
  real<lower=1e-9> alpha;
  real<lower=1e-9> beta;
  real<lower=1e-9> gamma;
  real<lower=1e-9> delta[S];
  }

// specify priors and data likelihood function
model {
// make a temporary vector to store the predicted log(RR)
  vector[N] pred_log_rr;

// non-informative prior on parameters
  alpha ~ gamma(1.0, 0.01);   // alpha can be big, so give it small precision. gamma is parameterized as gamma(shape,inverse-scale)
  beta ~ gamma(1.0, 1);     // whereas beta and gamma are small, so tighten them up closer to 1.0
  gamma ~ gamma(1.0, 1);
  delta ~ gamma(1.0, 0.01);


// make predictions for log(RR) using alpha/beta/gamma
  for (n in 1:N) {
      pred_log_rr[n] = log(
      (1 + alpha * (1 - exp(-1 * beta * pow((exposure[n] - tmrel), gamma)))) / 
      (1 + alpha * (1 - exp(-1 * beta * pow((cf_exposure[n] - tmrel), gamma))))
      );

  
  }

// setup the data likelihood
// our data is distributed with sd of sd(log(RR)) and mean being our predicted log(RR)
  for (n in 1:N) {
    log_rr[n] ~ normal(pred_log_rr[n], sqrt(pow(log_rr_sd[n],2) + pow(delta[source[n]], 2)));
  }
}

// generate values of RR at the test exposure levels
generated quantities {
vector[T] predicted_RR;

// generate predictions
for (t in 1:T) {
    predicted_RR[t] = (test_exposure[t] > tmrel ? 
    1 + alpha * (1 - exp(-1 * beta * pow((test_exposure[t] - tmrel), gamma))) :
    1.0);
  }
}



