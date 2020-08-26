functions{
  // this function will set a zero if there are no effects to be estimated. Otherwise stan would break
  real calc_effects(int n_effects, vector betas, row_vector design_row){
    real effect;
    if(n_effects>0){
      effect = design_row * betas;
    }else{
      effect = 0;
    }
    return(effect);
  }
}
data{

  int<lower=0> option_vector[1]; // place to hold options. 
    // First option determines whether to log, logit, or identity transform
    // Second option determines which sampling statement syntax to use
  
  // counts
  int n_i; // number of observations
  int n_k; // number of betas to estimate
  int n_l; // number of levels of random effects
  int n_s[n_l]; // total number of random effects for each level
  //int n_pred; // number of rows to predict for

  // data
  vector[n_i] y_i; // observations
  vector<lower=0>[n_i] weight_i; // weights to be passed in to be multiplied by lp
  
  // model matricies
  matrix[n_i, n_k] X_ik; // model matrix for fixed effects
  matrix[n_i, sum(n_s)] Z_is; // model matrix for random effects

  // priors
  vector[n_l] random_priors_l; // priors on how tight random effects distributions should be, necessary with small groups
  
  // prediction model matricies
  // matrix[n_pred, n_k] X_ik_pred; // model matrix for fixed effects
  // matrix[n_pred, sum(n_s)] Z_is_pred; // model matrix for random effects
  // 
}
transformed data{
  int sum_n_s;
  sum_n_s = sum(n_s);
  //vector[n_i] trans_y_i;
  
  // transform response variable if necessary
  // if(option_vector[1]==2){
  //   trans_y_i = log(y_i);
  // }
  // if(option_vector[1]==1){
  //   trans_y_i = logit(y_i);
  // }
  // if(option_vector[1]==0){
  //   trans_y_i = y_i;
  // }
  // 
  // if(n_k==0){
  //   matrix[1, 1] X_ik_trans = 0;
  // 
  //   n_k = 1;
  // }
  // if(size(n_s)==0){
  //   matrixZ_is = 0;
  //   n_s = 0;
  // }
}
parameters{
  // vector[n_output_ages] omega; //sy: change between means
  // real<lower=0> sigma; //sy: global sd
  real<lower=0> sigma;
  real alpha;
  vector[n_k] beta_k;
  vector[sum_n_s] epsilon_s; // random effects
  vector<lower=0>[n_l] sigma_l; // sigma for random effects
  
  
}
transformed parameters{
  vector[n_i] linpred_i; //vector of linear predictors
  vector[n_i] fixefs;
  vector[n_i] ranefs;
  
  //sy: calculate linear predictor. Need to do it this way in case there are no fixed effects or no random effects
  for(i in 1:n_i){
    // calculate fixed effects and random effects
    fixefs[i] = calc_effects(n_k, beta_k, X_ik[i,]);
    ranefs[i] = calc_effects(sum_n_s, epsilon_s, Z_is[i,]);

    linpred_i[i] = alpha + fixefs[i] + ranefs[i];
  }

  //sy: calculate linear predictor
  //linpred_i = alpha + X_ik * beta_k + Z_is * epsilon_s;

}
model{
  int pos = 1; // initialize looping through random effects
  
  if(option_vector[1]==0){
    y_i ~ normal(linpred_i, sigma);
  }
  // second option, loop over each observation and use the target+= syntax (allows using weights)
  if(option_vector[1]==1){
    for(i in 1:n_i){
      target+=weight_i[i] * normal_lpdf(y_i[i] | linpred_i[i], sigma); // multiplying pdf by weight allows 'age integration'
    }
  }

  // random effects, loop through each level of random effects and estimate epsilons and random effect scales
  for(l in 1:n_l){
    segment(epsilon_s, pos, n_s[l]) ~ normal(0, sigma_l[l]);
    pos = pos + n_s[l];
    
    sigma_l[l] ~ cauchy(0, random_priors_l[l]); // prior on random effect scales can help convergence when there are few groups
  }
  
  
  sigma ~ cauchy(0, 2.5 * sd(y_i)); // multiplying prior by scale of y variable, as described here: https://cran.r-project.org/web/packages/rstanarm/vignettes/priors.html
}
generated quantities{
  // vector[n_pred] linpred_pred_i; //vector of linear predictors for predictions
  // vector[n_pred] fixefs_pred;
  // vector[n_pred] ranefs_pred;
  // 
  // //sy: calculate linear predictor. Need to do it this way in case there are no fixed effects or no random effects
  // for(i in 1:n_pred){
  //   
  //   // calculate fixed effects and random effects
  //   fixefs_pred[i] = calc_effects(n_k, beta_k, X_ik_pred[i,]);
  //   ranefs_pred[i] = calc_effects(sum_n_s, epsilon_s, Z_is_pred[i, ]);
  // 
  //   linpred_pred_i[i] = alpha + fixefs_pred[i] + ranefs_pred[i];
  // }
}


