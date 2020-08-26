functions{
  
}
data{

  int<lower=0> option_vector[1]; // place to hold options. First option: 1 to use weights, 0 not to use weights
  int n_lit;
  vector<lower=0, upper=1>[n_lit] lit_prop;
  vector [n_lit] haqi; //this has no bounds because I'm changing the covariate
  vector<lower=0>[n_lit] lit_mid_age;
  vector<lower=0, upper=1>[n_lit] lit_severity;
  vector<lower=0>[n_lit] weight_i; // weights to be passed in, allows age integration

  //generated quantities data (for plotting)
  int n_temp_haqi;
  row_vector[n_temp_haqi] temp_haqi;
  int n_temp_mid_age;
  vector[n_temp_mid_age] temp_mid_age;
  
  // full prediction data
  int n_pred;
  vector<lower=0>[n_pred] pred_haqi;
  vector<lower=0>[n_pred] pred_mid_age;
  vector<lower=0, upper=1>[n_pred] pred_severity;

}
transformed data{

  vector[n_lit] trans_lit;

  trans_lit=logit(lit_prop);

}
parameters {

  real<lower=0> prop_sigma;
  real alpha;
  vector[3] betas; //alpha and beta on haqi and beta on age and beta on moderate+severe
}
transformed parameters{

  vector[n_lit] prop_est; // estimate of the lit proportion based on haqi

  vector[n_lit] linpred_i; //vector of estimates to hold
  vector[n_lit] prop_sigma_vector;

  // calculate linear predictor
  for(i in 1:n_lit){
    linpred_i[i]=alpha+betas[1]*haqi[i]+betas[2]*lit_mid_age[i]+betas[3]*lit_severity[i];
    prop_sigma_vector[i] = prop_sigma;
  }
  // store estimated proportions
  prop_est=inv_logit(linpred_i);

}
model {

  if(option_vector[1]==0){
      trans_lit ~ normal(linpred_i, prop_sigma);
  }
  if(option_vector[1]==1){
    for(i in 1:n_lit){
      target+=weight_i[i] * normal_lpdf(trans_lit[i] | linpred_i[i], prop_sigma); // multiplying pdf by weight allows 'age integration'
    }
  }

  prop_sigma ~ cauchy(0, 2.5);

}
generated quantities{
  matrix[n_temp_mid_age, n_temp_haqi] haqi_preds;
  vector[n_pred] full_pred;
  // predict for some haqi/age values for plotting
  for(i in 1:n_temp_mid_age){

    haqi_preds[i, ]=inv_logit(alpha+betas[1]*temp_haqi+betas[2]*temp_mid_age[i]);
  }
  
  // for predictions
    full_pred=inv_logit(alpha+betas[1]*pred_haqi+betas[2]*pred_mid_age);
}
