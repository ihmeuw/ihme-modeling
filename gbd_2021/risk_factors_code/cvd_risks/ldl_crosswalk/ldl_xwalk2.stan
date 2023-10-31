data{
  int N; //number of ldl obs
  vector<lower=0, upper=10>[N] ldl; //input ldl data
  vector<lower=0, upper=20>[N] chl; //input chl data
  vector<lower=0, upper=20>[N] hdl; //input hdl data
  vector<lower=0, upper=20>[N] tgl; //input tgl data

  // standard errors of trainging data
  vector<lower=0, upper=10>[N] ldl_se; //input ldl se
  vector<lower=0, upper=20>[N] chl_se; //input chl se
  vector<lower=0, upper=20>[N] hdl_se; //input hdl se
  vector<lower=0, upper=20>[N] tgl_se; //input tgl se

  //values to be used for predictions (no _ldl data)
  int pred_N; // number of data points to predict for
  vector<lower=0, upper=20>[pred_N] pred_chl; //input chl data
  vector<lower=0, upper=20>[pred_N] pred_hdl; //input hdl data
  vector<lower=0, upper=20>[pred_N] pred_tgl; //input tgl data

   // standard errors of prediction data
  vector<lower=0, upper=20>[pred_N] pred_chl_se; //input chl se
  vector<lower=0, upper=20>[pred_N] pred_hdl_se; //input hdl se
  vector<lower=0, upper=20>[pred_N] pred_tgl_se; //input tgl se
  //

  int n_lipids; //number of possible combinations of chl, hdl, and tgl
  int lipids[N]; //an index variable signifying the number and type of other lipid variables available for a given ldl data point
  int pred_lipids[pred_N]; //index variable for predictions

  int K; //number of coeffs to est
  matrix[N,K] X;
  matrix[pred_N, K] pred_X;
}
parameters{
  real alpha[n_lipids]; //constant to added for each
  real lip_beta[3];
  vector[K] beta;
  real<lower=0> sigma;
  // hold draws of input data from data uncertainty
  // vector[N] ldl_est;
  // vector[N] chl_est;
  // vector[N] hdl_est;
  // vector[N] tgl_est;
}
transformed parameters{
  vector[N] linpred_i;

  for(i in 1:N){
    //linpred_i[i] = alpha[lipids[i]] + chl_est[i] * lip_beta[1] - (hdl_est[i] * lip_beta[2] + tgl_est[i] * lip_beta[3] / 2.2) + X[i] * beta; // replicate freidewald's equation
    linpred_i[i] = alpha[lipids[i]] + chl[i] * lip_beta[1] - (hdl[i] * lip_beta[2] + tgl[i] * lip_beta[3] / 2.2) + X[i] * beta; // replicate freidewald's equation

  }
}
model{
  // simulate standard errors from each lipid data input
  // for(i in 1:N){
  // ldl_est[i] ~ normal(ldl[i], ldl_se[i]);
  // chl_est[i] ~ normal(chl[i], chl_se[i]);
  // hdl_est[i] ~ normal(hdl[i], hdl_se[i]);
  // tgl_est[i] ~ normal(tgl[i], tgl_se[i]);
  //
  // // chl_est ~ normal(chl, chl_se);
  // // hdl_est ~ normal(hdl, hdl_se);
  // // tgl_est ~ normal(tgl, tgl_se);
  // }

  ldl ~ normal(linpred_i, sigma);
  sigma ~ cauchy(0, 2);

}
generated quantities{
  vector[N] ldl_hat; // re-estimate given ldl values
  vector[pred_N] pred_ldl; // predict missing ldl values

  // place holder for draw from s
  vector[pred_N] chl_pred_est;
  vector[pred_N] hdl_pred_est;
  vector[pred_N] tgl_pred_est;

  // hold draws

  for(i in 1:N){
    ldl_hat[i] = normal_rng(linpred_i[i], sigma);
  }

  // simulate standard errors from each lipid data input
  for(i in 1:pred_N){
    chl_pred_est[i] = normal_rng(pred_chl[i], pred_chl_se[i]);
    hdl_pred_est[i] = normal_rng(pred_hdl[i], pred_hdl_se[i]);
    tgl_pred_est[i] = normal_rng(pred_tgl[i], pred_tgl_se[i]);

    // predict
    pred_ldl[i] = normal_rng(alpha[pred_lipids[i]] + chl_pred_est[i] * lip_beta[1] - (hdl_pred_est[i] * lip_beta[2] + tgl_pred_est[i] * lip_beta[3]/2.2) + pred_X[i] * beta, sigma);
  }
}
