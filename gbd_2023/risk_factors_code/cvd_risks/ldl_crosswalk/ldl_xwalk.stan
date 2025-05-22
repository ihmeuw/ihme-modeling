data{
  int N; //number of ldl obs
  vector<lower=0, upper=10>[N] ldl; //input ldl data
  vector<lower=0, upper=20>[N] chl; //input chl data
  vector<lower=0, upper=20>[N] hdl; //input chl data
  vector<lower=0, upper=20>[N] tgl; //input chl data

  //values to be used for predictions (no _ldl data)
  int pred_N;
  vector<lower=0, upper=20>[pred_N] pred_chl; //input chl data
  vector<lower=0, upper=20>[pred_N] pred_hdl; //input hdl data
  vector<lower=0, upper=20>[pred_N] pred_tgl; //input tgl data

  int n_lipids; //number of possible combinations of chl, hdl, and tgl
  int lipids[N]; //an index variable signifying the number and type of other lipid variables available for a given ldl data point
  int pred_lipids[pred_N]; //index variable for predictions



}
parameters{
  real alpha[n_lipids]; //constant to added for each
  real beta[3];
  real<lower=0> sigma;
}
transformed parameters{
  vector[N] ldl_est;

  for(i in 1:N){
    ldl_est[i]=alpha[lipids[i]]+chl[i]*beta[1]-(hdl[i]*beta[2]+tgl[i]*beta[3]/2.2); // replicate freidewald's equation
  }
}
model{

  ldl ~ normal(ldl_est, sigma);
    sigma ~ cauchy(0, 2);

}
generated quantities{
  vector[N] ldl_hat;
  vector[pred_N] pred_ldl;
  for(i in 1:N){
    ldl_hat[i]=normal_rng(ldl_est[i], sigma);
  }

  for(i in 1:pred_N){
    pred_ldl[i]=normal_rng(alpha[pred_lipids[i]]+pred_chl[i]*beta[1]-(pred_hdl[i]*beta[2]+pred_tgl[i]*beta[3]/2.2), sigma);
  }
}
