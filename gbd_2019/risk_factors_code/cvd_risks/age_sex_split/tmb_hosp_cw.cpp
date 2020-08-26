#include <TMB.hpp>
//#include <kronecker.hpp> //for knocker product
// // Function for detecting NAs
// template<class Type>
// bool isNA(Type x){
//   return R_IsNA(asDouble(x));
// }

// lognormal density function
template<class Type>
Type dlognorm(Type x, Type meanlog, Type sdlog, int give_log=0){
  //return 1/(sqrt(2*M_PI)*sd)*exp(-.5*pow((x-mean)/sd,2));
  Type logres = dnorm( log(x), meanlog, sdlog, true) - log(x);
  if(give_log) return logres; else return exp(logres);
}

// Space time
template<class Type>
Type objective_function<Type>::operator() ()
{
  //using namespace density; // for MVNORM function
  //using namespace tmbutils; // for kronecker fun
  ////////////////////////// Data ////////////////////////////////////
  
  DATA_IVECTOR( option_vec ); // first position is whether to logit transform or not
  DATA_INTEGER( n_dims ); // number of dimensions to delta smooth
  DATA_IVECTOR( n_xvals ); // number of unique xvals for each dimension
  DATA_INTEGER( n_k ); // number if fixed effects
  DATA_INTEGER( n_l ); // number of levels of random effects
  DATA_INTEGER( n_s ); // total number of random effects
  DATA_IVECTOR( l_s ); // vector giving the level for each random effect
  //DATA_INTEGER( n_zero_prob ); // number of zero probs to estimate. either 1 or number of sites
  //DATA_VECTOR( xvals ); // unique ages
  //data point
  DATA_VECTOR( y_i ); // response variable 1
  DATA_VECTOR( se_i ); // standard error for each observation
  DATA_VECTOR( weight_i); // weight for each observation, usually coming from population structure
  DATA_IVECTOR( ko_i ); // binary for holding out a data point
  
  DATA_MATRIX( X_ik ); // model matrix of fixed effects
  DATA_MATRIX( Z_is ); // model matrix for random effects
  DATA_MATRIX( A_ia ); // model matrix for delta smoothing
  
  //DATA_IVECTOR( age_i ); // individual age
  DATA_VECTOR( log_tau_data ); // rate of change across age groups

  // prediction inputs
  // DATA_INTEGER( n_preds );
  // DATA_MATRIX( X_ik_pred );
  // DATA_MATRIX( Z_is_pred );
  // DATA_IVECTOR( age_i_pred );
  
  
  
  ////////////////////////////////////////// Pars ////////////////////////////////////////
  
  PARAMETER( alpha ); // global intercept
  PARAMETER_VECTOR( beta_k ); // vector of parameters for fixed effects
  //PARAMETER( init_omega ); // initialize omega
  PARAMETER_VECTOR( omega_a ); // differences between age groups
  PARAMETER( log_sigma_e ); // SD of data
  PARAMETER_VECTOR( log_tau_param ); // smoothing SD
  PARAMETER_VECTOR( epsilon_s ); // random effects for all levels
  PARAMETER_VECTOR( log_sigma_l ); // vector of sigmas for each level of random effects
  

  ///////////////////////////////////// Intermediate variables /////////////////////////////////////////
  
  // joint negative log likelihood components
  vector<Type> jnll_comp(3); // components of the joint negative log likelihood
  jnll_comp.setZero();
  vector<Type> jnll_i(y_i.size()); // jnll for individual observations
  jnll_i.setZero();
  Type jnll_ko = 0; // jnll for KO observations
  
  //Type k = beta_k.size(); // number of fixed effects
  //vector<Type> mu_a( n_ages ); // mean for each age group
  vector<Type> linpred_i( y_i.size() );
  Type sigma_e = exp( log_sigma_e ); //exponentiate SD

  
  //////////////////////// likelihood of age effects, difference smoothing (random) ///////////////////////////////////////

  //jnll_comp(2) -= dnorm(omega_a(0), init_omega, exp( log_tau_param * log_tau_data ), true);
  //mu_a(0) = alpha; // initialize here
  int pos = 1; // keep track of dimension
  for(int d=0; d<n_dims; d++){
    for(int a=pos; a<n_xvals(d) + pos - 2; a++){
      //mu_a(a) = mu_a(a-1) + omega_a(a-1) * (age_a(a)-age_a(a-1));
      //if(a < (n_xvals(d) - 1)){
      jnll_comp(2) -= dnorm(omega_a(a), omega_a(a-1), exp( log_tau_param(d) * log_tau_data(d) ), true);
      //}
    }
    pos = pos + n_xvals(d) - 1;
  }

  
  //////////////// likelihood of random effects (random intercepts) ///////////////////////////////
  
  for(int s=0; s<n_s; s++){
    jnll_comp(1) -= dnorm(epsilon_s(s), Type(0.0), exp( log_sigma_l(l_s(s) - 1) ), true);
    //jnll_comp(1) -= dnorm(epsilon_s(s), Type(0.0), exp( log_sigma_l ), true);
  }
  
  //////////////////////////// likelihood of data ////////////////////////////////////
  
  //matrix<Type> fixeffs = X_ik.array * beta_k.array();
  //vector<Type> fixef = X_ik * beta_k; // calculate fixed effects
  for(int i=0; i<y_i.size(); i++){
    // calculate fixed effects... not sure what a better generalizable way would be
    Type fixef=Type(0);
    for(int b=0; b<n_k; b++){
      fixef += X_ik(i, b) * beta_k(b);
    }
    
    // calculate random effects
    Type ranef=Type(0);
    for(int s=0; s<n_s; s++){
      ranef += Z_is(i, s) * epsilon_s(s);
    }
    
    // calculate xvals
    Type mu=Type(0);
    for(int a=0; a<(n_xvals.sum() - n_dims); a++){ // subtract off number of dimensions cuz initial value for each dimension is 0
      mu += A_ia(i, a) * omega_a(a);
    }
    
    //REPORT(ranef);
    // calculate linear predictor
    linpred_i(i) =  alpha + fixef + ranef + mu;
    
    // likelihood if logit transforming
    // if(option_vec(0)==1){
    //   jnll_i(i) = ( 1 / weight_i(i) ) * dnorm( log( y_i(i) / (1-y_i(i)) ), linpred_i(i), pow( pow(sigma_e, 2) + pow(se_i(i), 2), .5 ), true); // multiplying density by weight, also adding data variance to estimated variance
    // }
    
    // likelihood if no transform
    //if(option_vec(0)==0){
    jnll_i(i) = ( weight_i(i) ) * dnorm( y_i(i), linpred_i(i), pow( pow(sigma_e, 2) + pow(se_i(i), 2), .5 ), true); // logit transforming observations
    //}
    
    // calculate jnll for in-sample and out-of-sample data
    if(ko_i(i)==0){
      jnll_comp(0) -= jnll_i(i);
    }
    if(ko_i(i)==1){
      jnll_ko -= jnll_i(i);
    }
  }
  
  
  ///////////////////// Generated quantities /////////////////////////////

  // predict for ages
  // vector<Type> age_pred( n_ages );
  // for(int a=0; a<n_ages; a++){
  //   //age_pred(a) = beta_k(0) + mu_a(a);
  //   age_pred(a) = mu_a(a);
  //   if(option_vec(0)==2){
  //     age_pred(a) = exp( age_pred(a) ); // exp for lognormal model
  //   }
  //   // inverse logit transform
  //   if(option_vec(0)==1){
  //     age_pred(a) = 1 / (1 + exp( -age_pred(a) ));
  //   }
  // }
  
  // predict for prediction data -- doing this outside of TMB now
  // vector<Type> pred_i( n_preds );
  // for(int i=0; i<n_preds; i++){
  //   
  //   Type fixef=Type(0);
  //   for(int b=0; b<n_k; b++){
  //     fixef += X_ik_pred(i, b) * beta_k(b);
  //   }
  //   
  //   // calculate random effects
  //   Type ranef=Type(0);
  //   for(int s=0; s<n_s; s++){
  //     ranef += Z_is_pred(i, s) * epsilon_s(s);
  //   }
  //    // calculate prediction
  //   pred_i(i) = fixef + ranef + mu_a(age_i_pred(i)-1);
  //   if(option_vec(0)==2){
  //     pred_i(i) = exp( pred_i(i) ); // exp for lognormal model
  //   }
  //   // inverse logit transform
  //   if(option_vec(0)==1){
  //     pred_i(i) = 1 / (1 + exp( -pred_i(i) ));
  //   }
  // }
  
  
  ///////////////////// REPORTING ///////////////////////////////////////
  
  // sum up jnll components
  Type jnll = jnll_comp.sum();
  REPORT( jnll );
  REPORT( jnll_comp );
  ADREPORT( jnll_comp );
  ADREPORT( alpha );
  ADREPORT( log_sigma_e );
  ADREPORT( beta_k );
  ADREPORT( epsilon_s );
  ADREPORT( omega_a );
  ADREPORT( log_sigma_l );
  // ADREPORT( mu_a );
  // ADREPORT( age_pred );
  // ADREPORT( pred_i );
  ADREPORT( jnll_ko );
  //ADREPORT( spatial_pred3 );
  
  
  return jnll;
}
