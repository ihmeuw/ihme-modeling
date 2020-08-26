// Implements the two sex model described in "Bayesian reconstruction of two-sex
// populations by age: estimating sex ratios at birth and sex ratios of mortality"
// by Wheldon et al. I use the original form from the single sex model since they
// are identical. Besides also estimating the male population, this model also
// differs in that it estimates a yearly sex ratio at birth (srb) instead of fixing
// this parameter at 1.05 in all years.
//
// Level 1: ((n*{a,t,l} - n{a,t,l}) / n{a,t,l}) ~ Normal(0, sigma2_n)
// Level 2: n_{a,t,l} = CCMPP(n{.,t-5,.}, f{.,t-5,.}, s{.,t-5,.}, g{.,t-5,.})
// Level 3: ((n{a,t0,l} - n*{a,t0,l}) / n*{a,t0,l}) ~ Normal(0, sigma2_n0)
//          logit_s{a,t,l} ~ Normal(logit_s*{a,t,l}, sigma2_s)
//          (g{a,t,l} - g*{a,t,l}) ~ AR1:AR1(sigma2_g, rho_g_a, rho_g_t)
//          log_f{a,t} ~ Normal(log_f*{a,t}, sigma2_f)  (for fertility ages)
//          log_srb(t) ~ Normal(log_f*{t}, sigma2_srb)
// Level 4: log(sigma2_v) ~ Normal(mu_v, sig2_v)  (v -> {n,s,g,f,srb})
//
// Note: Expands on model version 3 by modeling migration as an AR1 distribution
// over time and age.

#include <iostream>
#include <TMB.hpp>
#include "pop_model_functions.cpp"
using namespace density;

// Objective function
template<class Type>
Type objective_function<Type>::operator() () {

  // Define data and inputs
  // prior means for level 3
  std::cout << "Read in data:"<< std::endl;
  DATA_ARRAY(input_n_female);
  DATA_ARRAY(input_n_male);
  DATA_IARRAY(input_n_ages_female);
  DATA_IARRAY(input_n_ages_male);
  DATA_ARRAY(input_n0_female);
  DATA_ARRAY(input_n0_male);
  DATA_ARRAY(input_logit_s_female);
  DATA_ARRAY(input_logit_s_male);
  DATA_ARRAY(input_g_female);
  DATA_ARRAY(input_g_male);
  DATA_ARRAY(input_log_f);
  DATA_ARRAY(input_log_srb);

  DATA_INTEGER(age_int);
  DATA_INTEGER(row_fert_start);
  DATA_INTEGER(row_fert_end);
  DATA_IVECTOR(col_census_year);

  // hyperparameters for level 4
  DATA_SCALAR(mu_s);
  DATA_SCALAR(sig_s);
  DATA_SCALAR(mu_g);
  DATA_SCALAR(sig_g);
  DATA_SCALAR(mu_f);
  DATA_SCALAR(sig_f);
  DATA_SCALAR(mu_srb);
  DATA_SCALAR(sig_srb);

  // hyperparameters for migration correlation
  DATA_SCALAR(rho_g_t_mu);
  DATA_SCALAR(rho_g_t_sig);
  DATA_SCALAR(rho_g_a_mu);
  DATA_SCALAR(rho_g_a_sig);

  // Define parameters
  // census likelihood parameters
  std::cout << "Read in parameters:"<< std::endl;

  // baseline population parameters
  PARAMETER_ARRAY(log_n0_female);
  array<Type> n0_female = exp_array(log_n0_female);
  PARAMETER_ARRAY(log_n0_male);
  array<Type> n0_male = exp_array(log_n0_male);

  // survival probability parameters
  PARAMETER_ARRAY(logit_s_female);
  array<Type> s_female = invlogit_array(logit_s_female);
  PARAMETER_ARRAY(logit_s_male);
  array<Type> s_male = invlogit_array(logit_s_male);
  PARAMETER(log_sigma_s);
  Type sigma_s = exp(log_sigma_s);

  // net migration proportion parameters
  PARAMETER_ARRAY(g_female);
  PARAMETER_ARRAY(g_male);
  PARAMETER(log_sigma_g);
  Type sigma_g = exp(log_sigma_g);
  PARAMETER(logit_rho_g_t);
  Type rho_g_t = invlogit(logit_rho_g_t);
  PARAMETER(logit_rho_g_a);
  Type rho_g_a = invlogit(logit_rho_g_a);

  // fertility rate parameters
  PARAMETER_ARRAY(log_f);
  array<Type> f = exp_array(log_f);
  PARAMETER(log_sigma_f);
  Type sigma_f = exp(log_sigma_f);

  // srb parameters
  PARAMETER_ARRAY(log_srb);
  array<Type> srb = exp_array(log_srb);
  PARAMETER(log_sigma_srb);
  Type sigma_srb = exp(log_sigma_srb);

  // Get number of years and ages
  int n_periods = g_female.cols();
  int n_ages = log_n0_female.rows();

  //**** Level 2 CCMPP.
  std::cout << "CCMP:"<< std::endl;
  // project initial population forward
  array<Type> pop_female = ccmpp(n0_female, n0_male, s_female, s_male,
                                 g_female, g_male, f, srb,
                                 age_int, row_fert_start, row_fert_end, true);
  array<Type> pop_male = ccmpp(n0_female, n0_male, s_female, s_male,
                               g_female, g_male, f, srb,
                               age_int, row_fert_start, row_fert_end, false);

  // interpolation to handle irregular census intervals when estimating using age interval of 5
  std::cout << "Interpolation if needed:"<< std::endl;
  array<Type> full_pop_female = arc_interpolation(pop_female, n_ages, n_periods, age_int);
  array<Type> full_pop_male = arc_interpolation(pop_male, n_ages, n_periods, age_int);

  // aggregate projected population to census age groups. Use -1 to signify a missing value
  std::cout << "Aggregate to census age groups:"<< std::endl;
  array<Type> agg_pop_female = aggregate_age_groups(full_pop_female, input_n_ages_female);
  array<Type> agg_pop_male = aggregate_age_groups(full_pop_male, input_n_ages_male);

  //**** Levels 4, 3 and 1

  Type nll = 0;
  max_parallel_regions = omp_get_max_threads();
  printf("This is thread %d", max_parallel_regions);

  //** NLL contributions from Level 4.
  std::cout << "Level 4 likelihood:"<< std::endl;

  // variance parameters
  PARALLEL_REGION nll -= dnorm(log_sigma_f, mu_f, sig_f, true);
  PARALLEL_REGION nll -= dnorm(log_sigma_s, mu_s, sig_s, true);
  PARALLEL_REGION nll -= dnorm(log_sigma_g, mu_g, sig_g, true);
  PARALLEL_REGION nll -= dnorm(log_sigma_srb, mu_srb, sig_srb, true);

  // correlation parameters
  PARALLEL_REGION nll -= dnorm(logit_rho_g_t, rho_g_t_mu, rho_g_t_sig, true);
  PARALLEL_REGION nll -= dnorm(logit_rho_g_a, rho_g_a_mu, rho_g_a_sig, true);

  //** NLL contribution from Level 3
  std::cout << "Level 3 likelihood:" << std::endl;

  // baseline population
  ADREPORT(log_n0_female);
  ADREPORT(log_n0_male);
  // separate out population counts
  array<Type> female_baseline_counts = input_n0_female.col(0);
  array<Type> male_baseline_counts = input_n0_male.col(0);
  // separate out scaled standard deviations
  vector<Type> female_baseline_sd_scaled = input_n0_female.col(1);
  vector<Type> male_baseline_sd_scaled = input_n0_male.col(1);
  // calculate percent difference between estimated and input counts
  array<Type> pct_diff_n0_female = (n0_female - female_baseline_counts) / female_baseline_counts;
  array<Type> pct_diff_n0_male = (n0_male - male_baseline_counts) / male_baseline_counts;
  // calculate likelihood of each point
  PARALLEL_REGION nll -= dnorm(vector<Type>(pct_diff_n0_female), Type(0), female_baseline_sd_scaled, true).sum();
  PARALLEL_REGION nll -= dnorm(vector<Type>(pct_diff_n0_male), Type(0), male_baseline_sd_scaled, true).sum();

  // survival
  ADREPORT(logit_s_female);
  ADREPORT(logit_s_male);
  ADREPORT(log_sigma_s);
  array<Type> abs_diff_logit_s_female = logit_s_female - input_logit_s_female;
  array<Type> abs_diff_logit_s_male = logit_s_male - input_logit_s_male;
  PARALLEL_REGION nll -= dnorm(vector<Type>(abs_diff_logit_s_female), Type(0), sigma_s, true).sum();
  PARALLEL_REGION nll -= dnorm(vector<Type>(abs_diff_logit_s_male), Type(0), sigma_s, true).sum();

  // migration
  ADREPORT(g_female);
  ADREPORT(g_male);
  ADREPORT(log_sigma_g);
  ADREPORT(logit_rho_g_t);
  ADREPORT(logit_rho_g_a);
  array<Type> abs_diff_g_female = g_female - input_g_female;
  array<Type> abs_diff_g_male = g_male - input_g_male;
  PARALLEL_REGION nll += SCALE(SEPARABLE(AR1(rho_g_t), AR1(rho_g_a)), sigma_g)(abs_diff_g_female);
  PARALLEL_REGION nll += SCALE(SEPARABLE(AR1(rho_g_t), AR1(rho_g_a)), sigma_g)(abs_diff_g_male);

  // fertility
  ADREPORT(log_f);
  ADREPORT(log_sigma_f);
  array<Type> abs_diff_log_f = log_f - input_log_f;
  PARALLEL_REGION nll -= dnorm(vector<Type>(abs_diff_log_f), Type(0), sigma_f, true).sum();

  // srb
  ADREPORT(log_srb);
  ADREPORT(log_sigma_srb);
  array<Type> abs_diff_log_srb = log_srb - input_log_srb;
  PARALLEL_REGION nll -= dnorm(vector<Type>(abs_diff_log_srb), Type(0), sigma_srb, true).sum();

  //** NLL contribution from Level 1
  std::cout << "Level 1 likelihood:"<< std::endl;

  // separate out population counts
  array<Type> female_counts = input_n_female.col(0);
  array<Type> male_counts = input_n_male.col(0);
  // separate out scaled standard deviations
  vector<Type> female_sd_scaled = input_n_female.col(1);
  vector<Type> male_sd_scaled = input_n_male.col(1);
  // calculate percent difference between projected and data counts
  array<Type> pct_diff_n_female = (agg_pop_female - female_counts) / female_counts;
  array<Type> pct_diff_n_male = (agg_pop_male - male_counts) / male_counts;
  // calculate likelihood of each point
  PARALLEL_REGION nll -= dnorm(vector<Type>(pct_diff_n_female), Type(0), female_sd_scaled, true).sum();
  PARALLEL_REGION nll -= dnorm(vector<Type>(pct_diff_n_male), Type(0), male_sd_scaled, true).sum();

  return nll;
}
