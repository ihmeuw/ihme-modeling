
#include <iostream>
#include <TMB.hpp>
#include "ccmp_model_functions.cpp"
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
  DATA_ARRAY(input_mx_female);
  DATA_ARRAY(input_mx_male);
  DATA_ARRAY(input_logit_c_female);
  DATA_ARRAY(input_logit_c_male);
  DATA_IARRAY(full_completeness_info);
  DATA_ARRAY(input_net_flow_pattern_female);
  DATA_ARRAY(input_net_flow_pattern_male);
  DATA_ARRAY(input_f);
  DATA_ARRAY(input_srb);

  DATA_INTEGER(terminal_age);
  DATA_INTEGER(age_int);
  DATA_INTEGER(row_fert_start);
  DATA_INTEGER(row_fert_end);

  // hyperparameters for level 4
  DATA_SCALAR(mu_c);
  DATA_SCALAR(sig_c);

  // hyperparameters for migration correlation
  DATA_SCALAR(rho_c_t_mu);
  DATA_SCALAR(rho_c_t_sig);
  DATA_SCALAR(rho_c_a_mu);
  DATA_SCALAR(rho_c_a_sig);

  DATA_SCALAR(c_upper);
  DATA_SCALAR(c_lower);

  // Define parameters
  // census likelihood parameters
  std::cout << "Read in parameters:"<< std::endl;

  // completeness parameters
  PARAMETER_ARRAY(logit_c_female);
  array<Type> c_female = invlogit_array(logit_c_female, c_upper, c_lower);
  PARAMETER_ARRAY(logit_c_male);
  array<Type> c_male = invlogit_array(logit_c_male, c_upper, c_lower);
  PARAMETER(log_sigma_c);
  Type sigma_c = exp(log_sigma_c);
  PARAMETER(logit_rho_c_t);
  Type rho_c_t = invlogit(logit_rho_c_t);
  PARAMETER(logit_rho_c_a);
  Type rho_c_a = invlogit(logit_rho_c_a);

  // migration parameter
  PARAMETER(total_net_flow);

  // Get number of years and ages
  int n_periods = input_mx_female.cols();
  int n_ages = input_mx_female.rows();

  // Calculate number of net migrants based on input in/out flow migration patterns and estimated total in/out flow
  array<Type> g_female(n_ages + 1, n_periods);
  array<Type> g_male(n_ages + 1, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = 0; a < n_ages + 1; a++) {
      g_female(a, y) = (total_net_flow * input_net_flow_pattern_female(a, 0));
      g_male(a, y) = (total_net_flow * input_net_flow_pattern_male(a, 0));
    }
  }

  // Reformat completeness parameters to be all years and all-ages no matter what parameters you are estimating
  array<Type> full_c_female(n_ages, n_periods);
  array<Type> full_c_male(n_ages, n_periods);
  for (int y = 0; y < n_periods; y++) {
    int c_index = full_completeness_info(y, 1);
    for (int a = 0; a < n_ages; a++) {
      full_c_female(a, y) = c_female(0, c_index);
      full_c_male(a, y) = c_male(0, c_index);
    }
  }

  // Adjust mx for estimated completeness
  array<Type> adjusted_mx_female(n_ages, n_periods);
  array<Type> adjusted_mx_male(n_ages, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = 0; a < n_ages; a++) {
      adjusted_mx_female(a, y) = input_mx_female(a, y) / full_c_female(a, y);
      adjusted_mx_male(a, y) = input_mx_male(a, y) / full_c_male(a, y);
    }
  }

  // calculate qx and ax assuming mx is constant in the age interval
  array<Type> ax_female(n_ages, n_periods);
  array<Type> ax_male(n_ages, n_periods);
  array<Type> qx_female(n_ages, n_periods);
  array<Type> qx_male(n_ages, n_periods);
  array<Type> lx_female(n_ages, n_periods);
  array<Type> lx_male(n_ages, n_periods);
  array<Type> dx_female(n_ages, n_periods);
  array<Type> dx_male(n_ages, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = 0; a < n_ages; a++) {
      // assign ax values
      ax_female(a, y) = 0.5;
      ax_male(a, y) = 0.5;
      // assign ax for under1 age group
      if (a == 0) {
        if (adjusted_mx_female(a, y) >= 0.107) {
          ax_female(a, y) = 0.350;
        } else {
          ax_female(a, y) = 0.053 + (2.800 * adjusted_mx_female(a, y));
        }
        if (adjusted_mx_male(a, y) >= 0.107) {
          ax_male(a, y) = 0.330;
        } else {
          ax_male(a, y) = 0.045 + (2.684 * adjusted_mx_male(a, y));
        }
      }

      // calculate qx given ax and mx
      int t = 1;
      if (a == n_ages - 1) {
        qx_female(a, y) = 1;
        qx_male(a, y) = 1;
        t = 125 - terminal_age; // same as GBD mortality terminal age group length
      } else {
        qx_female(a, y) = (t * adjusted_mx_female(a, y)) / (1 + ((t - ax_female(a, y)) * adjusted_mx_female(a, y)));
        qx_male(a, y) = (t * adjusted_mx_male(a, y)) / (1 + ((t - ax_male(a, y)) * adjusted_mx_male(a, y)));
      }

      // recalculate ax given qx, mx and age length for the terminal age group
      if (a == n_ages - 1) {
        ax_female(a, y) = (qx_female(a,y) + (adjusted_mx_female(a, y) * t * (qx_female(a, y) - 1))) / (adjusted_mx_female(a, y) * qx_female(a, y));
        ax_male(a, y) = (qx_male(a,y) + (adjusted_mx_male(a, y) * t * (qx_male(a, y) - 1))) / (adjusted_mx_male(a, y) * qx_male(a, y));
      }

      // calculate lx given qx
      if (a == 0) {
        lx_female(a, y) = 1;
        lx_male(a, y) = 1;
      } else {
        lx_female(a, y) = lx_female(a - 1, y) * (1 - qx_female(a - 1, y));
        lx_male(a, y) = lx_male(a - 1, y) * (1 - qx_male(a - 1, y));
      }

      // calculate dx given lx
      if (a > 0) {
        dx_female(a - 1, y) = lx_female(a - 1, y) - lx_female(a, y);
        dx_male(a - 1, y) = lx_male(a - 1, y) - lx_male(a, y);
      }
      if (a == n_ages - 1) {
        dx_female(a, y) = lx_female(a, y);
        dx_male(a, y) = lx_male(a, y);
      }
    }
  }

  // calculate nLx given lx, ax, and dx
  array<Type> nLx_female(n_ages, n_periods);
  array<Type> nLx_male(n_ages, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = 0; a < n_ages; a++) {
      if (a == n_ages - 1) {
        nLx_female(a, y) = lx_female(a, y) / adjusted_mx_female(a, y);
        nLx_male(a, y) = lx_male(a, y) / adjusted_mx_male(a, y);
      } else {
        nLx_female(a, y) = (age_int * lx_female(a + 1, y)) + (ax_female(a, y) * dx_female(a, y));
        nLx_male(a, y) = (age_int * lx_male(a + 1, y)) + (ax_male(a, y) * dx_male(a, y));
      }
    }
  }

  // calculate Tx given nLx
  array<Type> Tx_female(n_ages, n_periods);
  array<Type> Tx_male(n_ages, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = n_ages - 1; a >= 0; a--) {
      if (a == n_ages - 1) {
        Tx_female(a, y) = nLx_female(a, y);
        Tx_male(a, y) = nLx_male(a, y);
      } else {
        Tx_female(a, y) = Tx_female(a + 1, y) + nLx_female(a, y);
        Tx_male(a, y) = Tx_male(a + 1, y) + nLx_male(a, y);
      }
    }
  }

  // calculate survivorship ratio
  array<Type> s_female(n_ages + 1, n_periods);
  array<Type> s_male(n_ages + 1, n_periods);
  for (int y = 0; y < n_periods; y++) {
    for (int a = 0; a < n_ages + 1; a++) {
      if (a == 0) { // youngest age group
        s_female(a, y) = nLx_female(a, y) / (age_int * lx_female(a, y));
        s_male(a, y) = nLx_male(a, y) / (age_int * lx_male(a, y));
      } else if (a >= n_ages - 1) { // terminal age group
        s_female(a, y) = Tx_female(n_ages - 1, y) / Tx_female(n_ages - 2, y);
        s_male(a, y) = Tx_male(n_ages - 1, y) / Tx_male(n_ages - 2, y);
      } else {
        s_female(a, y) = nLx_female(a, y) / nLx_female(a - 1, y);
        s_male(a, y) = nLx_male(a, y) / nLx_male(a - 1, y);
      }
    }
  }

  //**** Level 2 CCMPP.
  std::cout << "CCMP:"<< std::endl;
  // project initial population forward
  array<Type> pop_female = ccmpp(input_n0_female.col(0), input_n0_male.col(0), s_female, s_male,
                                 g_female, g_male, input_f, input_srb,
                                 age_int, row_fert_start, row_fert_end, true, false);
  array<Type> pop_male = ccmpp(input_n0_female.col(0), input_n0_male.col(0), s_female, s_male,
                               g_female, g_male, input_f, input_srb,
                               age_int, row_fert_start, row_fert_end, false, false);

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
  PARALLEL_REGION nll -= dnorm(log_sigma_c, mu_c, sig_c, true);

  // correlation parameters
  PARALLEL_REGION nll -= dnorm(logit_rho_c_t, rho_c_t_mu, rho_c_t_sig, true);
  PARALLEL_REGION nll -= dnorm(logit_rho_c_a, rho_c_a_mu, rho_c_a_sig, true);

  //** NLL contribution from Level 3
  std::cout << "Level 3 likelihood:" << std::endl;

  ADREPORT(logit_c_female);
  ADREPORT(logit_c_male);
  ADREPORT(log_sigma_c);
  array<Type> abs_diff_logit_c_female = (logit_c_female - input_logit_c_female);
  array<Type> abs_diff_logit_c_male = (logit_c_male - input_logit_c_male);

  PARALLEL_REGION nll += SCALE(SEPARABLE(AR1(rho_c_t), AR1(rho_c_a)), sigma_c)(abs_diff_logit_c_female);
  PARALLEL_REGION nll += SCALE(SEPARABLE(AR1(rho_c_t), AR1(rho_c_a)), sigma_c)(abs_diff_logit_c_male);

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