// Defines helper functions needed to fit bayespop models

#include <math.h>       /* exp, pow, log, lgamma, tgamma */

//this scaled logit restricts x between -1 and 1 instead of 0 and 1.
template<class Type>
Type scaled_logit(Type x) {
  return log((1 + x) / (1 - x));
}
template<class Type>
Type inv_scaled_logit(Type x) {
  return (exp(x) - 1) / (exp(x) + 1);
}

// Probability density function of the inverse gamma distribution
template<class Type>
Type dinvGamma(Type x, Type shape, Type scale, int give_log=0) {
  if (give_log) {
    return shape * log(scale) - lgamma(asDouble(shape)) - (shape + Type(1)) * log(x) - (scale / x);
  } else {
    return pow(scale, shape) / tgamma(asDouble(shape)) * pow((Type(1) / x), (shape + Type(1))) * exp(-scale / x);
  }
}

// exponentiate each element of a array
template<class Type>
array<Type> exp_array(array<Type> log_array) {
  array<Type> a(log_array.rows(), log_array.cols());
  for (int i = 0; i < log_array.rows(); i++) {
    for (int j = 0; j < log_array.cols(); j++) {
      a(i, j) = exp(log_array(i, j));
    }
  }
  return a;
}

// inverse logit each element of a array
template<class Type>
array<Type> invlogit_array(array<Type> logit_array) {
  array<Type> a(logit_array.rows(), logit_array.cols());
  for (int i = 0; i < logit_array.rows(); i++) {
    for (int j = 0; j < logit_array.cols(); j++) {
      a(i, j) = invlogit(logit_array(i, j));
    }
  }
  return a;
}

// log each element of a array
template<class Type>
array<Type> log_array(array<Type> a) {
  array<Type> result(a.rows(), a.cols());
  for (int i = 0; i < a.rows(); i++) {
    for (int j = 0; j < a.cols(); j++) {
      result(i, j) = log(a(i, j));
    }
  }
  return result;
}

// make leslie matrix
template<class Type>
matrix<Type> make_leslie_matrix(int year, array<Type> srb, array<Type> fertility, array<Type> survival, int age_int, int row_fert_start, int row_fert_end, bool female) {

  int n_ages = survival.rows() - 1;

  // initialize Leslie matrix
  matrix<Type> leslie;
  leslie.setZero(n_ages, n_ages);

  // fill in Leslie matrix
  // fill survival
  for (int a = 0; a < n_ages - 1; a++) {
    leslie(a + 1, a) = survival(a + 1, year);
  }
  leslie(n_ages - 1, n_ages - 1) = survival(n_ages, year);

  // fill fertility + birth survival for female population
  if (female) {
    Type fert_this_age_group;
    Type fert_one_age_group_above;
    Type surv_one_age_group_above;
    for (int a = 0; a < n_ages; a++) {
      if (a >= row_fert_start - 1 && a <=  row_fert_end - 1) {
        fert_one_age_group_above = fertility(a + 1 - row_fert_start, year);
        surv_one_age_group_above = survival(a + 1, year);
        leslie(0, a) = fert_one_age_group_above * surv_one_age_group_above;
      }
      if (a >= row_fert_start && a <= row_fert_end) {
        fert_this_age_group = fertility(a - row_fert_start, year);
        leslie(0, a) = leslie(0, a) + fert_this_age_group;
      }
      leslie(0, a) = leslie(0, a) * age_int * 0.5;
    }
  }
  return leslie;
}

// use cohort migration rate times population at the beginning of the projection period to
// get half the net number of migrants during a projection period by age
// first element is zero since we need birth numbers to calculate it
template<class Type>
matrix<Type> calculate_half_number_migrants(matrix<Type> pop, array<Type> migration, int age_int) {
  int n_ages = pop.rows();
  matrix<Type> half_net_migrants_cohort;
  half_net_migrants_cohort.setZero(n_ages + 1, 1);

  for (int a = 0; a < n_ages; a++) {
    half_net_migrants_cohort.row(a + 1) = pop.row(a) * migration(a + 1) * Type(age_int) * Type(0.5);
  }
  return half_net_migrants_cohort;
}

// totals to be added on at the beginning of each projection interval for the leslie matrix
// does not include net number of baby migrants
template<class Type>
matrix<Type> calculate_migrants_beginning(matrix<Type> half_net_migrants_cohort) {
  int cohort_groups = half_net_migrants_cohort.rows();
  matrix<Type> half_net_migrants_cohort_beginning;
  half_net_migrants_cohort_beginning.setZero(cohort_groups - 1, 1);

  for (int a = 0; a < cohort_groups - 1; a++) {
    half_net_migrants_cohort_beginning.row(a) = half_net_migrants_cohort.row(a + 1);
  }
  return half_net_migrants_cohort_beginning;
}

// totals to be added on at the end of each projection interval for the leslie matrix
// combines the last two age groups together
template<class Type>
matrix<Type> calculate_migrants_end(matrix<Type> half_net_migrants_cohort) {
  int cohort_groups = half_net_migrants_cohort.rows();
  matrix<Type> half_net_migrants_cohort_end;
  half_net_migrants_cohort_end.setZero(cohort_groups - 1, 1);

  for (int a = 0; a < cohort_groups - 2; a++) {
    half_net_migrants_cohort_end.row(a) = half_net_migrants_cohort.row(a);
  }

  // combine last two age groups on since they will both be added to the terminal age group
  half_net_migrants_cohort_end.row(cohort_groups - 2) = half_net_migrants_cohort.row(cohort_groups - 2) + half_net_migrants_cohort.row(cohort_groups - 1);

  return half_net_migrants_cohort_end;
}


// Project baseline population forward using vital rates
template<class Type>
array<Type> ccmpp(array<Type> n0_female, array<Type> n0_male,
                  array<Type> s_female, array<Type> s_male,
                  array<Type> g_female, array<Type> g_male,
                  array<Type> f, array<Type> srb, int age_int,
                  int row_fert_start, int row_fert_end, bool female) {

  // Get number of years and ages
  int n_periods = g_female.cols();
  int n_ages = n0_female.rows();

  // initialize population matrices
  array<Type> pop_male(n_ages, n_periods + 1);
  array<Type> pop_female(n_ages, n_periods + 1);
  pop_male.col(0) = n0_male;
  pop_female.col(0) = n0_female;

  // loop through years. Construct leslie matrix, calculate migration counts and project population forward one year.
  for (int y = 0; y < n_periods; y++) {

    // The starting population of the current projection period
    matrix<Type> curr_pop_male = pop_male.col(y);
    matrix<Type> curr_pop_female = pop_female.col(y);

    // Calculate the net number of migrants that occur in a cohort
    matrix<Type> half_net_migrants_cohort_male = calculate_half_number_migrants(curr_pop_male, g_male.col(y), age_int);
    matrix<Type> half_net_migrants_cohort_female = calculate_half_number_migrants(curr_pop_female, g_female.col(y), age_int);

    // Prepare object to be added on at the beginning of each projection period,
    // Should have the same age groups as the population vector, does not include baby migrants
    matrix<Type> half_net_migrants_cohort_beginning_male = calculate_migrants_beginning(half_net_migrants_cohort_male);
    matrix<Type> half_net_migrants_cohort_beginning_female = calculate_migrants_beginning(half_net_migrants_cohort_female);

    // Prepare object to be added on at the end of each projection period
    // The terminal age group and the one below need to both be added on to the terminal age group population of the next projection period
    // The first element, baby migrants is 0 since this needs to be manually calculated and added on later
    matrix<Type> half_net_migrants_cohort_end_male = calculate_migrants_end(half_net_migrants_cohort_male);
    matrix<Type> half_net_migrants_cohort_end_female = calculate_migrants_end(half_net_migrants_cohort_female);

    // project one year
    matrix<Type> leslie_male = make_leslie_matrix(y, srb, f, s_male, age_int, row_fert_start, row_fert_end, false);
    matrix<Type> leslie_female = make_leslie_matrix(y, srb, f, s_female, age_int, row_fert_start, row_fert_end, true);
    pop_male.col(y + 1) = (leslie_male * (curr_pop_male + half_net_migrants_cohort_beginning_male)) + half_net_migrants_cohort_end_male;
    pop_female.col(y + 1) = (leslie_female * (curr_pop_female + half_net_migrants_cohort_beginning_female)) + half_net_migrants_cohort_end_female;

    // total births calculated in the first row of the female matrix, need to split into sex specific births
    pop_male(0, y + 1) = pop_female(0, y + 1) * (srb(0, y) / (1 + srb(0, y)));
    pop_female(0, y + 1) = pop_female(0, y + 1) * (1 / (1 + srb(0, y)));

    // use sex specific birth totals to calculate the number of baby migrants
    Type half_new_born_migrants_male = g_male(0, y) * pop_male(0, y + 1) * 0.5;
    Type half_new_born_migrants_female = g_female(0, y) * pop_female(0, y + 1) * 0.5;

    // need to add on migrating babies and apply survival ratios to get the population in this first age group
    pop_male(0, y + 1) = ((pop_male(0, y + 1) + half_new_born_migrants_male) * s_male(0, y)) + half_new_born_migrants_male;
    pop_female(0, y + 1) = ((pop_female(0, y + 1) + half_new_born_migrants_female) * s_female(0, y)) + half_new_born_migrants_female;
  }

  if (female) {
    return pop_female;
  } else {
    return pop_male;
  }
}

// interpolation to handle irregular census intervals when estimating using age interval of 5
template<class Type>
array<Type> arc_interpolation(array<Type> pop, int n_ages, int n_periods, int age_int) {

  // initialize full population matrix
  int n_full_periods;
  if (age_int == 5) {
    n_full_periods = ((n_periods) * 5) + 1;
  } else {
    n_full_periods = n_periods + 1;
  }
  array<Type> full_pop(n_ages, n_full_periods);

  if (age_int == 5) {

    // calculate age specific growth rates between projection intervals
    array<Type> growth_rate(n_ages, n_periods);
    for (int a = 0; a < n_ages; a++) {
      for (int y = 0; y < n_periods; y++) {
        growth_rate(a, y) = log(pop(a, y + 1) / pop(a, y)) / age_int;
      }
    }

    // apply growth rates to projected counts to derive interpolated projects
    for (int a = 0; a < n_ages; a++) {
      for (int y = 0; y < n_periods; y++) {
        for (int y1 = 0; y1 < 5; y1++) {
          int full_pop_year = (y * 5) + y1;
          full_pop(a, full_pop_year) = pop(a, y) * exp(y1 * growth_rate(a, y));
        }
      }
      // Last year remains the same
      full_pop(a, n_full_periods - 1) = pop(a, n_periods);
    }
  } else {
    full_pop = pop;
  }
  return full_pop;
}


// aggregate projected population to census age groups. Use -1 to signify a missing value
template<class Type>
array<Type> aggregate_age_groups(array<Type> pop, array<int> ages) {

  int comparisons = ages.rows();
  array<Type> agg_pop(comparisons, 1);

  for (int i = 0; i < comparisons; i++) {
    // get the specified rows where the age group begins and ends
    int year_col = ages(i, 0);
    int age_start_row = ages(i, 1);  // inclusive
    int age_end_row = ages(i, 2);  // exclusive

    // add on all the populations needed
    for (int agg_a = age_start_row; agg_a < age_end_row; agg_a++) {
      agg_pop(i) += pop(agg_a, year_col);
    }
  }

  return agg_pop;
}
