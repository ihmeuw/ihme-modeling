% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extend_qx.R, R/extend_qx_new_format.R
\name{extend_qx}
\alias{extend_qx}
\title{Extend qx up to age 105 using HMD regression parameters
logit(5q{x+5}) - logit(5qx) = age_dummy + B1*[logit(5qx base)] + location RE}
\usage{
extend_qx(empir_lt, hmd_qx_results, by_vars)

extend_qx(empir_lt, hmd_qx_results, by_vars)
}
\arguments{
\item{empir_lt}{data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, ax}

\item{hmd_qx_results}{data.table with variables sex, start, par_cons, pari_logitqx60(5)95, par_Iage_65(5)100}

\item{by_vars}{character vector containing the variable names of all variables that uniquely identify the observations (except for age)}

\item{empir_lt}{data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, ax}

\item{hmd_qx_results}{data.table with variables sex, start, par_cons, pari_logitqx60(5)95, par_Iage_65(5)100}

\item{by_vars}{character vector containing the variable names of all variables that uniquely identify the observations (except for age)}
}
\value{
returns empir_lt with the same variables, but qx extended up to age 105

returns empir_lt with the same variables, but qx extended up to age 105
}
\description{
NOTE: this is for the GBD 2017 HMD extension method, but with a newly generated parameter file format
Instead of the stata-based hmd_qx_results input, it's regression results from empirical_life_tables/hmd_prep/gen_hmd_qx_ax_extension.R
}
