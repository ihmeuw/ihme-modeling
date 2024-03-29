% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions_utility.R
\name{calculate_diff}
\alias{calculate_diff}
\title{Calculate differences between random variables}
\usage{
calculate_diff(df, alt_mean, alt_sd, ref_mean, ref_sd)
}
\arguments{
\item{df}{a data frame}

\item{alt_mean}{column name for the alternative (i.e. non-reference) means}

\item{alt_sd}{column name for the alternative standard deviations}

\item{ref_mean}{column name for the reference means}

\item{ref_sd}{column name for the reference standard deviations}
}
\value{
a data frame with the differences and their standard deviations
}
\description{
\code{calculate_diff} calculates means and SDs for differences between random variables,
ensuring that the alternative defintion/method is in the numerator: log(alt/ref) = log(alt) - log(ref)
}
