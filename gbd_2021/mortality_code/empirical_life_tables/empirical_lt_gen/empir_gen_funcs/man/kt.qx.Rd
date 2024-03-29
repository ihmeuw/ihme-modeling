% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extend_qx_kt.R
\name{kt.qx}
\alias{kt.qx}
\title{Kannisto-Thatcher model, in 5qx space
5qx = 1 - ([a * exp(bx) + 1]/[a * exp(b(x+5)) + 1])^(1/b)}
\usage{
kt.qx(x, params)
}
\arguments{
\item{x}{age (numeric)}

\item{params}{vector of K-T parameters (a, b)}
}
\value{
returns 5qx output of K-T model for age x and parameters a, b (numeric)
}
\description{
Kannisto-Thatcher model, in 5qx space
5qx = 1 - ([a * exp(bx) + 1]/[a * exp(b(x+5)) + 1])^(1/b)
}
