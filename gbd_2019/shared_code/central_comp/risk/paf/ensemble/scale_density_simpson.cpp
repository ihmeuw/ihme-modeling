#include <Rcpp.h>
#include <iostream>
#include <iomanip>
#include <numeric>
using namespace std;
using namespace Rcpp;
// scale fx density using Simpson's approximation
// [[Rcpp::export]]
List scale_density_simpson(NumericVector x, NumericVector fx) {
  // init vector
  double n = fx.size();
  std::vector<double> fim(n);
  // NumericVector fim(n);
  fim[0] = fx[0];
  // with of interval is (b-a)/n
  long double fraction = (1/3.);
  double h = (x[x.size() - 1]-x[1])/n;
  for (int p = 1; p < n; p++) {
    fim[p] = (4*((fx[p+1]-fx[p])/2)+fx[p] + fx[p] + fx[p+1])*h*fraction;
  }

  // set last index to one before it
  fim[fim.size()-1] = fim[fim.size()-2];

  double SUM = 0;
  for(std::vector<double>::iterator it = fim.begin(); it != fim.end(); ++it)
    SUM += *it;

  std::vector<double> s_fx(n);
  for (int p = 1; p <= n; p++) {
    s_fx[p]=fx[p]/SUM;
  }

  return List::create(Named("s_fx") = s_fx);
}
