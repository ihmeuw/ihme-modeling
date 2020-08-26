#include <Rcpp.h>
#include <iostream>
#include <iomanip>
#include <numeric>
using namespace std;
using namespace Rcpp;
// compute population attributable fraction from x and fx density vector
// [[Rcpp::export]]
List ensemble(NumericVector x, NumericVector fx, double tmrel, double rr, double rr_scalar, int inv_exp, double cap) {
  // access the global env.
  // Environment env = Environment::global_env();
  // NumericVector fx = env["fx"];
  // NumericVector xx = env["x"];
  // std::cout.precision(20);
  // double h = env["h"];
  // float h = 0.0595242;
  // double h = 5;
  // cout.precision(10);
  // std::cout << "(1) " << std::fixed << x << std::endl;
  // std::cout << std::setprecision(10) << h << '\n';
  // double tmrel = env["tmrel"];
  // double tmrel = 3;
  // double rr = env["rr"];
  // double rr = 1.2;
  // double rr_scalar = env["rr_scalar"];
  // double rr_scalar = 1;
  // init vector
  double n = fx.size();
  std::vector<double> fim(n);
  // NumericVector fim(n);
  fim[0] = fx[0];
  double xp = x[2]-x[1];
  // with of interval is (b-a)/n
  long double fraction = (1/3.);
  double h = (x[x.size() - 1]-x[1])/n;
  if (inv_exp == 0) {
    for (int p = 1; p < n; p++) {
      double xm = x[p] + xp;
      fim[p] = (4*((fx[p+1]-fx[p])/2)+fx[p] + fx[p] + fx[p+1])*h*fraction * std::pow(rr , ((((xm-tmrel + std::abs(xm-tmrel))/2) - ((xm-cap)+std::abs(xm-cap))/2)/rr_scalar));
    }
  }

  // protective
  if (inv_exp == 1) {
    for (int p = 1; p < n; p++) {
      double xm = x[p] + xp;
      fim[p] = (4*((fx[p+1]-fx[p])/2)+fx[p] + fx[p] + fx[p+1])*h*fraction * std::pow(rr , ((((tmrel-xm + std::abs(tmrel-xm))/2) - ((cap-xm) + std::abs(cap-xm))/2)/rr_scalar));
    }
  }

  double SUM = 0;
  for(std::vector<double>::iterator it = fim.begin(); it != fim.end(); ++it)
    SUM += *it;

  double paf_e = (SUM-1)/SUM;
  return List::create(Named("paf") = paf_e);
}
