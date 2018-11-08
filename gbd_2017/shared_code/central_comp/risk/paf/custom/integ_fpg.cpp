#include <Rcpp.h>
#include <iostream>
#include <iomanip>
#include <numeric>
using namespace std;
using namespace Rcpp;
// integrate fpg to use in diabetes optimization using midpoint approximation
// [[Rcpp::export]]
List integ_fpg(NumericVector x, NumericVector fx) {
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
  for (int p = 1; p < n; p++) {
    double xm = x[p] + xp;
    if (xm >= 7) {
      fim[p] = (4*((fx[p+1]-fx[p])/2)+fx[p] + fx[p] + fx[p+1])*h*fraction;
    }
  }

  // set last index to one before it
  fim[fim.size()-1] = fim[fim.size()-2];

  double SUM_diabetic = 0;
  for(std::vector<double>::iterator it = fim.begin(); it != fim.end(); ++it)
    SUM_diabetic += *it;

  return List::create(Named("diabetic") = SUM_diabetic);
}

