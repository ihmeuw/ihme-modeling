// multilikeihood model for malaria disaggregation
// Given point parasite surveys and country/admin level cases data, estimate pixel level malaria prevalence.
// 
// Author: ***** ****
// Date: 2016-10-03
// For IMHE modelling

// Requirements
// To be compiled/optimised with TMB - ADDRESS
// Uses a mesh built with INLA - ADDRESS


// Table of contents
// Data: Spatial field mesh and matrices, polygon data, point data
// Read in parameter starting values and set priors: regression slopes, field parameters, time series parameters.
// Measure size of objects
// Likelihood from parameters and priors
// Calculate fields
// Likelihood from data: point data, polygon data.

// The model: 
// point +ve | examined = inv.logit( raster covariates + national covariates + spatial field 2016 + spline[5 x time slice fields] ) + binomial error
// polygon API = sum(pixel pop x prevToIncConversion(inv.logit(PR))) / sum(pixel pop) + normal or gamma error
// PR = raster covariates + national covariates + spatial field 2016 + spline[5 x time slice fields]
// prevToIncConversion = exp(polynomial model)


#include <TMB.hpp>

template<class Type>
Type objective_function<Type>::operator() () {

using namespace R_inla;
using namespace density;
using namespace Eigen;

// Control parameter. Reduce the strength of the likelihood.
//   This means to get good starting values you can have no constraints on the field
//   You will therefore get a very non smooth field. Apprently this works well as a starting value.
DATA_SCALAR(softprior);
DATA_SCALAR(pointlike);

// ------------------------------------------------------------------------ //
// Spatial field data
// ------------------------------------------------------------------------ //

// The A matrices are for projecting the mesh to a point for the pixel and point data respectively.
// spde is the spde model itself.
DATA_SPARSE_MATRIX(Apixel);
DATA_SPARSE_MATRIX(Apoint);
DATA_STRUCT(spde, spde_t);



// ------------------------------------------------------------------------ //
// Polygon level data
// ------------------------------------------------------------------------ //

// Pixel data. 
// All in long format so that if a pixel is in multiple polygons or multiple years, it will be represented by multiple rows.
// Environmental/other covariates data matrix
DATA_MATRIX(x);

// And national level covariates (1 row for each case in polygon_mean_API, in order)
DATA_MATRIX(nationalcovs);
// region id (pixel i is in polygon j).
//DATA_VECTOR(xid);
// binary with 1 indicating the beginning of a new block of rows. 
// Assumes that each polygon is represented by a contiguous block of ros.
//DATA_IVECTOR(augmentcaseid);
// Population fraction of a polygon in a pixel. i.e. pixel i makes contains 0.01 of the population of polygon j
DATA_VECTOR(xpop);
// For each cases row, how strong is the contribution from each of the 4 (columns) spline fields
DATA_MATRIX(time_spline_cov); // dim: npix x 4
// EEach country has a seperate block of 7 predictors which govern the splines.
DATA_MATRIX(time_spline_national); //dim: ncountries * 7 x ncases


// two col matrix with start end indices for each shape case. 
DATA_IARRAY(startendindex); 

// i.e. the pixels that are in polygon_normal_likelihoods[1] are 
//   x[startendindex[1, 1], startendindex[1, 2]]


// Shape data. Cases and region id.
// Cases are given as an indicator variable `subnational_normal_likelihoods`
// 1 => use a normal likelihood (API is not very close to zero)
// 0 => use a gamme likelihood (API is very close to zero)
DATA_IVECTOR(polygon_normal_likelihoods);
DATA_VECTOR(polygon_mean_API);
DATA_VECTOR(polygon_sd_API);
//DATA_VECTOR(casesid);


// Year INDEX
// Starting at 0, 1, 2 -... S
//DATA_VECTOR(casesyear);




// ------------------------------------------------------------------------ //
// Point level data
// ------------------------------------------------------------------------ //

// Point data. 
// Covariates matrix,
DATA_MATRIX(pointx);
// And national level covariates
DATA_MATRIX(nationalpointcovs);
// number of cases and denominators.
DATA_VECTOR(pointcases);
DATA_VECTOR(pointtested);

// For each year (1980-2016 = 37 rows) how strong is the contribution from each of the 5 (columns) fields
DATA_MATRIX(point_time_spline_cov); // dim: npoints x 5
DATA_MATRIX(point_time_spline_national);

// Weight polygon likelihood by this much
DATA_VECTOR(weights);


// ------------------------------------------------------------------------ //
// Parameters
// ------------------------------------------------------------------------ //

// regression slopes
// (log of) empirical mean incidence to guide intercept
PARAMETER(intercept); // intercept
PARAMETER_VECTOR(slope); 
PARAMETER_VECTOR(nationalslope);
PARAMETER_VECTOR(nationalsplines);

Type priormean_intercept = -4.0; 
Type priorsd_intercept = 1.0; //priormean_intercept from data entry
Type priormean_slope = 0.0;
Type priorsd_slope = 2.0;


// 2016 spde hyperparameters
// tau defines strength of random field. 
// kappa defines distance within which points in field affect each other. 
PARAMETER(log_tau);
PARAMETER(log_kappa);

// Priors on spde hyperparameters
//   kappa -- i.e. exp(priormean_log_kappa) -- set as approximately the width of the region being studied.
//   This implies prior belief in a fairly flat field.
//   tau -- exp(priormean_log_kappa) -- set to close to zero. Betas on regression coefficients have priors of 0 so this is reasonable.
Type priormean_log_kappa = -4.0;
Type priorsd_log_kappa   = 1.0;
Type priormean_log_tau   = -2.0;
Type priorsd_log_tau     = 2.0;

// Convert hyperparameters to natural scale
Type tau = exp(log_tau);
Type kappa = exp(log_kappa);


// Space-time random effect parameters
// matrix logit_pr_offset [nrows = n_mesh, col=n_years].
PARAMETER_VECTOR(nodemean);




// Spline parameters
// Node values for each of 5 temporal fields
PARAMETER_VECTOR(spline_nodes_one);
PARAMETER_VECTOR(spline_nodes_two);
PARAMETER_VECTOR(spline_nodes_three);
PARAMETER_VECTOR(spline_nodes_four);

// temporal meshes hyperparameters
PARAMETER(log_kappa_spline);
PARAMETER(log_tau_spline);
PARAMETER_VECTOR(mean_spline_coeffs); // length: 4

// Priors on spde hyperparameters
//   kappa -- i.e. exp(priormean_log_kappa) -- set as approximately the width of the region being studied.
//   This implies prior belief in a fairly flat field.
//   tau -- exp(priormean_log_kappa) -- set to close to zero. Betas on regression coefficients have priors of 0 so this is reasonable.
Type priormean_log_kappa_spline = -3.0;
Type priorsd_log_kappa_spline   = 1;
Type priormean_log_tau_spline   = -2.0;
Type priorsd_log_tau_spline     = 2.0;

Type priormean_mean_spline_coeffs = 0;
Type priorsd_mean_spline_coeffs = 1;


// Convert hyperparameters to natural scale
Type tau_spline = exp(log_tau_spline);
Type kappa_spline = exp(log_kappa_spline);




// Prevalence to incidence conversion parameters 
DATA_VECTOR(prev_inc_par) // length: 3 



// get number of data points to loop over
// y (cases) length
int n = polygon_mean_API.size(); 
// Number of pixels
int pixn = x.rows();
// Number of point surveys
int pointn = pointcases.size();


// ------------------------------------------------------------------------ //
// Likelihood from priors
// ------------------------------------------------------------------------ //



// Initialise negative log likelihood then calc nll of the gaussian markov random field and AR time series
Type nll = 0.0;


// Likelihood of slope parameters given priors
nll -= dnorm(intercept, priormean_intercept, priorsd_intercept, true);
for(int s = 0; s < slope.size(); s++){
  nll -= dnorm(slope[s], priormean_slope, priorsd_slope, true);
}
for(int s = 0; s < nationalslope.size(); s++){
  nll -= dnorm(nationalslope[s], priormean_slope, priorsd_slope, true);
}
for(int s = 0; s < nationalsplines.size(); s++){
  nll -= dnorm(nationalsplines[s], priormean_slope, priorsd_slope, true);
}


// Likelihood of hyperparameters for 2016 field
nll -= dnorm(log_kappa, priormean_log_kappa, priorsd_log_kappa, true);
nll -= dnorm(log_tau, priormean_log_tau, priorsd_log_tau, true);

// Likelihood of hyperparameters for 2016 field
nll -= dnorm(log_kappa_spline, priormean_log_kappa_spline, priorsd_log_kappa_spline, true);
nll -= dnorm(log_tau_spline, priormean_log_tau_spline, priorsd_log_tau_spline, true);


// Build 2016 spde matrix
SparseMatrix<Type> Q = Q_spde(spde, kappa);

// Likelihood of the random field.
nll += softprior * SCALE(GMRF(Q), 1.0/tau)(nodemean);

// Build temporal spde matrices.

SparseMatrix<Type> Q_spline = Q_spde(spde, kappa_spline);

nll += softprior * SCALE(GMRF(Q_spline), 1.0/tau_spline)(spline_nodes_one);
nll += softprior * SCALE(GMRF(Q_spline), 1.0/tau_spline)(spline_nodes_two);
nll += softprior * SCALE(GMRF(Q_spline), 1.0/tau_spline)(spline_nodes_three);
nll += softprior * SCALE(GMRF(Q_spline), 1.0/tau_spline)(spline_nodes_four);

nll -= dnorm(mean_spline_coeffs[0], priormean_mean_spline_coeffs, priorsd_mean_spline_coeffs, true);
nll -= dnorm(mean_spline_coeffs[1], priormean_mean_spline_coeffs, priorsd_mean_spline_coeffs, true);
nll -= dnorm(mean_spline_coeffs[2], priormean_mean_spline_coeffs, priorsd_mean_spline_coeffs, true);
nll -= dnorm(mean_spline_coeffs[3], priormean_mean_spline_coeffs, priorsd_mean_spline_coeffs, true);

Type nll1 = nll;




// ------------------------------------------------------------------------ //
// Calculate random field effects
// ------------------------------------------------------------------------ //


// Calculate spline fields for pixel data
vector<Type> logit_prevalence_field_2016;
logit_prevalence_field_2016 = Apixel * nodemean;
vector<Type> spline_field_one;
spline_field_one = Apixel * spline_nodes_one;
vector<Type> spline_field_two;
spline_field_two = Apixel * spline_nodes_two;
vector<Type> spline_field_three;
spline_field_three = Apixel * spline_nodes_three;
vector<Type> spline_field_four;
spline_field_four = Apixel * spline_nodes_four;


// Calculate spline fields for point data
vector<Type> logit_prevalence_field_point_2016;
logit_prevalence_field_point_2016 = Apoint * nodemean;
vector<Type> spline_field_point_one;
spline_field_point_one = Apoint * spline_nodes_one;
vector<Type> spline_field_point_two;
spline_field_point_two = Apoint * spline_nodes_two;
vector<Type> spline_field_point_three;
spline_field_point_three = Apoint * spline_nodes_three;
vector<Type> spline_field_point_four;
spline_field_point_four = Apoint * spline_nodes_four;



// ------------------------------------------------------------------------ //
// Likelihood from data
// ------------------------------------------------------------------------ //



// Point data likelihood

if(pointlike != 0){
  vector<Type> pointcovars(pointn);
  pointcovars = intercept + 
                  pointx * slope + 
                  nationalpointcovs * nationalslope + 
                  point_time_spline_national * nationalsplines +
                  logit_prevalence_field_point_2016.array() + 
                  point_time_spline_cov.col(0).array() * (mean_spline_coeffs[0] + spline_field_point_one.array()) +
                  point_time_spline_cov.col(1).array() * (mean_spline_coeffs[1] + spline_field_point_two.array()) +
                  point_time_spline_cov.col(2).array() * (mean_spline_coeffs[2] + spline_field_point_three.array()) +
                  point_time_spline_cov.col(3).array() * (mean_spline_coeffs[3] + spline_field_point_four.array()) ;
                  
  // Transform linear predictor to prevalence scale. Then push through prevalence to incidence transformation
  vector<Type> point_prevalence(pointn);
  point_prevalence = invlogit(pointcovars);
  
  REPORT(point_prevalence);
  
  vector<Type> pointnll;
  pointnll = (pointcases.array() * log(point_prevalence.array()) + (pointtested.array() - pointcases.array()) * log(Type(1) - point_prevalence.array()));
  nll -= pointlike * sum(pointnll);
  
  REPORT(pointnll);
}
Type nll2 = nll;

// Polygon level likelihood
// For each i in n = cases.size()
//   Find the pixels with correct OBJECTID
//   Sum rate from each pixel
//   Calculate likelihood of binomial.


vector<Type> pixel_linear_pred(pixn);
pixel_linear_pred = intercept + x*slope +
                      logit_prevalence_field_2016.array();

// recalculate startendindices to be in the form start, n
startendindex.col(1) = startendindex.col(1) - startendindex.col(0) + 1;

// national covs effect
vector<Type> nationalcovseffect = nationalcovs * nationalslope;
vector<Type> nationalsplineeffect = time_spline_national * nationalsplines;

Type nationallinearpred = 0.0;

vector<Type> inshape_prev;
vector<Type> inshape_incidencerate;
vector<Type> inshape_incidence;
vector<Type> inshape_pop;
Type shapeincidence = 0.0;
Type shapepop = 0.0;

vector<Type> reportinc(n);
vector<Type> reportpop(n);
//vector<Type> inshape_prevpop;
vector<Type> reportpopprev(n);
vector<Type> reportprev(n);
vector<Type> mean_inshape_prev(n);
vector<Type> min_inshape_prev(n);
vector<Type> reportnat_lin_pred(n);

//For each shape use startendindex to find sum of pixel incidence rates
for (int s = 0; s < n; s++) {
  // Calc national covariate linear pred that will be added to each pixel (
  nationallinearpred = nationalcovseffect[s] + nationalsplineeffect[s];
  reportnat_lin_pred[s] = nationallinearpred;
  // Sum pixel risks (raster + field + national

  // Create logit prevalence (linear predictor) using temporal splines and national covs
  inshape_prev = pixel_linear_pred.segment(startendindex(s, 0), startendindex(s, 1)).array() +
                         nationallinearpred +
                         time_spline_cov(s, 0) * (mean_spline_coeffs[0] + spline_field_one.segment(startendindex(s, 0), startendindex(s, 1)).array()) +
                         time_spline_cov(s, 1) * (mean_spline_coeffs[1] + spline_field_two.segment(startendindex(s, 0), startendindex(s, 1)).array()) +
                         time_spline_cov(s, 2) * (mean_spline_coeffs[2] + spline_field_three.segment(startendindex(s, 0), startendindex(s, 1)).array()) +
                         time_spline_cov(s, 3) * (mean_spline_coeffs[3] + spline_field_four.segment(startendindex(s, 0), startendindex(s, 1)).array());
  inshape_prev = invlogit(inshape_prev).pow(4);
  mean_inshape_prev[s] = sum(inshape_prev) / inshape_prev.size();
  min_inshape_prev[s] = min(inshape_prev);
    
    
  // Push through **** prevalence to incidence rate model
  inshape_incidencerate = inshape_prev * prev_inc_par[0] +
                          inshape_prev.pow(2) * prev_inc_par[1] +
                          inshape_prev.pow(3) * prev_inc_par[2];
  // Calculate pixel incidence and then polyogn incidence
  inshape_incidence = (inshape_incidencerate * xpop.segment(startendindex(s, 0), startendindex(s, 1)).array());
  shapeincidence = sum(inshape_incidence);

  // extract pixel pop and then sum to polygon population
  inshape_pop = xpop.segment(startendindex(s, 0), startendindex(s, 1));
  shapepop = sum(inshape_pop);

  reportinc[s] = 1000 * shapeincidence / shapepop;
  reportpop[s] = shapepop;
  //inshape_prevpop = (inshape_prev * xpop.segment(startendindex(s, 0), startendindex(s, 1)).array());
  //reportpopprev[s] = sum(inshape_prevpop) / shapepop;
  reportprev[s] = sum(inshape_prev) / startendindex(s, 1);
  
  
  if (polygon_normal_likelihoods[s] == 1) {
    nll -= weights[s] * dnorm(1000 * shapeincidence/shapepop,polygon_mean_API[s],polygon_sd_API[s],true); 
  } else {
    nll -= weights[s] * dgamma(1000 * shapeincidence / shapepop, Type(1.0), polygon_mean_API[s], true);
  }
}

Type nll3 = nll;


REPORT(reportinc);
REPORT(reportpop);
REPORT(reportprev);
REPORT(reportpopprev);
REPORT(polygon_mean_API);
REPORT(mean_inshape_prev);
REPORT(min_inshape_prev);
REPORT(reportnat_lin_pred);
REPORT(nll1);
REPORT(nll2);
REPORT(nll3);


return nll;
}
