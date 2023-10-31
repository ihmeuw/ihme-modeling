#include <R.h>
#include <Rinternals.h>

#include <math.h>

#define POP_15TO49_COL   0
#define POP_15ENTER_COL  1
#define POP_50EXIT_COL   2
#define POP_NETMIGR_COL  3
#define POP_HIVP15YR_COL 4
#define POP_ART15YR_COL  5
#define POP_A50RATE_COL  6
#define POP_MX_COL       7

#define EPP_RSPLINE 0
#define EPP_RTREND 1

const size_t DS = 8;  // number of disease stages (CD4 stages, including uninfected)
const size_t TS = 4;  // number


SEXP eppC(SEXP s_eppPopTS, SEXP s_projsteps, SEXP s_dt,
	   SEXP s_eppmod,
	   SEXP s_rspline_rvec, SEXP s_iota, SEXP s_relinfectART, SEXP s_tsEpidemicStart,
	   SEXP s_rtrend_beta, SEXP s_rtrend_tstabilize, SEXP s_rtrend_r0,
	   SEXP s_cd4init, SEXP s_cd4prog, SEXP s_cd4artmort,
	   SEXP s_artnumTS, SEXP s_arteligidxTS, SEXP s_mortyears, SEXP s_cd4years, SEXP s_specpop_perceligTS,
     SEXP s_hivp15yr_cd4dist, SEXP s_art15yr_cd4dist){

  size_t nsteps = length(s_projsteps);
  double dt = *REAL(s_dt);

  double *projsteps = REAL(s_projsteps);
  double *pop15to49_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_15TO49_COL));
  double *age15enter_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_15ENTER_COL));
  double *netmigr_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_NETMIGR_COL));
  double *hivp15yr_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_HIVP15YR_COL));
  double *art15yr_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_ART15YR_COL));
  double *age50rate_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_A50RATE_COL));
  double *mx_ts = REAL(VECTOR_ELT(s_eppPopTS, POP_MX_COL));

  int eppmod = *INTEGER(s_eppmod);

  double iota = *REAL(s_iota);
  double relinfectART = *REAL(s_relinfectART);
  double tsEpidemicStart = *REAL(s_tsEpidemicStart);

  double *rspline_rvec;
  double *rtrend_beta, rtrend_tstab, rtrend_r0;
  if(eppmod == EPP_RSPLINE)
    rspline_rvec = REAL(s_rspline_rvec);
  else {
    rtrend_beta = REAL(s_rtrend_beta);
    rtrend_tstab = *REAL(s_rtrend_tstabilize);
    rtrend_r0 = *REAL(s_rtrend_r0);
  }

  int mortyears = *INTEGER(s_mortyears);
  int cd4years = *INTEGER(s_cd4years);

  double *cd4init = REAL(s_cd4init);
  // double *cd4prog = REAL(s_cd4prog);
  double test_cd4prog[cd4years][DS-2];
  for (int i = 0; i < cd4years; ++i)
    for (int m = 0; m < DS-2; ++m)
      // test_cd4prog[i][m] = 0.03;
      test_cd4prog[i][m] = REAL(s_cd4prog)[i + (cd4years-1)*m];

  // double test_v[DS-2];
  // for (int m=0; m<DS-2; ++m)
  //   test_v[m] = REAL(s_cd4prog)[m];
  double test_cd4artmort[(DS-1)*(mortyears)][TS];
  for(size_t y = 0; y < mortyears; y++)
    for(size_t m = 0; m < DS-1; m++)
      for(size_t u = 0; u < TS; u++)
        test_cd4artmort[m+y*(DS-1)][u] = REAL(s_cd4artmort)[m + (DS-1)*y + (DS-1)*(mortyears)*u];

  // double cd4artmort[DS-1][TS];
  // for(size_t m = 0; m < DS-1; m++)
  //   for(size_t u = 0; u < TS; u++)
  //     cd4artmort[m][u] = REAL(s_cd4artmort)[m + (DS-1)*u];

  double *artnum_ts = REAL(s_artnumTS);
  int *arteligidx_ts = INTEGER(s_arteligidxTS);
  double *specpop_perceligTS = REAL(s_specpop_perceligTS);

  double *hivp15yr_cd4dist = REAL(s_hivp15yr_cd4dist);
  double *art15yr_cd4dist = REAL(s_art15yr_cd4dist);
  
  // initialise output
  size_t numOutDates = ceil(dt * nsteps), outIdx = 0;
  SEXP s_Xout, s_Xout_dim, s_rvec; 
  PROTECT(s_Xout = allocVector(REALSXP, numOutDates * DS * TS));
  PROTECT(s_Xout_dim = allocVector(INTSXP, 3));
  INTEGER(s_Xout_dim)[0] = numOutDates;
  INTEGER(s_Xout_dim)[1] = DS;
  INTEGER(s_Xout_dim)[2] = TS;
  setAttrib(s_Xout, R_DimSymbol, s_Xout_dim);

  PROTECT(s_rvec = allocVector(REALSXP, nsteps));
  setAttrib(s_Xout, install("rvec"), s_rvec);

  double *Xout = REAL(s_Xout);
  double *rvec = REAL(s_rvec);


  // initialise population
  double X[DS][TS];
  for(size_t m = 0; m < DS; m++)
    for(size_t u = 0; u < TS; u++)
      X[m][u] = 0.0;
  X[0][0] = pop15to49_ts[0];

  double prevlast = 0.0; // store last prevalence value for r-trend

  // do timesteps
  for(size_t ts = 0; ts < nsteps; ts++){

    // record outputs
    if((projsteps[ts] - floor(projsteps[ts])) == dt*floor(1.0/(dt*2))){
        for(size_t m = 0; m < DS; m++)
    	  for(size_t u = 0; u < TS; u++)
    	    Xout[outIdx + (m + DS*u)*numOutDates] = X[m][u];
    	outIdx++;
    }

    // sum population sizes
    double Xhivp_noart = 0.0, Xonart = 0.0;
    for(size_t m = 1; m < DS; m++){
      Xhivp_noart += X[m][0];
      for(size_t u = 1; u < TS; u++)
	Xonart += X[m][u];
    }
    double Xtot = X[0][0] + Xhivp_noart + Xonart;
    double prevcurr = 1.0 - X[0][0] / Xtot;

    // calculate r(t)
    if(eppmod == EPP_RSPLINE)
      rvec[ts] = rspline_rvec[ts];
    else {
      double t = projsteps[ts];
      if(t > tsEpidemicStart){
	double gamma_t = (t < rtrend_tstab)?0.0:(prevcurr-prevlast) * (t - rtrend_tstab) / (dt * prevlast);
	double logr_diff = rtrend_beta[1]*(rtrend_beta[0] - rvec[ts-1]) + rtrend_beta[2]*prevlast + rtrend_beta[3]*gamma_t;
	rvec[ts] = exp(log(rvec[ts-1]) + logr_diff);
      } else {
	rvec[ts] = rtrend_r0;
      }
    }
    
    // ageing, natural mortality, and net migration
    double grad[DS][TS];
    for(size_t m = 0; m < DS; m++)
      for(size_t u = 0; u < TS; u++)
	grad[m][u] = X[m][u] * (-age50rate_ts[ts] - mx_ts[ts] + netmigr_ts[ts]/(Xtot * dt));

    // new entrants
    grad[0][0] += (age15enter_ts[ts] - hivp15yr_ts[ts]) / dt;
    for(size_t m = 1; m < DS; m++){
      grad[m][0] += hivp15yr_cd4dist[m-1] * (hivp15yr_ts[ts] - art15yr_ts[ts]) / dt;
      grad[m][3] += art15yr_cd4dist[m-1] * art15yr_ts[ts] / dt;  // Assume been on ART 1+ years (not sure what EPP does)
    }


    // new HIV infections
    double incrate = rvec[ts] * (Xhivp_noart + relinfectART*Xonart)/Xtot + ((projsteps[ts] == tsEpidemicStart)?iota:0);
    grad[0][0] -= X[0][0] * incrate;
    for(size_t m = 1; m < DS; m++)
      grad[m][0] += X[0][0] * incrate * cd4init[m-1];

    // disease progression and mortality
    int step_size = 1/dt;
    int cd4prog_i = floor(ts/step_size);
    if (cd4prog_i > (cd4years-1))
      cd4prog_i = (cd4years-1);

    // double curr_prog[DS-2];
    // for (int m=0; m<DS-2; ++m)
    //   curr_prog[m] = REAL(s_cd4prog)[m];
    for(size_t m = 1; m < (DS-1); m++){  // cd4 stage progression (not on ART)
      // grad[m][0] -= cd4prog[m-1] * X[m][0];
      // grad[m+1][0] += cd4prog[m-1] * X[m][0];
      grad[m][0] -= test_cd4prog[cd4prog_i][m-1] * X[m][0];
      grad[m+1][0] += test_cd4prog[cd4prog_i][m-1] * X[m][0];
    }
    for(size_t m = 1; m < DS; m++){
      grad[m][1] -= 2.0 * X[m][1];   // ART stage progression (HARD CODED 6 months mean duration)
      grad[m][2] += 2.0 * X[m][1];
      grad[m][2] -= 2.0 * X[m][2];   
      grad[m][3] += 2.0 * X[m][2];

      for(size_t u = 0; u < TS; u++)
	grad[m][u] -= test_cd4artmort[(m-1) + (DS-1)*cd4prog_i][u] * X[m][u];  // HIV mortality
    }
    

    // ART initiation
    if(artnum_ts[ts] > 0){
      // determine number of desired ART initiations
      double artchange = 0.0;
      for(size_t m = 1; m < DS; m++)
  for(size_t u = 1; u < TS; u++)
    artchange += grad[m][u];

      size_t artelig_idx = arteligidx_ts[ts] - 1;   // -1 for 0-based indexing vs. 1-based indexing in R
      double specpop_percelig = specpop_perceligTS[ts];
      double art_anninits = (artnum_ts[ts] - Xonart) / dt - artchange;

      // determine number weights for number from each stage (average of expected mortality and eligibility)
      double sum_mortweight = 0.0, artelig = 0.0;
      for(size_t m = (specpop_percelig > 0) ? 1 : artelig_idx; m < DS; m++){
  double artelig_m = (m < artelig_idx) ? specpop_percelig * X[m][0] : X[m][0];
  artelig += artelig_m;
  sum_mortweight += test_cd4artmort[(m-1) + (DS-1)*cd4prog_i][0] * artelig_m;
      }
      // determine rate to initiate each stage
      double artinitweight[DS], artstageinit[DS];
      for(size_t m = (specpop_percelig > 0) ? 1 : artelig_idx; m < DS; m++){
  artinitweight[m] = (test_cd4artmort[(m-1) + (DS-1)*cd4prog_i][0]/sum_mortweight + 1.0/artelig)/2.0;
  double artelig_m = (m < artelig_idx) ? specpop_percelig * X[m][0] : X[m][0];
  artstageinit[m] = art_anninits * artinitweight[m] * artelig_m;
  artstageinit[m] = (artstageinit[m] > artelig_m/dt) ? artelig_m/dt : artstageinit[m]; // check not greater than number eligible
  artstageinit[m] = (artstageinit[m] > (X[m][0]/dt + grad[m][0])) ? (X[m][0]/dt + grad[m][0]) : artstageinit[m]; // check number on stage won't be less than 0;

  grad[m][0] -= artstageinit[m];
  grad[m][1] += artstageinit[m];
      }
    }
    // store prevalence to use in next time step
    prevlast = prevcurr;
      
    // do projection (euler integration)
    for(size_t m = 0; m < DS; m++)
      for(size_t u = 0; u < TS; u++)
       X[m][u] += dt*grad[m][u];

  }
  UNPROTECT(3);

  return s_Xout;
}
