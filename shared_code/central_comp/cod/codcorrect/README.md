CoDCorrect
=====

CoDCorrect is a core piece of mortality machinery that is primarily responsible for making the Causes of Death and All-cause Mortality results internally consistent.  Using the CoDCorrect cause hierarchy, it reads in draw files from Causes of Death models (CODEm and custom models).  CoDCorrect will then scale all of the level 1 (based off cause hierarchy) CoD model data to the all-cause mortality envelope.  In other words, after CoDCorrect, all of the level 1 causes will add up to the all-cause mortality envelope.  CoDCorrect then flows down the CoDCorrect hierarchy and then scales the the sub-causes of the level 1 causes to match the parent cause.  The result is that all sub-causes add up to their parent cause and all CoD data adds up to the all-cause mortality envelope.

This code has been updated for GBD 2016
