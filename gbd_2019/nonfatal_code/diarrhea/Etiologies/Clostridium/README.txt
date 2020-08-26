Clostridium difficile modeling is relatively straightforward. 

* cdiff_crosswalks_decomp2.R: Runs the crosswalks for literature, clinical data and uploads
those data to the Epi database. 

*******************************************
** Run a DisMod full compartmental model **
*******************************************

*00_launch_clostridium.R: This is a file that launches the attributable fraction
code that is performed in parallel by location_id

*01_clostridium_codcorrect.R: This is the script that runs in parallel to produces
draw files for the attributable fraction of Clostridium difficile.

