# Author: 
# Date: 12/11/17
# Purpose: pass arguments to run DDM
rm(list=ls())
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

before_kids <- 1
ddm <- 1
after_kids <- 1
input_prep <- 1
pre_5q0 <- 1
post_5q0 <- 0
gen_new <- 1

if (pre_5q0 == 1){
	post_5q0 <- 0
}
if (post_5q0 == 1){
	pre_5q0 <- 0
}

qsub("ddm_run", 
	paste0("FILEPATH", "ddm_set_up.r"), 
	pass = list(before_kids, ddm, after_kids, input_prep, pre_5q0, post_5q0, gen_new), 
	slots = 5, 
	mem= 10, 
	submit=T, 
	proj = "proj_mortenvelope", 
	shell = "FILEPATH/r_shell_singularity_3402.sh")

# DONE