// Launch DisMod ODE Jobs


clear all
set more off

local runtype `1'
local dismod_type `2'
cd "FILEPATH/`runtype'"

if "`dismod_type'" == "standard" {
	! FILEPATH/sample_post.py
	! FILEPATH/stat_post.py scale_beta=false 
	! FILEPATH/data_pred data_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv data_pred.csv
	! FILEPATH/predict_post.py 10
	! FILEPATH/plot_post.py "Figure"
	! FILEPATH/model_draw draw_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv model_draw2.csv
}

else {
	! FILEPATH/dismod_ode-`dismod_type'/bin/sample_post.py
	! FILEPATH/dismod_ode-`dismod_type'/bin/stat_post.py scale_beta=false 
	! FILEPATH/dismod_ode-`dismod_type'/bin/data_pred data_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv data_pred.csv
	! FILEPATH/dismod_ode-`dismod_type'/bin/predict_post.py 10
	! FILEPATH/dismod_ode-`dismod_type'/bin/plot_post.py "Figure"
	! FILEPATH/dismod_ode-`dismod_type'/bin/model_draw draw_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv model_draw2.csv
}
