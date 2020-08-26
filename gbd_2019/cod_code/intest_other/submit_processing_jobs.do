local cause_id 321
local location 148
local inflator_draw_stub 0
local modelDir "/filepath/temp"
local minAge 4
local link "logit"
local random "no"

! qsub -N customCod_`cause_id'_`location' -l fthread=2 -l m_mem_free=8G -q all.q -l archive=TRUE -e "/filepath/errors/" -P ihme_general "/filepath/submit_prediction_processor.sh" "`cause_id'" "`location'" "`link'" "`min_age'" "`random'" "`=subinstr("`saving_cause_ids'", " ", "_", .)'" "`modelDir'" "0" "`inflator_draw_stub'"
