import excel "strPath/epi_input_datafile.xlsx", clear sheet("extraction") firstrow
expand 2 if measure=="cfr" & group_review==1, generate(mtexcess)
replace measure="mtexcess" if mtexcess==1
replace mean = -ln(1-cfr)/(30/365)
replace parent_id = row_num if mtexcess==1
replace row_num==. if mtexcess==1
