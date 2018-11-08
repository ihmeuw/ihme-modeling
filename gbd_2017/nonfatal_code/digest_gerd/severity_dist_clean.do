*Import severity extraction and select variables and rows
import excel "FILEPATH", sheet("Extraction") firstrow cellrange(A1:AU149) clear
keep nid sample_size total_cases Count1 Perc1 Count2 Perc2 Count3 Perc3 Count4 Perc4 Count5 Perc5 Use
drop if nid==.
drop if Use==0

*Calculate counts for each severity if not given as such
replace Count1=total_cases*Perc1 if Count1==. & Perc1!=.
replace Count2=total_cases*Perc2 if Count2==. & Perc2!=.
replace Count3=total_cases*Perc3 if Count3==. & Perc3!=.
replace Count4=total_cases*Perc4 if Count4==. & Perc4!=.
replace Count5=total_cases*Perc5 if Count5==. & Perc5!=.

*Calculate my two categories from each level in extraction
egen c_mild_mod=rowtotal(Count1 Count2 Count3)
egen c_sev_very=rowtotal(Count4 Count5)

*Export desired variables as new sheet
keep nid sample_size total_cases c_mild_mod c_sev_very
export excel "FILEPATH", firstrow(variables) sheet("Mapped", replace)

*Proportions for mild-moderate and severe-very severe and their confidence intervals were then calculated in Excel spreadsheet





