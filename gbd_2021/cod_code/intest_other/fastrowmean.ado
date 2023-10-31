/*
Authors:	names
Created:	29 Mar 2014
Purpose:	Create rowmean much faster than egen
How To:		fastrowmean varlist, mean_var_name(string)
			where varlist contains the variables you want to compute mean over (e.g. draw*)
			mean_var_name is the name of your output variable (e.g. (daly_draws_row_mean)

            Note: Ignores missing values
*/

capture program drop fastrowmean
program define fastrowmean

	syntax varlist, mean_var_name(string)
	
	mata fastmean_view=.
	mata st_view(fastmean_view, ., "`varlist'")

	mata `mean_var_name' = rowsum(fastmean_view) :/ rownonmissing(fastmean_view)

	getmata `mean_var_name', double

end



