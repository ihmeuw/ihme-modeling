** Interpolate odds ratios for Influenza and RSV

insheet using "filepath", comma names clear

egen panel = group(lri)
tsset panel age
tsfill, full

foreach metric in "mean" "lower" "upper" {
	bysort panel: ipolate `metric' age, gen(`metric'_new) epolate
}

bysort panel: replace lri = lri[_n-1] if lri==""

drop panel

outsheet using "filepath", comma names replace
