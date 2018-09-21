clear all
set more off

* Read in the extracted data
import excel using "FILEPATH", clear firstrow

* Transform into log space
    gen logrr = log(mean)
    gen logrrl = log(lower)
    gen logrru = log(upper)
    gen selogrr = (logrru - logrrl) / 3.92

* Run meta-analysis
metaan logrr selogrr, dl exp