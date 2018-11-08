This folder contains code to do the following:

1. take the proportion of anemia attributable to various subtypes (sickle cell, iron etc.)
2. take the proportion of anemia attributable to mild, moderate and severe anemia
3. take some expert priors about which way each subtype should lean (in terms of severity)
4. estimate anemia by subtype AND severity, in a way that respects the three inputs above

A working paper describing the exact methods can be found here:

"FILEPATH"

In short, it uses linear optimization to maximize the posterior probability by operating on two rows at a time so that the margins match but the cells can vary. A bunch of penalties are used to define the boundaries of the search space, and two log-likelihood functions (one for rows, one for columns) are used to guide the search.

The code outputs draw-specific files in a format desirable to the GBD database.