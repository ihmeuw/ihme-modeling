## Imported Cases Generator
Integral to the Causes of Death modeling pipeline, the Imported Cases Generator should be run before the final CoDCorrect.

### Introduction
Imported cases allow us to count deaths in countries that would otherwise be restricted to the cause of death.

For instance, the class example, a British citizen travels to Africa and contracts malaria. They return to England and pass away from this disease. We want to count this person's passing, but we also want to recognize that malaria cannot be contracted in England.

Imported cases are simple CoD models that are appended onto the final CoDCorrect draws and as such, the Imported Cases Generator is a very simple machine.


### Process
1. Backfill location/years for new round in codcorrect.spacetime_restrictions table.
2. Check the filesystem under /ihme/centralcomp/imported_cases/ for the next version number. (There is no database versioning at this time.)
3. Increment the version and run the `launch.py` script.
 

The Imported Cases Generator will loop through the cause_ids that are considered restricted (returned from the codcorrect database's spacetime_restriction table).

For each cause, we:
1. read in the raw VR data via `get_cod_data`.
2. generate a distribution (to represent 1000 draws).
3. make the data square
4. call `save_results_cod` to save these as model_version_type_id 7 (imported_cases).


### Software Structure

`launch.py` - is the main script to call<br>
`imported_cases/core.py` - has many of the database calls and helper functions that make generating the data possible.<br>
`imported_cases.py` - the individual script that is run on each cause id.<br>
`imported_cases/job_swarm.py` - the jobmon implementation (will need to be updated for round 6)<br>


Happy importing!