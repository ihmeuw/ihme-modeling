## Population Level Estimates

This was formerly a set of scripts named roughly "apply envelope" and we still refer to these steps in the Inpatient pipeline as such. However we moved over the application of population for fully covered data and the application of the envelope to this new module.

In short this code is converting inpatient admission **counts** to population level estimate **rates**. I thought about moving this out of /Envelope but the tradition was just too much to overcome

There are currently two different ways to transform data from admissions to population rates
1. Create admission fractions and multiply these by the envelope
    draws (most sources)
2. For fully representative sources, aka full_coverage, divide admit
    counts by GBD population (just England UTLA as of Sept 2019)

Requirements-
- As with almost all other Inpatient code, it writes entirely within a run_id
- behaves differently based on coverage
- inputs are cause fractions or admissions
- outputs population level rate estimate
- retain all draws from HUE (Hospital Utilizaton Envelope)

The sub-package consists of 4 main pieces of code
- `PopEstimates`
    - The Classes are created here to process data both with and without the hospital util. envelope
- `submit_pop_ests`
   - `worker_pop_ests`
         - Submit and worker scripts to instantiate the PopEstimate Classes by age and sex. Submit script deletes any existing parallel outputs when it start, and then confirms that all expected files are 1) present and 2) contain more than 0 bytes
