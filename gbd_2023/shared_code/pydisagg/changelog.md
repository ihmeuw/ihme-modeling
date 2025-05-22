## [0.5.1] - 2024-07-09

### Added

- **Count splitting:**
  - `output_type = 'rate'` satisfies the condition that the population weighted mean of the post-split result adds up to the pre-split value.
  - `output_type = 'total'` satisfies the condition that the SUM of the post-split results adds up to the pre-split value.

- **Age splitting columns:**
  - `Population_total`: the sum of age group specific populations across a study.
  - `Population_proportion`: the normalized population in each post-split group.
  - Implication: can multiply `population_proportion` by sample study size to get the post-split pseudo sample sizes.

### Fixed

- Error messaging in sex splitting if a population is missing.

## [0.5.0] - 2024-06-17

### Added
- Sex splitting API, it requires data, ratio pattern, population. It assumes the ratio is female/male (requires draw mean and standard error).
- Propagating Zeros, there is now an optional argument to the end of the split function in AgeSplitter, `propagate_zeros=False` which can now be switched to `True` if there are 0s that you’d like to keep. *WARNING* This changes standard error of the estimate to be 0 as well. Short reason: we assume a binomial distribution that is strictly positive.

### Changes
- Restructured files, which will change import call in python but should not impact R users.
- Age-splitting API now has “Age” appended to the front of the config files for clarity/ consistency (DataConfig -> AgeDataConfig).
- Error messages. Thanks to the feedback from the users we have changed the error messaging to be more succinct.

### Bug-fix
- If a study had an age range fully contained within a single age group pattern it would error with “pattern not found”. This is now rectified and the study should be passed through. Keep in mind that the pre-split and post-split values will be the same.