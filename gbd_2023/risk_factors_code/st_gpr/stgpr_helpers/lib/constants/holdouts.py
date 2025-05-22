"""Constants for holdout generation for cross validation."""

from typing import List

# Hold out 20% of data for validation; use 80% for training set
HOLDOUT_PROPORTION: float = 0.2

# Location level at which holdouts are pulled.
# 3 means holdouts are drawn at the national level
HOLDOUT_LOCATION_LEVEL: int = 3

# USA Race/Ethnicity project selects holdouts at the state-race level that meet the following:
#   Must have at least 100,000 people in the smallest R/E group within the state.
USA_RE_HOLDOUT_LOCATIONS: List[int] = [
    53711,  # Arizona; Hispanic, Any race
    53762,  # Arizona; Non-Hispanic, Other races
    53813,  # Arizona; Non-Hispanic, Black
    53864,  # Arizona; Non-Hispanic, White
    53713,  # California; Hispanic, Any race
    53764,  # California; Non-Hispanic, Other races
    53815,  # California; Non-Hispanic, Black
    53866,  # California; Non-Hispanic, White
    53714,  # Colorado; Hispanic, Any race
    53765,  # Colorado; Non-Hispanic, Other races
    53816,  # Colorado; Non-Hispanic, Black
    53867,  # Colorado; Non-Hispanic, White
    53715,  # Connecticut; Hispanic, Any race
    53766,  # Connecticut; Non-Hispanic, Other races
    53817,  # Connecticut; Non-Hispanic, Black
    53868,  # Connecticut; Non-Hispanic, White
    53718,  # Florida; Hispanic, Any race
    53769,  # Florida; Non-Hispanic, Other races
    53820,  # Florida; Non-Hispanic, Black
    53871,  # Florida; Non-Hispanic, White
    53719,  # Georgia; Hispanic, Any race
    53770,  # Georgia; Non-Hispanic, Other races
    53821,  # Georgia; Non-Hispanic, Black
    53872,  # Georgia; Non-Hispanic, White
    53722,  # Illinois; Hispanic, Any race
    53773,  # Illinois; Non-Hispanic, Other races
    53824,  # Illinois; Non-Hispanic, Black
    53875,  # Illinois; Non-Hispanic, White
    53723,  # Indiana; Hispanic, Any race
    53774,  # Indiana; Non-Hispanic, Other races
    53825,  # Indiana; Non-Hispanic, Black
    53876,  # Indiana; Non-Hispanic, White
    53729,  # Maryland; Hispanic, Any race
    53780,  # Maryland; Non-Hispanic, Other races
    53831,  # Maryland; Non-Hispanic, Black
    53882,  # Maryland; Non-Hispanic, White
    53730,  # Massachusetts; Hispanic, Any race
    53781,  # Massachusetts; Non-Hispanic, Other races
    53832,  # Massachusetts; Non-Hispanic, Black
    53883,  # Massachusetts; Non-Hispanic, White
    53731,  # Michigan; Hispanic, Any race
    53782,  # Michigan; Non-Hispanic, Other races
    53833,  # Michigan; Non-Hispanic, Black
    53884,  # Michigan; Non-Hispanic, White
    53732,  # Minnesota; Hispanic, Any race
    53783,  # Minnesota; Non-Hispanic, Other races
    53834,  # Minnesota; Non-Hispanic, Black
    53885,  # Minnesota; Non-Hispanic, White
    53734,  # Missouri; Hispanic, Any race
    53785,  # Missouri; Non-Hispanic, Other races
    53836,  # Missouri; Non-Hispanic, Black
    53887,  # Missouri; Non-Hispanic, White
    53737,  # Nevada; Hispanic, Any race
    53788,  # Nevada; Non-Hispanic, Other races
    53839,  # Nevada; Non-Hispanic, Black
    53890,  # Nevada; Non-Hispanic, White
    53739,  # New Jersey; Hispanic, Any race
    53790,  # New Jersey; Non-Hispanic, Other races
    53841,  # New Jersey; Non-Hispanic, Black
    53892,  # New Jersey; Non-Hispanic, White
    53741,  # New York; Hispanic, Any race
    53792,  # New York; Non-Hispanic, Other races
    53843,  # New York; Non-Hispanic, Black
    53894,  # New York; Non-Hispanic, White
    53742,  # North Carolina; Hispanic, Any race
    53793,  # North Carolina; Non-Hispanic, Other races
    53844,  # North Carolina; Non-Hispanic, Black
    53895,  # North Carolina; Non-Hispanic, White
    53744,  # Ohio; Hispanic, Any race
    53795,  # Ohio; Non-Hispanic, Other races
    53846,  # Ohio; Non-Hispanic, Black
    53897,  # Ohio; Non-Hispanic, White
    53745,  # Oklahoma; Hispanic, Any race
    53796,  # Oklahoma; Non-Hispanic, Other races
    53847,  # Oklahoma; Non-Hispanic, Black
    53898,  # Oklahoma; Non-Hispanic, White
    53747,  # Pennsylvania; Hispanic, Any race
    53798,  # Pennsylvania; Non-Hispanic, Other races
    53849,  # Pennsylvania; Non-Hispanic, Black
    53900,  # Pennsylvania; Non-Hispanic, White
    53751,  # Tennessee; Hispanic, Any race
    53802,  # Tennessee; Non-Hispanic, Other races
    53853,  # Tennessee; Non-Hispanic, Black
    53904,  # Tennessee; Non-Hispanic, White
    53752,  # Texas; Hispanic, Any race
    53803,  # Texas; Non-Hispanic, Other races
    53854,  # Texas; Non-Hispanic, Black
    53905,  # Texas; Non-Hispanic, White
    53755,  # Virginia; Hispanic, Any race
    53806,  # Virginia; Non-Hispanic, Other races
    53857,  # Virginia; Non-Hispanic, Black
    53908,  # Virginia; Non-Hispanic, White
    53756,  # Washington; Hispanic, Any race
    53807,  # Washington; Non-Hispanic, Other races
    53858,  # Washington; Non-Hispanic, Black
    53909,  # Washington; Non-Hispanic, White
    53758,  # Wisconsin; Hispanic, Any race
    53809,  # Wisconsin; Non-Hispanic, Other races
    53860,  # Wisconsin; Non-Hispanic, Black
    53911,  # Wisconsin; Non-Hispanic, White
]
