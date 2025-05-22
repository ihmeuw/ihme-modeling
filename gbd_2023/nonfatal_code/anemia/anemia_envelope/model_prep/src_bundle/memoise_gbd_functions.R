get_model_results <- memoise::memoise(
  ihme::get_model_results,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)

get_age_metadata <- memoise::memoise(
  ihme::get_age_metadata,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)

get_demographics <- memoise::memoise(
  ihme::get_demographics,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)

get_location_metadata <- memoise::memoise(
  ihme::get_location_metadata,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)

get_population <- memoise::memoise(
  ihme::get_population,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)