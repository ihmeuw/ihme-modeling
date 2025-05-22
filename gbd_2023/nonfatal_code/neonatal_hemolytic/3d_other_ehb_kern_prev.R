source("est_prevs.R")
params_global <- readr::read_rds("params_global.rds")

join_cols <- c(
  "location_id",
  "year_id",
  "age_group_id",
  "sex_id",
  "measure_id",
  "metric_id"
)

read_draws <- function(name, dir_step) {
  data.table::fread(fs::path(dir_step, paste0(name, "_draws.csv"))) |>
    dplyr::select(tidyselect::all_of(join_cols),
                  tidyselect::num_range("draw_", 0:999)) |>
    dplyr::mutate(age_group_id = nch::id_for("age_group", "Birth"))
}

nms <- c("rh_disease", "g6pd", "preterm")
draws_list <- purrr::pmap(
  list(
    name = nms,
    dir_step = fs::path(
      purrr::chuck(params_global, "dir_mnt"),
      c("3a_Rhesus", "3b_G6PD", "3c_Preterm")
    )
  ),
  read_draws
) |>
  purrr::set_names(nms)

draws_list <- purrr::imap(
  draws_list, \(draws, nm) hemo::add_draw_prefix(draws, paste0(nm, "_"))
)

all_draws <- purrr::reduce(
  draws_list,
  \(x, y) dplyr::full_join(
    x,
    y,
    by = join_cols
  )
)

get_other_draws <- function(draws, draw_ids) {
  checkmate::assert_data_table(draws, any.missing = FALSE)
  for (draw_id in draw_ids) {
    data.table::set(
      x = draws,
      j = paste0("rh_disease_draw_", draw_id),
      value = (
        1 - (purrr::chuck(draws, paste0("rh_disease_draw_", draw_id)) +
               purrr::chuck(draws, paste0("g6pd_draw_", draw_id)) +
               purrr::chuck(draws, paste0("preterm_draw_", draw_id)))
      )
    )
  }
  draws |>
    dplyr::select(
      !(tidyselect::starts_with("g6pd_draw_") |
          tidyselect::starts_with("preterm_draw_"))
    ) |>
    hemo::remove_draw_prefix()
}

draws <- get_other_draws(all_draws, 0:999)

name <- "other"
dir_other <- fs::path(purrr::chuck(params_global, "dir_mnt"), "3d_Other")
data.table::fwrite(draws, fs::path(dir_other, paste0(name, "_draws.csv")))

est_prevs(
  name = name,
  directory = dir_other,
  dir_old =
    "FILEPATH",
  # CI below was derived from combined Canada and Denmark population studies
  # (1-7) that specified causes for EHB (not including Rh disease because of
  # effective national Rh prophylaxis programs) and unpublished data that were
  # further provided by study authors Sgro and Ebbesen. This was also cited in
  # Bhutani and colleagues (8).
  # 1. Ebbesen F. Recurrence of kernicterus in term and near-term infants in
  # Denmark. Acta Paediatr 2000;89:1213–7.
  # 2. Ebbesen F, Andersson C, Verder H, et al. Extreme hyperbilirubinaemia in
  # term and near-term infants in Denmark. Acta Paediatr 2005;94:59–64.
  # 3. Ebbesen F, Bjerre JV, Vandborg PK. Relation between serum bilirubin
  # levels =450 μmol/L and bilirubin encephalopathy; a Danish population-based
  # study. Acta Paediatr 2012;101:384–9.
  # 4. Sgro M, Campbell D, Barozzino T, Shah V. Acute neurological findings in a
  # national cohort of neonates with severe neonatal hyperbilirubinemia. J
  # Perinatol 2011;31:392–6.
  # 5. Sgro M, Campbell D, Shah V. Incidence and causes of severe neonatal
  # hyperbilirubinemia in Canada. CMAJ 2006;175:587–90.
  # 6. Sgro M, Campbell DM, Kandasamy S, Shah V. Incidence of chronic bilirubin
  # encephalopathy in Canada, 2007-2008. Pediatrics 2012;130: e886–90.
  # 7. Vandborg PK, Hansen BM, Greisen G, Jepsen M, Ebbesen F. Follow-up of
  # neonates with total serum bilirubin levels = 25mg/dL: a Danish
  # population-based study. Pediatrics 2012;130:61–6.
  # 8. Bhutani VK, Zipursky A, Blencowe H, et al. Neonatal hyperbilirubinemia
  # and Rhesus disease of the newborn: incidence and impairment estimates for
  # 2010 at regional and global levels. Pediatr Res 2013; 74 Suppl 1: 86–100.
  mean = 0.00038,
  lower = 0.00033,
  upper = 0.00163
)
