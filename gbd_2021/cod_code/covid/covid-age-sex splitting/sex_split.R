#' Sex-split an outcome with the male:female relative-risk of the outcome and 
#' the male population proportion
#' @param outcome_both Count of the outcome
#' @param sex_rr Male:female relative-risk of the outcome
#' @param male_pop_prop Male population proportion
#' @return Outcome for males
sex_split_outcome <- function(outcome_both, sex_rr, male_pop_prop) {
    male_outcome_prop <- sex_rr / (1 + sex_rr)
    male_outcome_prob <- male_pop_prop * male_outcome_prop
    female_outcome_prob <- (1 - male_pop_prop) * (1 - male_outcome_prop)
    total_outcome_prob <- male_outcome_prob + female_outcome_prob
    outcome_male <- outcome_both * male_outcome_prob / total_outcome_prob
    return(outcome_male)
}
