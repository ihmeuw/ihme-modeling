#' Generate ax values by iterating ax values to minimize the difference across ax, dx, etc.
#'
#' See Preston section 3.2 (pg 45) on strategies for choosing ax values
#' With this method, we can estimate ax from dx, but we iterate through dx because it changes when we change ax, so there is a circular relationship.
#'
#' @param dt data.table with variables: ihme_loc_id, year, source_name, sex, age, age_length, dx, ax, qx
#' @param id_vars character vector of id variables that uniquely identify each observation (last one must be age)
#' @param n_iterations numeric, number of iterations to run
#'
#' @return None. Modifies given data.table in-place
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

iterate_ax <- function(dt, id_vars, n_iterations) {
    
    dt <- copy(dt)
    dt[, max_ax_diff := 2]
    iter_num <- 1

    setkeyv(dt, id_vars)
    by_vars <- id_vars[id_vars != "age"]

    dt[, max_age := max(age), by = by_vars]
    holdouts <- data.table()

    while(nrow(dt) > 0 & iter_num < n_iterations) {
        
        print(paste0("Iteration ", iter_num))
        setorderv(dt, id_vars)
        
        ## Estimate ax from dx (Preston pg 45)
        dt[, new_ax := ((-5/24) * shift(dx, 1, type="lag")
                        + 2.5 * dx
                        + (5/24) * shift(dx, 1, type="lead"))
                        / dx]

        ## Reset ax if it goes out of bounds
        dt[new_ax <= 0, new_ax := 0.01]
        dt[new_ax >= 5, new_ax := 4.99]

        ## For those that don't have previous or next age groups, or are just very young, just use the original ax
        ## Limitation: this method relies on the original ax extension for the last age group
        dt[age < 10 | age == max_age, new_ax := ax] 
        
        ## Calculate max difference between ax and new ax
        dt[, diff := abs(new_ax - ax)]
        dt[, max_ax_diff := max(diff), by = by_vars]
        
        ## Replace ax with new_ax
        dt[age >= 10 & age != max_age, ax := new_ax]
        
        ## calculate qx, lx, dx
        dt[, qx := mx_ax_to_qx(mx, ax, age_length)]
        dt[qx > 1, qx := 0.99] ## cap qx
        dt[, ax := mx_qx_to_ax(mx, qx, age_length)]
        qx_to_lx(dt, assert_na = T)
        dt[, dx := lx * qx]

        dt[, new_ax := NULL]

        ## Remove life tables where diff is satisfactorily small already
        holdouts <- rbindlist(list(holdouts, dt[max_ax_diff <= .01]), use.names = T, fill = T)
        dt <- dt[max_ax_diff > .01]

        print(paste0("Number of remaining iteration rows: ", nrow(dt)))
        print(summary(dt$max_ax_diff))

        iter_num <- iter_num + 1
    }

    print(nrow(dt))
    print(nrow(holdouts))

    dt <- rbindlist(list(dt, holdouts), use.names = T, fill = T)

    print("Iterations done")
    return(dt)
}
