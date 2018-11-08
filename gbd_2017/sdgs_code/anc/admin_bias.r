###########################################################
### Project: HSA (maternal)
### Purpose: Administrative bias adjustment
###########################################################


###########################################################################################################################
# Calculate out administrative bias
###########################################################################################################################

## For each country/vaccine, run a individual regressions to estimate
## a country/vaccine shift, running a spline model + a dummy term for if the data
## is admin or not

calc.shift <- function(df, me, loc, graph=FALSE) {
  ## Subset
  df <- df[me_name==me & ihme_loc_id==loc]
  ## If any admin data
  if (nrow(df[cv_admin==1])>1) {
  #------------------------------
    ## Set knots based on how many data points there are
    n <- nrow(df[!is.na(data) & cv_admin == 0])
    n.knots <- ifelse(n < 5, 2, ifelse(n >=5 & n < 10, 3, 5))
    ## Formula
    formula <- as.formula(paste0("logit(data) ~ cv_admin + ns(year_id, knots=", n.knots, ")"))
    mod <- lm(formula, data=df)
    ## Extract shift
    ## Set cv_admin = 0 and predict
    df <- df[cv_admin==1, admin := 1]
    df <- df[, cv_admin := 0]
    df <- df[, predict := predict(mod, newdata=df) %>% inv.logit]
    ## Calculate mean shift as the administrative bias
    df <- df[admin==1, shift := mean(predict-data, na.rm=TRUE)]
    shift <- unique(df[admin==1]$shift)
    ## Graph
    if (graph) {
      df <- df[, year_id := as.numeric(as.character(year_id))]
      df <- df[order(year_id)]
      df <- df[admin==1, newadmin := data + shift]
      df <- df[, newadmin := ifelse(newadmin <0, 0, newadmin)]
      p <- ggplot(df) +
        geom_point(aes(y=data, x=year_id, color='Survey')) +
        geom_point(data=df[admin==1], aes(y=data, x=year_id, color='Original Admin')) +
        geom_point(data=df[admin==1], aes(y=newadmin, x=year_id, color='Adjusted Admin'), shape=2) +
        geom_ribbon(data=df[admin==1], aes(ymax=data, ymin=newadmin, x=year_id), alpha=0.1) +
        scale_color_manual(values= c("Survey"="Black", "Original Admin"="Red", "Adjusted Admin"="Blue")) +
        ylab("Coverage") + xlab("Year") +
        theme_bw()+
        theme(axis.title=element_text(),
              plot.title=element_text(size=10),
              strip.text=element_text(size=12, face ="bold"),
              strip.background=element_blank(),
              axis.text.x = element_text(size = 9),
              legend.position = "right",
              legend.title = element_blank(),
              legend.background = element_blank(),
              legend.key = element_blank()
        ) +
        ggtitle(loc)
      print(p)
    } else {
      return(list(ihme_loc_id=loc, me_name=me, cv_admin_bias=shift))
    }
  #-------------------------------
  } else {
    return(list(ihme_loc_id=loc, me_name=me, cv_admin_bias=0))
  }
}


