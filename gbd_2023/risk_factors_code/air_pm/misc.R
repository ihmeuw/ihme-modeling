#----DEPENDENCIES-----------------------------------------------------------------------------------------------------------------
# load packages
require(data.table)

#********************************************************************************************************************************

#********************************************************************************************************************************
# find and replace values in your data (also gives you the option to change the variable name, or to expand the rows by specifying more than one replacement for a single input)
findAndReplace <- function(table, #data table that you want to replace values in
                           input.vector, # vector of the old values you want to replace
                           output.vector, # vector of the new values you want to replace them with
                           input.variable.name, # the current variable name
                           output.variable.name, # new variable name (same as previous if no change required)
                           expand.option=FALSE) { # set this option TRUE if you are doing a one:many replace (IE expanding rows)

  values <- input.vector
  new.values <- output.vector

  # Replacement data table
  replacement.table = data.table(variable = values, new.variable = new.values, key = "variable")

  # Replace the values (note the data table must be keyed on the value to replace)
  setkeyv(table, input.variable.name) #need to use setkeyv (verbose) in order to pass the varname this way
  table <- setnames(
    replacement.table[table, allow.cartesian=expand.option][is.na(new.variable), new.variable := variable][,variable := NULL],
    'new.variable',
    output.variable.name)

  return(table)

}
#********************************************************************************************************************************

# a function made by hadley wickham to steal the legend from a plot
# used for gridExtra graphing where you want multiplot with just 1 shared legend
gLegend<-function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}
#********************************************************************************************************************************

#********************************************************************************************************************************
# proper cap, used for titles
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
        sep="", collapse=" ")
}
#********************************************************************************************************************************
checkYourself <- function() {

  for (thing in ls()) {
    message(thing);
    print(object.size(get(thing)), units='auto')
  }

}
#----DRAW TRACKING------------------------------------------------------------------------------------------------------
#a little extra trick! (or treat)

#sometimes working with draws can cause cognitive overload if you try to follow a process through several steps
#you create 1000 variables at each step, trying to view the dt can a) take forever to print, b) be hard to read
#using what we know about how DT syntax allows you to index/subset based on cols
#and the vectors of draw cols we have already created, is it possible to:
#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {

  varlist[-random.draws] %>% return

}

#example:
#subtract <- sapply(list(prev.cols, cig.cols, smoker.cols, cig.ps.cols, pm.cols),
                   #drawTracker,
                   #random.draws=sample(draws.required, 3))

#head(all.data[, -(subtract), with=F])
#********************************************************************************************************************************