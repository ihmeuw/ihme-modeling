make_line <- function(s){
  sp <- strsplit(s,split="\n")[[1]]
  sp <- sp[which(sp!="#N/A")]
  out <- paste(sp,collapse=" ")
  writeClipboard(out)
  return(out)
}