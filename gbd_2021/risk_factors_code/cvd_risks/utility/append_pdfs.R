
## Purpose: function to somewhat flexibly append together pdfs in the cluster environment

append_pdfs<-function(folder, pattern, output, rm=F){


files<-list.files(folder, full.names=T)   ##USERNAME:getting all of the files
inputs <- grep(pattern, files, value=T)  ##USERNAME: grepping for the pattern

input<-gsub(",", "", toString(inputs))  ##USERNAME: the ghostscript needs all of the files to look like this

cmd <- paste0("FILEPATH/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=", output, " ", input)
system(cmd)	
if (rm){
  invisible(lapply(inputs, unlink))
  } 
}