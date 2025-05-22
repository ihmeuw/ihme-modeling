#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Read in hdf5 files
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(rhdf5, data.table)
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### reading .h5 files
read_block_names <- function(str){
  # Use string splitting to get the column names of a value block
  split_vec <- strsplit(str, "'")[[1]]
  N <- length(split_vec)
  split_vec[seq(from=2, to=N, by=2)]
}
value_blocks <- function(attrib){
  # get all the value block names 
  grep(glob2rx("values_block_*_kind"), names(attrib), value=TRUE)
}
h5_value_col_names <- function(h5File, key){
  # return a list where each element are the column names 
  attrib <- h5readAttributes(h5File, paste0(key, "/table/"))
  value_block_names <- value_blocks(attrib)
  nam <- lapply(value_block_names, function(x) read_block_names(attrib[[x]]))
  names(nam) <- gsub("_kind", "", value_block_names)
  nam
}
mat_to_df <- function(mat, mat_names){
  # pytables hdf5 saves values as matrices when more than one column exists 
  if (length(dim(mat)) == 1){
    mat <- matrix(data=mat, nrow=length(mat), ncol=length(mat_names))
  }
  else{
    mat <- t(mat)
  }
  df <- as.data.frame(mat)
  names(df) <- mat_names
  df
}
read_hdf5_table <- function(h5File, key, is.data.table=TRUE){
  # read in the indices  values as well as the value blocks
  data_list <- h5read(h5File, paste0(key, "/table/"), compoundAsDataFrame=F)
  data_value_names <- h5_value_col_names(h5File, key)
  indices <- setdiff(names(data_list), c("index", names(data_value_names)))
  df_index <- data.frame(data_list[indices])
  df_values <- lapply(names(data_value_names), function(x) 
    mat_to_df(data_list[[x]], data_value_names[[x]]))
  if (is.data.table) { do.call(cbind, c(list(df_index), df_values)) %>% as.data.table 
    } else { do.call(cbind, c(list(df_index), df_values)) }
}
#***********************************************************************************************************************