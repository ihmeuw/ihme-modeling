
# update NID values -------------------------------------------------------

update_nid <- function(u_nid, nid){
  i_vec <- which(is.na(nid) | nid == "")
  nid[i_vec] <- u_nid[i_vec]
  
  i_vec <- which(nid == 146860)
  nid[i_vec] <- 210231
  
  return(nid)
}
