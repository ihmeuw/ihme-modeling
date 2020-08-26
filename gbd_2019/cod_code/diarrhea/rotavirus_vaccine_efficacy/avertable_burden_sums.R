os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

#calculate global burden from location-specific burdens

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

hierarchy <- get_location_hierarchy(location_set_id=22, gbd_round_id=6)

find_locations <- function(path){
  fns0 <- list.files(path,full.names=TRUE)
  fns <- fns0[grep(x=fns0,pattern="_ve")]
  loc_ids <- as.numeric(unlist(sapply(fns,function(fn){
    fn <- unlist(strsplit(fn,split="/"))
    fn <- fn[length(fn)]
    return(unlist(strsplit(fn,split="_ve.csv")[1]))
  })))
  fn_df <- data.frame(filename=fns,loc_id=loc_ids)
  return(fn_df)
}

get_ve <- function(path,location_id=101,year=2019){
  if(location_id==1){
    loc_df <- find_locations(path)
    pop_df <- get_population(age_group_id=1,
                             location_id=loc_df$loc_id,
                             year_id=year,
                             gbd_round_id=6,
                             decomp_step="step4")
    sum_ve <- 0
    sum_pop <- 0
    for(loc in loc_df$loc_id){
      draws <- read.csv(as.character(subset(loc_df,loc_id==loc)$filename))
      non_inds <- draw_inds(draws,"rv_impact")[[1]]
      draws <- subset(draws,year_id==year)[,non_inds]
      pop <- subset(pop_df,location_id==loc)$population
      sum_ve <- sum_ve + pop*draws
      sum_pop <- sum_pop + pop
    }
    ve <- sum_ve/sum_pop
  } else{
    path <- fix_path(path)
    label <- "_ve.csv"
    ve <- read.csv("FILEPATH")
    ve <- subset(ve,year_id==year)
    inds <- draw_inds(ve,"rv_impact")
    non_ve <- ve[,inds[[2]]]
    ve <- ve[,inds[[1]]]
  }
  return(quantile(ve,c(0.025,0.5,0.975)))
}

has_subnats <- function(loc_id){
  return(nrow(subset(hierarchy,level_3==loc_id|level_2==loc_id|level_1==loc_id))>1)
}

agg_current <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "d_current"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}

agg_averted <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "d_averted"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}

agg_avertable <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "d_avertable"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}

nf_agg_current <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "nf_current"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}

nf_agg_averted <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "nf_averted"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}

nf_agg_avertable <- function(path,location_id=1){
  path <- fix_path(path)
  label <- "nf_avertable"
  if(has_subnats(location_id)&location_id!=1){
    f_df <- data.frame(fn=list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))])
    f_df$fn <- as.character(f_df$fn)
    f_df$location_id <- sapply(f_df$fn,function(f0){
      f <- unlist(strsplit(x=f0,"/"))
      f <- unlist(strsplit(f[length(f)],".csv"))
      location_id <- unlist(strsplit(f,"_"))
      location_id <- location_id[length(location_id)-1]
      return(as.numeric(location_id))
    })
    f_df <- merge(f_df,hierarchy[,c("location_id","level_0","level_1","level_2","level_3","level_4","level_5","level_6")])
    loc_id <- location_id
    sub_f_df <- subset(f_df,level_3==loc_id|level_2==loc_id|level_1==loc_id)
    fs <- sub_f_df$fn
  } else if(location_id!=1){
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_",location_id,"_"))]
  } else{
    fs <- list.files(path,full.names=TRUE)[grep(list.files(path),pattern=paste0(label,"_"))]
  }
  sum_draws <- data.frame(val=0:999)
  sum_draws$val <- 0
  for(fp in fs){
    f <- read.csv(fp)
    sum_draws <- sum_draws + f$val
  }
  return(quantile(sum_draws$val,c(0.025,0.5,0.975)))
}
