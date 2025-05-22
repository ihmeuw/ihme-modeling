# Calculate the RRs for bone lead to CVD

# clear memory
rm(list=ls())

#libraries
library(ggplot2)


# values ###########################################
draw_cols <- paste0("draw_", 0:999) #draw columns

  
  type<-"deciles_ed_sbp_adj"
dir<-paste0("FILEPATH/",type,"/FILEPATH")

## Import draws #########
# Grab the outer draws (those include fixed + random effects)
draws<-fread(paste0(dir,"/outer_draws.csv"))

## Extract RR and CI ###########
#Make an empty data table
rr<-data.table(draws)

# Get the RR and CI
rr[, (draw_cols) := lapply(.SD, function(x) exp(x)), .SDcols = draw_cols]
rr[, rr_mean := rowMeans(.SD), .SDcols = draw_cols]
rr[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = draw_cols]
rr[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = draw_cols]
rr[, sd := apply(.SD,1,sd), .SDcols = draw_cols]

#Only keep the columns we want
rr<-rr[,c("risk","rr_mean","rr_lower","rr_upper","sd")]

## Export #################
write.csv(rr,paste0(dir,"/rr_est.csv"))

# Plot ######################
pdf(file=paste0(dir,"/rr_est_graph.pdf"), height = 9, width = 16)
graph<-ggplot(data = rr,aes(x=risk,y=rr_mean))+
  geom_line()+
  geom_point()+
  geom_ribbon(aes(ymin=rr_lower,ymax=rr_upper),alpha=0.2)+
  theme_minimal() + 
  theme(plot.title = element_text(hjust = 0.5))+
  labs(title="Bone lead-CVD RR estimates",
       x="Bone Lead Exposure (ug/g)",
       y="RR value",
       caption = paste0(type," from USERNAME's analysis"))+
  ylim(1,2)

print(graph)
dev.off()













