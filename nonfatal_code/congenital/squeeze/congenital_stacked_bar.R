##########################################################
# Author: {AUTHOR NAME}
# Date: 6 April 
# Description: Stacked bar graphs of GBD 2016 congenital causes,
#              comparision between squeezed and unsqueezed models,
#              level 2 locations
##########################################################

#############################
# DEFINE LIBRARIES (INSTALL NEW VERSIONS IF NEEDED)
#############################
require(ggplot2)
require(stringr)
require(scales)
require(RColorBrewer)
require(data.table)
require(foreign)
require(grid)
require(gridExtra)
require(plyr)

#############################
# DEFINE PARAMETERS
#############################
if (Sys.info()["sysname"] == "Linux") d <- "{FILEPATH}"
if (Sys.info()["sysname"] == "Windows") d <- "{FILEPATH}"

#check if output directory with today's date exists and, if not, create it to organize visualizations
mainDir <- paste0(d,"{FILEPATH}")
subDir <- Sys.Date()
out_dir <- file.path(mainDir, subDir)

ifelse(!dir.exists(out_dir), dir.create(out_dir), FALSE)


# Parse arguments
print(commandArgs())
cause_name <- as.character(commandArgs()[3])
year <- as.integer(commandArgs()[4])

# #############################
# # GET DATA
# #############################

# read in files 
temp_dir <- sprintf(paste0(getwd(),"/../%s/graphs/"),cause_name)
df <- fread(sprintf("%s%s_[%d].csv", temp_dir, cause_name, year))
loc_df <- fread(paste0(temp_dir, "location_metadata.csv"))

# convert to dataframes
df <- data.frame(df)
loc_df <- data.frame(loc_df)

#drop envelope
df <- df[!(df$description == 'env'),]

# change state of env if you want to graph the total/envelope along side raw and sqzd
#df$state[df$description == 'env'] <- "env"

#merge in average draw data with location metadata
df <- merge(x=df, y=loc_df, by='location_id', all.x = TRUE)


sexes <- c(
  '1' = 'Male',
  '2' = 'Female'
)

# grab level 2 location_ids and location_names for x-axis label creation
# convert everything to characters
lvl2 <- loc_df[(loc_df$level == 2), c('location_id', 'location_name')]
lvl2$location_id <- as.character(lvl2$location_id)

# automate vector creation for x axis labels
# the following code creates something like this:
# region_names <- c(
# '5' = 'East Asia',
# '9' = 'Southeast Asia',
# '21' = 'Oceania',
# '32' = 'Central Asia',
# etc...
#)
# and allows us to change the string wrapping as needed to make the x axis labels look nice

list_length = length(lvl2$location_id)
region_names <- character(length(lvl2$location_id))
names(region_names) = lvl2$location_id
lvl2$location_name = str_wrap(lvl2$location_name, width = 15)
for ( i in 1:list_length) {
  region_names[[i]] <- lvl2$location_name[i]
}


# Red - cong_heart
# Critical malformations of great vessels, congenital valvular heart disease and patent ductus arteriosis" : rgb(254,224,210)
# Ventricular septal defect and atrial septal defect : rgb(252, 187, 161)
# Single ventricle and single ventricle pathway heart defects : rgb(252, 146, 114)
# Severe congenital heart defects excluding single ventricle and single ventricle pathway : rgb(251, 106, 74)
# Other cong_heart : rgb(239, 59, 44)
# Not used1 : rgb(203, 24, 29)
# Not used2 : rgb(153, 0, 13)

# Blue - cong_neural
# Spina Bifida : rgb(198, 219, 239)
# Anencephaly : rgb(158, 202, 225)
# Encephalocele : rgb(107, 174, 214)
# Not used3 : rgb(49, 130, 189)
# Not used4 : rgb(8, 81, 156)

# Purple - cong_msk
# Limb reduction deficits : rgb(218, 218, 235)
# Polydactyly and syndactyly : rgb(188, 189, 220)
# Other cong_msk : rgb(158, 154, 200)
# Not used5 : rgb(117, 107, 177)
# Not used6 : rgb(84, 39, 143);

# Green - cong_digestive
# Congenital diaphragmatic hernia : rgb(186, 228, 179);
# Congenital atresia and/or stenosis of the digestive tract : rgb(116, 196, 118)
# Congenital malformations of the abdominal wall : rgb(49, 163, 84)
# Other cong_digestive : rgb(0, 109, 44)

cause_colors <- c('Critical malformations of great vessels, congenital valvular heart disease and patent ductus arteriosis' =  rgb(254,224,210, maxColorValue=255),
                  "Ventricular septal defect and atrial septal defect" = rgb(252, 187, 161, maxColorValue=255),
                  "Single ventricle and single ventricle pathway heart defects" = rgb(252, 146, 114, maxColorValue=255),
                  "Severe congenital heart defects excluding single ventricle and single ventricle pathway" = rgb(251, 106, 74, maxColorValue=255),
                  "Other cong_heart" = rgb(239, 59, 44, maxColorValue=255),
                  "Not used1" = rgb(203, 24, 29, maxColorValue=255),
                  "Not used2" = rgb(153, 0, 13, maxColorValue=255),
                  'Spina Bifida' = rgb(198, 219, 239, maxColorValue=255),
                  'Anencephaly' = rgb(158, 202, 225, maxColorValue=255),
                  'Encephalocele' = rgb(107, 174, 214, maxColorValue=255),
                  'Not used3' = rgb(49, 130, 189, maxColorValue=255),
                  'Not used4' = rgb(8, 81, 156, maxColorValue=255),
                  "Limb reduction deficits" = rgb(218, 218, 235, maxColorValue=255),
                  "Polydactyly and syndactyly" = rgb(188, 189, 220, maxColorValue=255),
                  "Other cong_msk" = rgb(158, 154, 200, maxColorValue=255),
                  "Not used5" = rgb(117, 107, 177, maxColorValue=255),
                  "Not used6" = rgb(84, 39, 143, maxColorValue=255),
                  'Congenital diaphragmatic hernia' = rgb(186, 228, 179, maxColorValue=255),
                  'Congenital atresia and/or stenosis of the digestive tract' = rgb(116, 196, 118, maxColorValue=255),
                  'Congenital malformations of the abdominal wall' = rgb(49, 163, 84, maxColorValue=255),
                  'Other cong_digestive' = rgb(0, 109, 44, maxColorValue=255))

# average, ave draws by region_id
df <- data.table(df)
df <- df[, list(ave = mean(ave)), by='sex_id,age_group_id,year_id,description,state,region_id']
df <- data.frame(df)

#find max value of raw and sqzd data so y-axis can be fixed for ease of comparison across age groups
df_max <- data.table(df)
df_max <- df_max[, list(ave = sum(ave)), by='sex_id,age_group_id,state,region_id']
df_max <- data.frame(df_max)
max_value <- max(df_max$ave)

# wrap description text and related cause_colors palette as needed to make legend look nice
df$description = str_wrap(df$description, width = 50)
names(cause_colors) = str_wrap(names(cause_colors), width = 50)

# turn pdf devide on, give it a file path, and declare width and height of final printed pages
pdf(sprintf("%s/%s_%d.pdf", out_dir, cause_name, year), width=30, height=13) #, width=40, height=13

#loop and filter by age group, drop uneeded columns
#for (age_group in c(sort(unique(df$age_group_id)))) {
for (age_group in c(164, 2, 3, 4, 6, 13)) {
  df_age <- df[(df$age_group_id == age_group), c("sex_id", "age_group_id", "year_id", "description", "state", "region_id", "ave")]
  
  #create separate df with sqzd data only so area can be calculated for black outline
  dfsqzd <- df_age[df_age$state != 'raw',]
  
  #Calculate size of stacked sqzd data for black outline
  dfsqzd <- data.table(dfsqzd)
  dfsqzd <- dfsqzd[, list(ave = sum(ave)), by='sex_id,state,region_id']
  dfsqzd <- data.frame(dfsqzd)
  
  cong_stacked <- ggplot(df_age, aes(x=factor(state),
                          y=ave)) +
     scale_fill_manual(values=cause_colors) +
     geom_bar(data=df_age,
              stat='identity',
              width=.7,
              aes(fill=factor(description, levels=names(cause_colors)))) +
     geom_bar(data=dfsqzd,
              stat='identity',
              color='black',
              size=.1,
              width=.7,
              fill=NA) +
     guides(fill = guide_legend(ncol = 1)) +
     scale_y_continuous(labels = comma, expand=c(0,0), limits =c(0, max_value)) + 
     facet_grid(sex_id ~ region_id, switch = 'x', labeller = labeller(sex_id=sexes,region_id=region_names)) +
     theme_bw() +
     theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 10),
           axis.text.y = element_text(size = 15),
           axis.title = element_text(size = 15),
           legend.title = element_text(face = "bold", size = 15),
           legend.text = element_text(size = 15),
           plot.title = element_text(size = 25, face = "bold"),
           axis.line = element_line(colour="black"),
           strip.text.y = element_text(size = 25),
           strip.background = element_blank(),
           panel.grid.major = element_blank(),
           panel.grid.minor = element_blank(),
           panel.border = element_blank(),
           #space between location groups and male and female plots
           panel.spacing = unit(1, "lines")) +
     labs(title =sprintf("Average %s disease prevalence by GBD region, %d, age group: %d", cause_name, year, age_group),
          x = "Location (GBD Regions)",
          y = "Prevalence",
          fill = "Congenital Causes") +
     theme(legend.key.height = unit(.9, "cm"),
           legend.key.width = unit(.9, "cm"))
  
   plot(cong_stacked)
}
# shut down the pdf device
# all pages will be saved to file path given when device was first turned on
dev.off()