rm(list=ls())

library(data.table)
library(dplyr)
library(ggplot2)
library(reshape2)
library(shiny)
library(plotly)
library(RColorBrewer)

qual_col_pals = brewer.pal.info[brewer.pal.info$category == 'qual',]
col_vector = unlist(mapply(brewer.pal, qual_col_pals$maxcolors, rownames(qual_col_pals)))
color <- col_vector[48:74]

df <- fread('FILEPATH/hf_inputs.csv')
#df <- fread('hf_inputs.csv')

sex_name <- c('Male', 'Female')
sex_id <- c(1, 2)
sexes <- data.table(sex_id, sex_name)

df <- data.table(merge(data.frame(sexes), data.frame(df)))
df <- df[order(rank(age_group_id))]

sex_choices <- c('Male', 'Female')

server <- function(input, output, session) {
  output$bar <- renderPlotly({
    DF <- df[(location_ascii_name==input$location)&(sex_name==input$sex)]
    val = DF$age_group_name
    cols <- unique(df$age_group_name)
    p <- ggplot(DF, aes(x=val, y=proportion, fill=cause_name, text=paste("cause: ", cause_name, 
                                                                         "\nProportion: ", proportion,
                                                                         "\nStandar Error: (", std_err_adj,")")), heigt = 800, width=800) +
                                                                         #"\nProp UI: (", lower, upper,")")), heigt = 800, width=800) +
      geom_bar(stat="identity", position = "fill") +
      theme(plot.title = element_text(size = rel(2))) +
      theme(plot.margin = unit(c(.3,.3,.3,.3), "cm")) +
      theme(axis.text.x = element_text(angle = 90, hjust = 0, vjust = 0), panel.background=element_blank(),
            axis.line.x = element_line(size = 0.5, linetype = "solid", colour = "black"),
            axis.line.y = element_line(size = 0.5, linetype = "solid", colour = "black")) +
      labs(
        y = "Heart Failure proportions by cause",
        x = "",
        fill = "proportion") +
      scale_fill_manual( values = c("legend", 
                                    "Asbestosis" = color[1],
                                    "Coal workers pneumoconiosis" = color[2],
                                    "Other pneumoconiosis" = color[3],
                                    "Congenital heart anomalies" = color[4],
                                    "Interstitial lung disease and pulmonary sarcoidosis" = color[5],
                                    "Iron-deficiency anemia" = color[6],
                                    "Other neonatal disorders" = color[7],
                                    "Other chronic respiratory diseases" = color[8],
                                    "Iodine deficiency" = color[9],
                                    "Alcoholic cardiomyopathy" = color[10],
                                    "Myocarditis" = color[11],
                                    "Other cardiomyopathy" = color[12],
                                    "Chagas disease" = color[13],
                                    "Thalassemias" = color[14],
                                    "G6PD deficiency" = color[15],
                                    "Other hemoglobinopathies and hemolytic anemias" = color[16],
                                    "Endocrine, metabolic, blood, and immune disorders" = color[17],
                                    "Rheumatic heart disease" = color[18],
                                    "Ischemic heart disease" = color[19],
                                    "Hypertensive heart disease" = color[20],
                                    "Cardiomyopathy and myocarditis" = color[21],
                                    "Endocarditis" = color[22],
                                    "Other cardiovascular and circulatory diseases" = color[23],
                                    "Chronic obstructive pulmonary disease" = color[24],
                                    "Silicosis" = color[25])) +  
      scale_y_continuous(labels = c("0%", "25%", "50%", "75%", "100%"), expand = c(0, 0))+
      #scale_y_continuous(limits = , expand = c(0, 0))+
      scale_x_discrete(limits = cols)
    ggplotly(p, tooltip = c("text"))
  })
  # "Return" data frame
  # build graph with ggplot syntax
  
  output$downloadData <- downloadHandler(
    filename = function() {
      paste("data-", Sys.Date(), ".csv", sep="")
    },
    content = function(file)
      write.csv(dataset(), file)
  )
  
}

ui <- fluidPage(
  headerPanel("Heart Failure Proportions (MarketScan x CoDcorrect, Dismod inputs)"),
  sidebarPanel(
    radioButtons('variable', 'Collapse by:', choices=unique(df$group), selected='Age Group'),
    conditionalPanel(
      condition = "input$variable=='Age Group'",
      selectInput('location', 'Pick a location:', choices=unique(df$location_ascii_name), selected='United States')),
    conditionalPanel(
      condition = "input$variable=='Sex'",
      selectInput('sex', 'Pick a sex:', choices=sex_choices, selected='Female')
    ),
    width = 2
  ),
  mainPanel(
    fluidRow("Bar Chart",
             plotlyOutput('bar', height="1000px")
    )
  )
)
shinyApp(ui, server)