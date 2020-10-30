#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
# Built with the help of Garret Grolemund's tutorial: https://shiny.rstudio.com/tutorial/

# Loading screen built with Dean Attali's code: https://github.com/daattali/advanced-shiny/tree/master/loading-screen

library(shiny)
library(shinyjs)
library(ggplot2)
library(wesanderson)
library(shinythemes)
library(tidyr)
library(dplyr)
library(stringr)
library(DT)
library(aws.s3)


# CSS is for loading page
appCSS <- "
#loading-content {
  color: black;
  position: absolute;
  background: white;
  opacity: 1;
  z-index: 100;
  left: 0;
  right: 0;
  height: 100%;
  text-align: center;
}
"
# This is to allow NAs to appear in datatables
# see https://stackoverflow.com/a/58526580/5593458
rowCallback <- c(
  "function(row, data){",
  "  for(var i=0; i<data.length; i++){",
  "    if(data[i] === null){",
  "      $('td:eq('+i+')', row).html('NA')",
  "        .css({'color': 'rgb(151,151,151)', 'font-style': 'italic'});",
  "    }",
  "  }",
  "}"
)

s3BucketName <- "erate-data"

# object <- get_object("data/Libraries_Funded_Committed.csv", s3BucketName, "AWS_ACCESS_KEY_ID" = Sys.getenv("AWS_ACCESS_KEY_ID"),
#                      "AWS_SECRET_ACCESS_KEY" = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
# object_data <- readBin(object, "character")
# libs_funded <- read.csv(text = object_data, stringsAsFactors = FALSE)

object2 <-
  get_object(
    "data/erate_imls_compact.csv",
    s3BucketName,
    "AWS_ACCESS_KEY_ID" = Sys.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY" = Sys.getenv("AWS_SECRET_ACCESS_KEY")
  )
object_data2 <- readBin(object2, "character")
erate_imls <-
  read.csv(text = object_data2, stringsAsFactors = FALSE)

# object3 <- get_object("data/USAC_Libs_Not_Matched_to_IMLS.csv", s3BucketName, "AWS_ACCESS_KEY_ID" = Sys.getenv("AWS_ACCESS_KEY_ID"),
#                       "AWS_SECRET_ACCESS_KEY" = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
# object_data3 <- readBin(object3, "character")
# erate_nonmatches <- read.csv(text = object_data3, stringsAsFactors = FALSE)

object4 <-
  get_object(
    "data/USAC_IMLS_MATCHED.csv",
    s3BucketName,
    "AWS_ACCESS_KEY_ID" = Sys.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY" = Sys.getenv("AWS_SECRET_ACCESS_KEY")
  )
object_data4 <- readBin(object4, "character")
rosetta_stone <-
  read.csv(text = object_data4, stringsAsFactors = FALSE)

libs_funded <- erate_imls %>%
  select(-X)

# Define UI
# Wrapping the navbarPage in tagList() allows use of shinyjs see
# https://cran.r-project.org/web/packages/shinyjs/vignettes/shinyjs-usage.html
ui <- tagList(
  # shinyjs and inlineCSS for loading page
  useShinyjs(),
  inlineCSS(appCSS),
  # Loading message
  div(
    id = "loading-content",
    br(),
    br(),
    h2(
      "Thank you for your patience while we perform some calculations and load the data..."
    )
  ),
  navbarPage(
    "Libraries and E-Rate",
    collapsible = T,
    theme = shinytheme("spacelab"),
    #shinythemes::themeSelector(), #use this code to choose a theme
    tabPanel(
      "State View",
      
      fluidRow(column(12,
                      h3(
                        "Yearly Summaries by State"
                      ),
                      br())),
      # Sidebar with a dropdown menu for year
      fluidRow(column(3,
                      wellPanel(
                        selectInput(
                          inputId = "StateYearDrop",
                          label = "Choose a Year",
                          choices = sort(unique(libs_funded$funding_year)),
                          selected = "2020"
                        )
                      )),
               column(
                 9,
                 DT::dataTableOutput(outputId = "State_Year_Sums")
               )),
      
      fluidRow(column(
        12,
        h3("Participation by Entity Per Year"),
        br()
      )),
      
      fluidRow(column(3,
                      wellPanel(
                        selectInput(
                          inputId = "StateDrop",
                          label = "Choose a State",
                          choices = sort(unique(libs_funded$ros_physical_state))
                        )
                      )),
               column(
                 9,
                 DT::dataTableOutput(outputId = "State_Participation_Yearly")
               )),
      
      fluidRow(column(
        12,
        h3("About the data"),
        p(
          "We gather E-Rate data every weekday from the ",
          tags$a(href = 'https://opendata.usac.org/E-rate/E-rate-Recipient-Details-And-Commitments/avi8-svp9', 'E-Rate Recipient Details and Commitments'),
          " table on ",
          tags$a(href = 'https://opendata.usac.org/', "USAC's Open Data Portal,"),
          " filter it to include only library data, and calculate the estimated commitments per entity. Any sums at the entity-level should be considered estimates. Please feel free to",
          tags$a(href = "mailto:chrisjow@uw.edu", 'email'),
          "us with questions."
        )
      ))
    ),
    tabPanel(
      "National View",
      fluidRow(column(12,
                      h3("National Summary"),
                      br())),
      fluidRow(column(
        12,
        DT::dataTableOutput(outputId = "Nat_Year_Sums"),
        br()
      )),
      
      fluidRow(column(
        12,
        h3("National Yearly Summary by Service Type"),
        br()
      )),
      
      fluidRow(column(
        12,
        DT::dataTableOutput(outputId = "Nat_Year_Sums_Types"),
        br()
      )),
      
      
      fluidRow(column(
        12,
        h3("About the data"),
        p(
          "We gather E-Rate data every weekday from the ",
          tags$a(href = 'https://opendata.usac.org/E-rate/E-rate-Recipient-Details-And-Commitments/avi8-svp9', 'E-Rate Recipient Details and Commitments'),
          " table on ",
          tags$a(href = 'https://opendata.usac.org/', "USAC's Open Data Portal,"),
          " filter it to include only library data, and calculate the estimated commitments per entity. Any sums at the entity-level should be considered estimates. Please feel free to",
          tags$a(href = "mailto:chrisjow@uw.edu", 'email'),
          "us with questions."
        )
      ))
    ),
    tabPanel(
      "Congressional District View",
      fluidRow(column(
        12,
        h3("Congressional District Commitment Totals"),
        br()
      )),
      fluidRow(column(2,
                      wellPanel(
                        selectInput(
                          inputId = "StateDrop2",
                          label = "Choose a State",
                          choices = sort(unique(libs_funded$ros_physical_state))
                        )
                      )),
               column(
                 10,
                 DT::dataTableOutput(outputId = "Cong_Year_Sums")
               )),
      fluidRow(column(
        12,
        h3("About the data"),
        p(
          "We gather E-Rate data every weekday from the ",
          tags$a(href = 'https://opendata.usac.org/E-rate/E-rate-Recipient-Details-And-Commitments/avi8-svp9', 'E-Rate Recipient Details and Commitments'),
          " table on ",
          tags$a(href = 'https://opendata.usac.org/', "USAC's Open Data Portal,"),
          " filter it to include only library data, and calculate the estimated commitments per entity. Any sums at the entity-level should be considered estimates. Please feel free to",
          tags$a(href = "mailto:chrisjow@uw.edu", 'email'),
          "us with questions."
        )
      ))
    ),
    tabPanel(
      "Recipient View",
      fluidRow(column(12,
                      h3(
                        "Individual Recipients"
                      ),
                      br())),
      # Sidebar with a dropdown menu for year
      fluidRow(column(
        4,
        wellPanel(
          # Search or Drop Down Box
          selectizeInput(
            inputId = "EntitySelect",
            label = "1. Find an Entity",
            choices =
              libs_funded %>%
              mutate(
                ros_entity_name = str_to_upper(ros_entity_name),
                ros_physical_city = str_to_upper(ros_physical_city)
              ) %>%
              tidyr::unite(
                col = "lib_city_state",
                c(ros_entity_name, ros_physical_city, ros_physical_state),
                sep = ", ",
                remove = FALSE
              ) %>%
              distinct(lib_city_state) %>%
              arrange(lib_city_state)
            ,
            options = list(
              placeholder = 'Begin Typing',
              onInitialize = I('function() { this.setValue(""); }')
            )
          ),
          p(tags$b("2. Download Entity Data"),
            "(optional)"),
          # Button
          downloadButton("downloadData", "Download (.csv)")
        )
      ),
      column(
        8,
        DT::dataTableOutput(outputId = "Recipient_Sum")
      )),
      
      fluidRow(column(
        12,
        h3("Per Recipient Funding Averages"),
        br()
      )),
      
      fluidRow(column(12,
                      wellPanel(
                        plotOutput(outputId = "Funding_by_Org_Plot"),
                      ))),
      
      fluidRow(column(
        12,
        h3("About the data"),
        p(
          "We gather E-Rate data every weekday from the ",
          tags$a(href = 'https://opendata.usac.org/E-rate/E-rate-Recipient-Details-And-Commitments/avi8-svp9', 'E-Rate Recipient Details and Commitments'),
          " table on ",
          tags$a(href = 'https://opendata.usac.org/', "USAC's Open Data Portal,"),
          " filter it to include only library data, and calculate the estimated commitments per entity. Any sums at the entity-level should be considered estimates. Please feel free to",
          tags$a(href = "mailto:chrisjow@uw.edu", 'email'),
          "us with questions."
        )
      ))
    ),
    tabPanel(
      "IMLS PLS View",
      fluidRow(column(
        12,
        h3("Merging E-Rate Data and IMLS PLS Data"),
        p(
          "This page is a work-in-progress. We intend to bring you insights from our work merging the E-Rate data with the Institute for Museum and Library Services (IMLS) Public Library Survey (PLS)",
          tags$a(href = 'https://www.imls.gov/research-evaluation/data-collection/public-libraries-survey', 'data.'),
          " (We are currently using 2018 IMLS PLS data.) There are no perfectly matching columns between the two datasets so we have designed a methodology that uses both geolocation matching and character matching to combine the data. It is not perfect but we think it's a really good start. While you are waiting for new graphs and charts to appear on this page, perhaps you would like to peruse our 'decoder' dataset. Below you can download the matches we have made between E-Rate and IMLS data. Enjoy!"
        )
      )),
      fluidRow(column(
        6,
        p(tags$b("Download E-Rate Entities and IMLS Matches")),
        # Download Button
        downloadButton("downloadIMLSMatches", "Download (.csv)"),
        br(),
        br(),
        br()
      )),
      fluidRow(column(6,
                      wellPanel(
                        DT::dataTableOutput(outputId = "City_Size_Table")
                      )),
               column(6,
                      wellPanel(
                        plotOutput(outputId = "City_Size_Plot")
                      ))),
      fluidRow(column(
        12,
        h3("About the data"),
        p(
          "We gather E-Rate data every weekday from the ",
          tags$a(href = 'https://opendata.usac.org/E-rate/E-rate-Recipient-Details-And-Commitments/avi8-svp9', 'E-Rate Recipient Details and Commitments'),
          " table on ",
          tags$a(href = 'https://opendata.usac.org/', "USAC's Open Data Portal,"),
          " filter it to include only library data, and calculate the estimated commitments per entity. Any sums at the entity-level should be considered estimates. Please feel free to",
          tags$a(href = "mailto:chrisjow@uw.edu", 'email'),
          "us with questions."
        )
      )),
      hidden(div(id = "app-content"))
    )
  )
)

# Define server logic required to make tables and visualizations
server <- function(input, output) {
  # Simulate work being done for 1 second
  #Sys.sleep(15)
  
  # Hide the loading message when the rest of the server function has executed
  hide(id = "loading-content",
       anim = TRUE,
       animType = "fade")
  show("app-content")
  
  year_pick <- reactive({
    input$StateYearDrop
  })
  
  output$State_Year_Sums <- DT::renderDataTable({
    libs_funded_table <- libs_funded %>%
      select(
        ros_physical_state,
        funding_year,
        chosen_category_of_service,
        ros_allocation,
        discount_by_ros
      ) %>%
      filter(funding_year == year_pick()) %>%
      group_by(ros_physical_state) %>%
      summarise(
        Category1_Commitment = sum(discount_by_ros, na.rm = T),
        Category2_Commitment = sum(ros_allocation, na.rm = T),
        State_Total = Category1_Commitment + Category2_Commitment
      ) %>%
      mutate(Pct_of_Lib_Funding = State_Total / sum(State_Total))
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      libs_funded_table,
      caption = 'E-Rate commitments made to Libraries and Library Systems including those applying as part of a Consortium or as a Non-Instructional Facility.',
      rownames = F,
      colnames = c(
        'State',
        'Category 1 Commitment',
        'Category 2 Commitment',
        'State Total',
        paste0('Percentage of ', year_pick(), ' Library Commitments')
      ),
      options = list(dom = 'ltipr')
    ) %>%
      formatCurrency(
        columns = c(
          'Category1_Commitment',
          'Category2_Commitment',
          'State_Total'
        ),
        currency = "$",
        interval = 3,
        mark = ","
      ) %>%
      formatPercentage("Pct_of_Lib_Funding", 2)
  })
  
  state_pick <- reactive({
    input$StateDrop
  })
  
  output$State_Participation_Yearly <- DT::renderDataTable({
    libs_participation_table <- libs_funded %>%
      select(
        ros_entity_number,
        ros_entity_name,
        ros_physical_state,
        funding_year,
        chosen_category_of_service,
        ros_allocation,
        discount_by_ros
      ) %>%
      filter(ros_physical_state == state_pick()) %>%
      group_by(ros_entity_number, ros_entity_name, funding_year, ) %>%
      summarise(
        Category1_Commitment = sum(discount_by_ros, na.rm = T),
        Category2_Commitment = sum(ros_allocation, na.rm = T),
        Entity_Total = Category1_Commitment + Category2_Commitment
      ) %>%
      select(ros_entity_number,
             ros_entity_name,
             funding_year,
             Entity_Total) %>%
      arrange(funding_year) %>%
      pivot_wider(names_from = funding_year, values_from = Entity_Total) %>%
      arrange(ros_entity_name) %>%
      rename("Recipient Entity Number" = ros_entity_number,
             "Recipient Name" = ros_entity_name)
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      libs_participation_table,
      caption = 'Commitment totals by year per participating entity. Commitment sums to recipient entities are estimates when the recipient applied as part of a system or consortium.',
      rownames = F,
      options = list(dom = 'lftipr', rowCallback = JS(rowCallback))
    ) %>%
      formatCurrency(
        columns = c('2016', '2017', '2018', '2019', '2020'),
        currency = "$",
        interval = 3,
        mark = ","
      )
  })
  
  recipient_pick <- reactive({
    input$EntitySelect
  })
  
  output$Recipient_Sum <- DT::renderDataTable({
    recipient_table <- libs_funded %>%
      mutate(
        ros_entity_name = str_to_upper(ros_entity_name),
        ros_physical_city = str_to_upper(ros_physical_city)
      )  %>%
      tidyr::unite(
        col = "lib_city_state",
        c(ros_entity_name, ros_physical_city, ros_physical_state),
        sep = ", ",
        remove = FALSE
      ) %>%
      select(
        lib_city_state,
        ros_entity_name,
        ros_physical_state,
        funding_year,
        chosen_category_of_service,
        ros_allocation,
        discount_by_ros
      ) %>%
      filter(lib_city_state == recipient_pick()) %>%
      group_by(funding_year) %>%
      summarise(
        Category1_Commitment = sum(discount_by_ros, na.rm = T),
        Category2_Commitment = sum(ros_allocation, na.rm = T),
        Recipient_Total = Category1_Commitment + Category2_Commitment
      )
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      recipient_table,
      caption = 'Commitment sums to recipients are estimates when the recipient applied as part of a system or consortium.',
      rownames = F,
      colnames = c(
        'Funding Year',
        'Category 1 Commitment',
        'Category 2 Commitment',
        'Recipient Total'
      ),
      options = list(dom = 'tir')
    ) %>%
      formatCurrency(
        columns = c(
          'Category1_Commitment',
          'Category2_Commitment',
          'Recipient_Total'
        ),
        currency = "$",
        interval = 3,
        mark = ","
      )
  })
  
  # This creates the download file for the library chosen
  library_download <- reactive({
    recipient_table <- libs_funded %>%
      mutate(
        ros_entity_name = str_to_upper(ros_entity_name),
        ros_physical_city = str_to_upper(ros_physical_city)
      )  %>%
      tidyr::unite(
        col = "lib_city_state",
        c(ros_entity_name, ros_physical_city, ros_physical_state),
        sep = ", ",
        remove = FALSE
      ) %>%
      rename(category1_commitment_estimate = discount_by_ros,
             category2_commitment = ros_allocation) %>%
      filter(lib_city_state == recipient_pick())
  })
  
  # Downloadable csv of selected dataset ----
  output$downloadData <- downloadHandler(
    filename = function() {
      paste0(recipient_pick(), ".csv")
    },
    content = function(file) {
      write.csv(library_download(), file, row.names = FALSE)
    }
  )
  
  output$Nat_Year_Sums <- DT::renderDataTable({
    nat_libs_funded_table <- libs_funded %>%
      select(funding_year,
             chosen_category_of_service,
             ros_allocation,
             discount_by_ros) %>%
      group_by(funding_year) %>%
      summarise(
        Category1_Commitment = sum(discount_by_ros, na.rm = T),
        Category2_Commitment = sum(ros_allocation, na.rm = T),
        National_Total = Category1_Commitment + Category2_Commitment
      )
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      nat_libs_funded_table,
      caption = 'E-Rate commitments made to Libraries and Library Systems including those applying as part of a Consortium or as a Non-Instructional Facility.',
      rownames = F,
      colnames = c(
        'Year',
        'Category 1 Commitment',
        'Category 2 Commitment',
        'National Total'
      ),
      options = list(dom = 't')
    ) %>%
      formatCurrency(
        columns = c(
          'Category1_Commitment',
          'Category2_Commitment',
          'National_Total'
        ),
        currency = "$",
        interval = 3,
        mark = ","
      )
  })
  
  output$Nat_Year_Sums_Types <- DT::renderDataTable({
    nat_libs_funded_types_table <- libs_funded %>%
      mutate(Commitment = case_when(
        is.na(discount_by_ros) ~ ros_allocation,
        is.na(ros_allocation) ~ discount_by_ros
      )) %>%
      select(funding_year,
             form_471_service_type_name,
             Commitment) %>%
      group_by(funding_year,
               form_471_service_type_name) %>%
      summarise(Commitment = sum(Commitment, na.rm = T)) %>%
      mutate(Pct = Commitment / sum(Commitment)) %>%
      pivot_wider(
        names_from = funding_year,
        values_from = c(Commitment, Pct),
        names_sort = T
      ) %>%
      relocate(
        form_471_service_type_name,
        Commitment_2016,
        Pct_2016,
        Commitment_2017,
        Pct_2017,
        Commitment_2018,
        Pct_2018,
        Commitment_2019,
        Pct_2019,
        Commitment_2020,
        Pct_2020
      ) %>%
      arrange(desc(Pct_2016))
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      nat_libs_funded_types_table,
      caption = 'Additional details as to the totals committed for specific services.',
      rownames = F,
      colnames = c(
        'Service Type',
        '2016_Amt',
        '2016_%',
        '2017_Amt',
        '2017_%',
        '2018_Amt',
        '2018_%',
        '2019_Amt',
        '2019_%',
        '2020_Amt',
        '2020_%'
      ),
      options = list(dom = 't')
    ) %>%
      formatCurrency(
        columns = c(
          'Commitment_2016',
          'Commitment_2017',
          'Commitment_2018',
          'Commitment_2019',
          'Commitment_2020'
        ),
        currency = "$",
        interval = 3,
        mark = ","
      ) %>%
      formatPercentage(
        columns = c('Pct_2016', 'Pct_2017', 'Pct_2018', 'Pct_2019', 'Pct_2020'),
        digits = 1,
        interval = 3,
        mark = ","
      )
  })
  
  
  
  state_pick2 <- reactive({
    input$StateDrop2
  })
  
  output$Cong_Year_Sums <- DT::renderDataTable({
    cong_dist_funded_table <- libs_funded %>%
      select(
        ros_physical_state,
        ros_congressional_district,
        funding_year,
        chosen_category_of_service,
        ros_allocation,
        discount_by_ros
      ) %>%
      mutate(
        ros_congressional_district = factor(
          ros_congressional_district,
          levels = stringr::str_sort(unique(ros_congressional_district), numeric = TRUE)
        )
      ) %>%
      group_by(funding_year,
               ros_congressional_district,
               ros_physical_state) %>%
      summarise(
        sum_cat1_discount = sum(discount_by_ros, na.rm = T),
        sum_cat2_allocation = sum(ros_allocation, na.rm = T),
        cong_dist_total = sum_cat1_discount + sum_cat2_allocation
      ) %>%
      select(ros_congressional_district,
             ros_physical_state,
             funding_year,
             cong_dist_total) %>%
      pivot_wider(names_from = funding_year, values_from = cong_dist_total) %>%
      filter(ros_physical_state == state_pick2()) %>%
      select(-ros_physical_state)
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      cong_dist_funded_table,
      caption = 'E-Rate commitments made to Library and Library System recipients including those applying as part of a Consortium or as a Non-Instructional Facility grouped by Congressional District. The Congressional District is the one associated with the library recipient (i.e. not the library system). Some Congressional Districts may not be represented in the data.',
      rownames = F,
      colnames = c('Recipient Congressional District' = 'ros_congressional_district'),
      options = list(dom = 'ltpr', rowCallback = JS(rowCallback))
    ) %>%
      formatCurrency(
        columns = c('2016', '2017', '2018', '2019', '2020'),
        currency = "$",
        interval = 3,
        mark = ","
      )
  })
  
  erateimls_download <- reactive({
    rosetta_stone %>%
      select(-X) %>%
      rename(
        erate_recipient_entity_number = ros_entity_number,
        erate_recipient_entity_name = ros_entity_name,
        erate_recipient_state = ros_physical_state,
        erate_recipient_address = ros_physical_address,
        imls_state = STABR,
        imls_fscskey = FSCSKEY,
        imls_fscs_seq = FSCS_SEQ,
        imls_library_name = LIBNAME,
        imls_library_address = ADDRESS
      )
  })
  
  # Downloadable csv of selected dataset ----
  output$downloadIMLSMatches <- downloadHandler(
    filename = function() {
      paste0("ERateMatchedToIMLS", ".csv")
    },
    content = function(file) {
      write.csv(erateimls_download(), file, row.names = FALSE)
    }
  )
  
  output$City_Size_Table <- DT::renderDataTable({
    city_size_table <- libs_funded %>%
      group_by(ros_entity_number) %>%
      arrange(funding_year) %>%
      slice(1) %>%
      group_by(LOCALE_ADD_DESCR, ros_urban_rural_status) %>%
      count(name = "CountEntities")
    
    # DT info: https://datatables.net/reference/option/dom https://rstudio.github.io/DT/
    datatable(
      city_size_table,
      caption = 'Entity designations from both IMLS and E-Rate data for entire E-Rate dataset (2016-2020).',
      rownames = F,
      colnames = c(
        'IMLS Designation (LOCALE_ADD)',
        'E-Rate Designation',
        'Count of Unique Entities'
      ),
      options = list(dom = 'ltipr',
                     rowCallback = JS(rowCallback))
    )
  })
  
  output$City_Size_Plot <- renderPlot({
    libs_funded %>%
      group_by(ros_entity_number) %>%
      arrange(funding_year) %>%
      slice(1) %>%
      group_by(LOCALE_ADD_DESCR, ros_urban_rural_status) %>%
      count(name = "CountEntities") %>%
      ggplot(aes(
        x = CountEntities,
        y = reorder(LOCALE_ADD_DESCR, CountEntities),
        fill = ros_urban_rural_status
      )) +
      geom_col(color = "black", size = 0.2) +
      theme_linedraw() +
      scale_fill_manual(values = wes_palette("GrandBudapest1"),
                        na.value = "#d7d7ef") +
      labs(
        title = "Count of Entities",
        subtitle = "IMLS City Size Designation & E-Rate Designation",
        fill = "E-Rate Designation",
        caption = "NA = Data Not Available"
      ) +
      xlab("Number of Entities") +
      ylab("IMLS Designation")
  })
  
  output$Funding_by_Org_Plot <- renderPlot({
    libs_funded %>%
      select(
        ros_entity_number,
        organization_entity_type_name,
        funding_year,
        chosen_category_of_service,
        ros_allocation,
        discount_by_ros
      ) %>%
      group_by(funding_year, organization_entity_type_name) %>%
      summarise(
        sum_cat1_discount = sum(discount_by_ros, na.rm = T),
        sum_cat2_allocation = sum(ros_allocation, na.rm = T),
        org_type_total = sum_cat1_discount + sum_cat2_allocation,
        total_entities = n_distinct(ros_entity_number),
        per_entity_commitment = org_type_total / total_entities
      ) %>%
      select(
        funding_year,
        organization_entity_type_name,
        org_type_total,
        total_entities,
        per_entity_commitment
      ) %>%
      ggplot(aes(x = funding_year, y = per_entity_commitment)) +
      geom_bar(
        aes(
          fill = reorder(organization_entity_type_name,-per_entity_commitment)
        ),
        color = "black",
        size = 0.2,
        stat = "identity",
        position = "dodge"
      ) +
      theme_linedraw() +
      scale_fill_manual(values = c("#F1BB7B", "#FD6467", "#D67236")) +
      #scale_fill_manual(values = c("#E69F00", "#56B4E9", "#009E73")) +
      scale_y_continuous(labels = scales::dollar_format()) +
      labs(
        title = "Per Recipient Funding Commitment by Organization Type",
        #subtitle = "IMLS City Size Designation & E-Rate Designation",
        fill = "Organization Type",
        x = "Funding Year",
        y = "Average Commitment Per Recipient"
      )
  })
  
  
}

# Run the application
shinyApp(ui = ui, server = server)
