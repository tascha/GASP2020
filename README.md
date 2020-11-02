# Files for the Government Advances in Statistical Programming 2020 Conference

This repository contains code and prsentation slides as they were written at the time of the GASP2020 conference for the presentation: "E-rate Funding: How I Built a Data Pipeline in R From a Scheduled R Script on AWS to a Shiny Dashboard."

Authors: Bree Norlander & Chris Jowaisas

## Abstract

The Schools and Libraries Program, also known as E-Rate, provides discounts to schools and libraries on a variety of internet and telecommunications services and related equipment. The program is administered by the Universal Service Administrative Company (USAC) and funded by the Universal Service Fund. The data from applications for discounts, as well as the funding commitments and disbursements are all openly available on USAC’s open data portal. While the portal contains a wealth of information, the datasets are large, updated on a daily basis, and require additional calculations to understand usage at an individual recipient level. This level of analysis is often too time-consuming and cumbersome for the average stakeholder interested in the data.

Upon discovering this data and recognizing the valuable insights our team could uncover and share, we consulted with the American Library Association’s E-Rate Task Force and with USAC managers and analysts to gather research questions of relevance and to better understand the variables of interest within the datasets. We then set out to share our analysis via an up-to-date dashboard addressing questions of interest specifically about libraries that apply for and use E-Rate discounts. 

Because the datasets are so large, the most efficient way of retrieving the data is by using filtered Socrata Open Data API (SODA) calls. However, even with our filters applied, the dataset retrieval is too slow for direct usage within a Shiny Dashboard. To overcome this problem, I wrote an R script to retrieve, clean, and calculate the necessary data that runs on a virtual machine on AWS every weekday. It outputs datasets in .csv format to an S3 bucket on AWS that a Shiny application can then retrieve much more quickly. The datasets stored in S3 are designed specifically to power the tables and data visualizations viewable in our Shiny dashboard.

During this lightning talk I will walk through the process of automating an R Script on a virtual machine using AWS, saving the resulting .csvs to an S3 bucket on AWS, and then calling the data to power a Shiny application.
