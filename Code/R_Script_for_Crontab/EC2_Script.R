print(Sys.time())

# Load libraries
library(RSocrata)
library(dplyr)
library(tidyr)
library(ggplot2)
library(stringr)
library(glue)
library(purrr)
library(fuzzyjoin)
library(geosphere)
library(janitor)
library(stringdist)
library(aws.s3)
library(aws.ec2metadata)

# Pull disbursement data for all funded entities
disbursement <- read.socrata(
  "https://opendata.usac.org/resource/qdmp-ygft.json?form_471_frn_status_name=Funded",
  app_token = Sys.getenv("USAC_Socrata")
)

# Write to s3 bucket
s3write_using(disbursement, FUN = write.csv, bucket = "erate-data/data", object = "Funded_Disbursements.csv")

print("disbursement dataset written to s3")

# Gather the full list of years available in the dataset and create a list
years <- read.csv("https://opendata.usac.org/resource/avi8-svp9.csv?$select=distinct%20funding_year")[[1]]

y = list()
disblist1 = list()

# Call the API by each year for Category1 service
for (i in 1:length(years)) {
  y[[i]] <- read.socrata(
  glue("https://opendata.usac.org/resource/avi8-svp9.json?chosen_category_of_service=Category1&funding_year={years[i]}&form_471_status_name=Committed&form_471_frn_status_name=Funded"), # Parameters: Category 1, Funding Year, Committed, Funded
  app_token = Sys.getenv("USAC_Socrata"))

  disblist1[[i]] <- y[[i]] %>%
    filter(organization_entity_type_name %in% c("Library", "Library System", "Consortium"))

  y[[i]] <- y[[i]] %>%
    # Add a column indicating hpow many rows exist for a unique combo of billed entity no and 471 line item no
    add_count(billed_entity_number, form_471_line_item_number, name = "count_ros") %>%
    # Add estimated amount received by individual recipient by calculating
    mutate(discount_by_ros = as.numeric(post_discount_extended_eligible_line_item_costs)/count_ros) %>%
    # Keep only libraries

    mutate(organization_entity_type_name = str_to_lower(str_trim(organization_entity_type_name, side = "both")),
           ros_entity_type = str_to_lower(str_trim(ros_entity_type, side = "both")),
           ros_entity_name = str_to_lower(str_trim(ros_entity_name, side = "both")),
           ros_subtype = str_to_lower(str_trim(ros_subtype, side = "both"))) %>%
    filter(stringr::str_detect(ros_entity_type, "libr") |
             ros_entity_type == "non-instructional facility (nif)") %>%
    filter(str_detect(ros_entity_type, "libr") |
             (ros_entity_type == "non-instructional facility (nif)" & str_detect(organization_entity_type_name, "libr")) |
             (ros_entity_type == "non-instructional facility (nif)" & organization_entity_type_name == "consortium" & str_detect(ros_entity_name, "libr")),
           (is.na(ros_subtype) | !str_detect(ros_subtype, "public school")))
}

cat1_disblist <- bind_rows(disblist1)
cat1_all_libs <- bind_rows(y)

q = list()
disblist2 = list()

# Call the API by each year for Category2 service
for (i in 1:length(years)) {
  q[[i]] <- read.socrata(
  glue("https://opendata.usac.org/resource/avi8-svp9.json?chosen_category_of_service=Category2&funding_year={years[i]}&form_471_status_name=Committed&form_471_frn_status_name=Funded"), # Parameters: Category 2, Funding Year, Committed, Funded
  app_token = Sys.getenv("USAC_Socrata"))

  disblist2[[i]] <- q[[i]] %>%
    filter(organization_entity_type_name %in% c("Library", "Library System", "Consortium"))

  q[[i]] <- q[[i]] %>%
    # Add a column indicating how many rows exist for a unique combo of billed entity no and 471 line item no
    #add_count(billed_entity_number, form_471_line_item_number, name = "count_ros") %>%
    # Add estimated amount received by individual recipient by calculating
    mutate(ros_allocation = as.numeric(original_allocation)*as.numeric(dis_pct)) %>%
    # Keep only libraries
     mutate(organization_entity_type_name = str_to_lower(str_trim(organization_entity_type_name, side = "both")),
           ros_entity_type = str_to_lower(str_trim(ros_entity_type, side = "both")),
           ros_entity_name = str_to_lower(str_trim(ros_entity_name, side = "both")),
           ros_subtype = str_to_lower(str_trim(ros_subtype, side = "both"))) %>%
    filter(stringr::str_detect(ros_entity_type, "libr") |
             ros_entity_type == "non-instructional facility (nif)") %>%
    filter(str_detect(ros_entity_type, "libr") |
             (ros_entity_type == "non-instructional facility (nif)" & str_detect(organization_entity_type_name, "libr")) |
             (ros_entity_type == "non-instructional facility (nif)" & organization_entity_type_name == "consortium" & str_detect(ros_entity_name, "libr")),
           (is.na(ros_subtype) | !str_detect(ros_subtype, "public school")))

}

cat2_disblist <- bind_rows(disblist2)
cat2_all_libs <- bind_rows(q)

erate_libs <- bind_rows(cat1_all_libs, cat2_all_libs)

disbursement_merge <- bind_rows(cat1_disblist, cat2_disblist)

# Write to s3 bucket
s3write_using(erate_libs, FUN = write.csv, bucket = "erate-data/data", object = "Libraries_Funded_Committed.csv")

print("erate_libs dataset written to s3")

# Write to s3 bucket
s3write_using(disbursement_merge, FUN = write.csv, bucket = "erate-data/data", object = "Data_for_Disbursement_Merge.csv")

print("disbursement_merge dataset written to s3")

disbursement_merged <- disbursement_merge %>%
  distinct(funding_request_number, form_471_line_item_number, .keep_all = T) %>%
  mutate(post_discount_extended_eligible_line_item_costs =
           as.numeric(post_discount_extended_eligible_line_item_costs),
         funding_request_number = as.numeric(funding_request_number)) %>%
  group_by(funding_request_number) %>%
  summarise(post_discount_extended_eligible_line_item_costs_sum =
           sum(post_discount_extended_eligible_line_item_costs)) %>%
  left_join(disbursement %>%
              select(funding_request_number, funding_commitment_request, total_authorized_disbursement) %>%
              mutate(funding_request_number = as.numeric(funding_request_number)),
              by = "funding_request_number"
            ) %>%
  mutate(total_authorized_disbursement = as.numeric(total_authorized_disbursement),
  post_discount_extended_eligible_line_item_costs_sum = as.numeric(post_discount_extended_eligible_line_item_costs_sum),
  pct_of_committed_received = total_authorized_disbursement / post_discount_extended_eligible_line_item_costs_sum)


# Write to s3 bucket
s3write_using(disbursement_merged, FUN = write.csv, bucket = "erate-data/data", object = "Disbursement_Merged.csv")

print("disbursement_merged dataset written to s3")

rm(disbursement_merge, disbursement, disbursement_merged)

# Read in IMLS PLS datasets stored in S3
imls_outlets <- s3read_using(FUN = read.csv, object = "data/pls_fy18_outlet_pud18i.csv", bucket = "erate-data")
imls_ae <- s3read_using(FUN = read.csv, object = "data/pls_fy18_ae_pud18i.csv", bucket = "erate-data")

# Import data received from MA with ros_entity_numbers matched to FSCS keys
MA_PL <- s3read_using(FUN = read.csv, object = "data/MA_PL_Outlets.csv", bucket = "erate-data")
MA_BL <- s3read_using(FUN = read.csv, object = "data/MA_BranchLibrary_Outlets.csv", bucket = "erate-data")

# Change column names to match erate data
MA_BL <- MA_BL %>%
  select(BranchName, FSCSID, FSCS_SEQ, ErateEntityNumber) %>%
  rename(FSCSKEY = FSCSID,
          ros_entity_number = ErateEntityNumber) %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number))

MA_PL <- MA_PL %>%
  select(Library, FSCSID, FSCS_SEQ, ErateEntityNumber) %>%
  rename(FSCSKEY = FSCSID,
          ros_entity_number = ErateEntityNumber) %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number))

# Import hand matching dataset with ros_entity_numbers matched to FSCS keys
hand_match <- s3read_using(FUN = read.csv, na.strings = c(""," ","N/A","n/a"), object = "data/Hand_Matches_08-2020.csv", bucket = "erate-data")

# filter df down to only those entries with FSCS_SEQ
hand_match <- hand_match %>%
  filter(!is.na(FSCS_SEQ)) %>%
  mutate(FSCS_SEQ = as.numeric(FSCS_SEQ),
         ros_entity_number = as.numeric(ros_entity_number)) %>% 
  select(-NOTES) # remove notes column, not needed

# Combine the hand matching and MASS matched datasets into one
MA_and_hand_matches <- hand_match %>%
  full_join(MA_PL %>% 
          select(ros_entity_number, FSCSKEY, FSCS_SEQ),
          by = c("ros_entity_number", "FSCSKEY", "FSCS_SEQ")) %>% 
  full_join(MA_BL %>% 
          select(ros_entity_number, FSCSKEY, FSCS_SEQ),
          by = c("ros_entity_number", "FSCSKEY", "FSCS_SEQ")) %>% 
  select(ros_entity_number, FSCSKEY, FSCS_SEQ) %>% 
  distinct(ros_entity_number, FSCSKEY, FSCS_SEQ) %>% 
  filter(!is.na(ros_entity_number),
         ros_entity_number != "",
         !is.na(FSCSKEY),
         !is.na(FSCS_SEQ)) %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number))

# I'm now going to add FSCSKEY and FSCS_SEQ columns to erate_libs dataset and include the matches
erate_libs <- erate_libs %>% 
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
  left_join(MA_and_hand_matches,
            by = "ros_entity_number") %>% 
  relocate(c("FSCSKEY", "FSCS_SEQ"), .after = ros_entity_number)

# Write to s3 bucket
s3write_using(erate_libs, FUN = write.csv, bucket = "erate-data/data", object = "Libraries_Funded_Committed_with_FSCS.csv")

print("erate_libs with FSCS dataset written to s3")

# Create a dataframe of erate libraries to use in matching
erate_libs_for_matching <- erate_libs %>%
  # With distinct function, if there are multiple rows for a given combination of inputs, 
  # only the first row will be preserved.
  distinct(ros_entity_number, ros_physical_state, .keep_all = T) %>%
  as.data.frame()

# Need to get FSCS_SEQ codes in the imls_ae dataset where possible, if no FSCS_SEQ matches, then use code 999
imls_ae <- imls_ae %>%
  left_join(imls_outlets %>% select(FSCSKEY, FSCS_SEQ, LIBNAME),
            by = c("FSCSKEY", "LIBNAME")) %>%
  mutate(FSCS_SEQ = tidyr::replace_na(FSCS_SEQ, 999)) %>%
  relocate(FSCS_SEQ, .after = FSCSKEY)

# Create a dataframe of IMLS outlets to use for matching
outlets_for_matching <- imls_outlets %>%
  distinct(FSCSKEY, FSCS_SEQ, .keep_all = T) %>% # this may not be necessary but just in case of duplicates
  as.data.frame()

# Create a dataframe of IMLS outlets to use for matching
ae_for_matching <- imls_ae %>%
  distinct(FSCSKEY, FSCS_SEQ, .keep_all = T) %>% # this may not be necessary bust just in case of duplicates
  as.data.frame() 

# Prepare data for matching
# substring extraction derived from https://rpubs.com/iPhuoc/stringr_manipulation
# stringr and regex help from https://stringr.tidyverse.org/articles/regular-expressions.html
# Eliminate common words in library names like "the" "library" etc.
erate_libs_for_matching <- erate_libs_for_matching %>%
  mutate(ros_longitude = as.numeric(ros_longitude),
         ros_latitude = as.numeric(ros_latitude)) %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number),
         ros_physical_state = str_to_lower(str_trim(ros_physical_state, side = "both")),
         ros_physical_city = str_to_lower(str_trim(ros_physical_city, side = "both")),
         organization_name = str_to_lower(str_trim(organization_name, side = "both")),
         org_city = str_to_lower(str_trim(org_city, side = "both")),
         org_state = str_to_lower(str_trim(org_state, side = "both")),
         ros_entity_name_processed = str_replace_all(ros_entity_name, 
                                                     c("the " = "", "library" = "", "libraries" = "", "branch" = "",
                                                       "public" = "", "community" = "", "\\bbr\\b" = "", "\\blib\\b" = "")),
         ros_entity_name_processed = janitor::make_clean_names(ros_entity_name_processed, case="upper_camel"),
         erate_substring = stringr::str_sub(ros_entity_name_processed, 1, 15)
         )

outlets_for_matching <- outlets_for_matching %>%
  mutate(LIBNAME = str_to_lower(str_trim(LIBNAME, side = "both")),
         STABR = str_to_lower(str_trim(STABR, side = "both")),
         CITY = str_to_lower(str_trim(CITY, side = "both")),
         LIBNAME_PROCESSED = str_replace_all(LIBNAME, 
                                                     c("the " = "", "library" = "", "libraries" = "", "branch" = "",
                                                       "public" = "", "community" = "", "\\bbr\\b" = "", "\\blib\\b" = "")),
         LIBNAME_PROCESSED = janitor::make_clean_names(LIBNAME_PROCESSED, case="upper_camel"),
         SUBSTRING = stringr::str_sub(LIBNAME_PROCESSED, 1, 15)
         )

ae_for_matching <- ae_for_matching %>%
  mutate(LIBNAME = str_to_lower(str_trim(LIBNAME, side = "both")),
         STABR = str_to_lower(str_trim(STABR, side = "both")),
         CITY = str_to_lower(str_trim(CITY, side = "both")),
         LIBNAME_PROCESSED = str_replace_all(LIBNAME, 
                                                     c("the " = "", "library" = "", "libraries" = "", "branch" = "",
                                                       "public" = "", "community" = "", "\\bbr\\b" = "", "\\blib\\b" = "")),
         LIBNAME_PROCESSED = janitor::make_clean_names(LIBNAME_PROCESSED, case="upper_camel"),
         SUBSTRING = stringr::str_sub(LIBNAME_PROCESSED, 1, 15)
         )

# Get the list of states that exist in both erate and imls_outlets datasets
outletsstates <-  intersect(erate_libs_for_matching$ros_physical_state, outlets_for_matching$STABR)
# create empty list
geo_list = list()
# geo joining on lat/lon between erate data and outlets data
for (i in 1:length(outletsstates)) {
    geo_list[[i]] <- geo_join(
      erate_libs_for_matching %>% filter(ros_physical_state == outletsstates[i],
                                     !is.na(ros_latitude) | !is.na(ros_longitude),
                                     is.na(FSCSKEY) & is.na(FSCS_SEQ)),
      outlets_for_matching %>% filter(STABR == outletsstates[i],
                                  !is.na(LATITUDE) & !is.na(LONGITUD)),
      by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"), 
      method = "haversine",
      mode = "inner",
      max_dist = 0.4,
      distance_col = "miles_apart")
}

geo_matches_outlets <- bind_rows(geo_list)

# Get the list of states that exist in both erate and imls_ae datasets
aestates <-  intersect(erate_libs_for_matching$ros_physical_state, ae_for_matching$STABR)
# create empty list
geo_list = list()
# geo joining on lat/lon between erate data and ae data
for (i in 1:length(aestates)) {
    geo_list[[i]] <- geo_join(
      erate_libs_for_matching %>% filter(ros_physical_state == aestates[i],
                                     !is.na(ros_latitude) | !is.na(ros_longitude),
                                     is.na(FSCSKEY) & is.na(FSCS_SEQ)), 
      ae_for_matching %>% filter(STABR == aestates[i],
                             !is.na(LATITUDE) & !is.na(LONGITUD)),
      by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"), 
      method = "haversine",
      mode = "inner",
      max_dist = 0.4,
      distance_col = "miles_apart")
}

geo_matches_ae <- bind_rows(geo_list)


# What exists in USAC but NOT IMLS_OUTLETS?
geo_list = list()

for (i in 1:length(outletsstates)) {
    geo_list[[i]] <- geo_anti_join(
      erate_libs_for_matching %>% filter(ros_physical_state == outletsstates[i],
                                     !is.na(ros_latitude) & !is.na(ros_longitude),
                                     is.na(FSCSKEY) & is.na(FSCS_SEQ)),
      outlets_for_matching %>% filter(STABR == outletsstates[i],
                                  !is.na(LATITUDE) & !is.na(LONGITUD)),
      by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"),
      method = "haversine",
      max_dist = 0.4)
}

geo_non_matches_erate_outlets <- bind_rows(geo_list)


# What exists in USAC but NOT IMLS_AE?
geo_list = list()

for (i in 1:length(aestates)) {
    geo_list[[i]] <- geo_anti_join(
      erate_libs_for_matching %>% filter(ros_physical_state == aestates[i],
                                     !is.na(ros_latitude) & !is.na(ros_longitude),
                                     is.na(FSCSKEY) & is.na(FSCS_SEQ)), 
      ae_for_matching %>% filter(STABR == aestates[i], !is.na(LATITUDE) & !is.na(LONGITUD)),
      by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"),
      method = "haversine",
      max_dist = 0.4)
}

geo_non_matches_erate_ae <- bind_rows(geo_list)

# Using the intersection operator, find the rows that exist in both non_matches - this will be a full list of erate
# entities that don't match IMLS (with the exception of PR because it wasn't included in the outletsstates or aestates lists)
# We also have to add back in those entities that are missing a lat or lon and haven't matched by other means
geo_non_matches_full <- intersect(geo_non_matches_erate_ae, geo_non_matches_erate_outlets) %>% 
  full_join(erate_libs_for_matching %>%
              filter(ros_physical_state != "pr",
                     is.na(ros_latitude) | is.na(ros_longitude),
                     is.na(FSCSKEY) & is.na(FSCS_SEQ)),
            by = c("ros_entity_number", "FSCSKEY", "FSCS_SEQ", "ros_entity_name", "ros_entity_type", "is_school_library_independent",
                   "ros_subtype", "ros_status", "ros_physical_address", "ros_physical_city", "ros_physical_state",
                   "ros_physical_zipcode", "ros_physical_zipcode_ext", "ros_physical_county", "ros_latitude", "ros_longitude",
                   "ros_congressional_district",
                   "ros_urban_rural_status", "ros_number_of_full_time_students", "ros_total_number_of_part_time_students",
                   "ros_peak_number_of_part_time_students", "ros_number_of_nslp_students", "billed_entity_number", "organization_name",
                   "organization_entity_type_name", "org_address1", "org_city", "org_state", "org_zipcode",
                   "org_congressional_district", "funding_year", "application_number", "funding_request_number",
                   "form_471_line_item_number", "is_certified_in_window", "form_471_status_name", "form_471_frn_status_name",
                   "pending_reason", "spin_name", "spin_number", "chosen_category_of_service", "form_471_service_type_name",
                   "form_471_function_name", "form_471_product_name", "upload_speed", "form_471_upload_speed_unit_name",
                   "download_speed", "form_471_download_speed_unit_name", "total_monthly_cost", "monthly_recur_ineligible_cost",
                   "monthly_recurring_unit_eligible_costs", "monthly_quantity", "total_monthly_eligible_recurring_costs",
                   "months_of_service", "total_eligible_recurring_costs", "total_one_time_cost", "one_time_ineligible_cost",
                   "one_time_eligible_costs", "one_time_quantity", "total_eligible_one_time_costs",
                   "pre_discount_extended_eligible_line_item_costs", "dis_pct", "post_discount_extended_eligible_line_item_costs",
                   "post_discount_applicant_share", "ros_square_footage", "qty_allocation", "tribal_type",
                   "form_471_frn_fiber_type_name", "form_471_frn_fiber_sub_type_name", "ros_physical_address_2", "original_allocation",
                   "count_ros", "discount_by_ros", "ros_allocation", "ros_entity_name_processed",
                   "erate_substring"))

rm(geo_non_matches_erate_ae, geo_non_matches_erate_outlets, geo_list)

# Now we can eliminate the .x and rename the .y
geo_matches_outlets <- geo_matches_outlets %>%
  select(-FSCSKEY.x, -FSCS_SEQ.x) %>% 
  rename(FSCSKEY = FSCSKEY.y,
         FSCS_SEQ = FSCS_SEQ.y) %>% 
  relocate(c(FSCSKEY, FSCS_SEQ), .after = ros_entity_number) # relocate FSCSKEY and FSCS_SEQ column

geo_matches_ae <- geo_matches_ae %>%
  select(-FSCSKEY.x, -FSCS_SEQ.x) %>% 
  rename(FSCSKEY = FSCSKEY.y,
         FSCS_SEQ = FSCS_SEQ.y) %>% 
  relocate(c(FSCSKEY, FSCS_SEQ), .after = ros_entity_number) # relocate FSCSKEY and FSCS_SEQ column

# Merge the two different geo matches
geo_matches_full <- geo_matches_outlets %>% 
        select(ros_entity_number, FSCSKEY, FSCS_SEQ, ros_entity_name, ros_physical_state, ros_physical_address, erate_substring, STABR, LIBID, LIBNAME, ADDRESS, SUBSTRING, miles_apart) %>% 
  full_join(geo_matches_ae %>%
        select(ros_entity_number, FSCSKEY, FSCS_SEQ, ros_entity_name, ros_physical_state, ros_physical_address, erate_substring, STABR, LIBID, LIBNAME, ADDRESS, SUBSTRING, miles_apart),
        by = c("ros_entity_number", "FSCSKEY", "FSCS_SEQ", "ros_entity_name", "ros_physical_state", "ros_physical_address", "erate_substring", "STABR", "LIBID", "LIBNAME", "ADDRESS", "SUBSTRING", "miles_apart")
  )

x <- names(geo_matches_full)
# Add back in the matches we already knew before the geojoining
geo_matches_full <- geo_matches_full %>%
  full_join(erate_libs_for_matching %>%
              filter(ros_physical_state != "pr", # no pr states will match imls
                     !is.na(FSCSKEY) & !is.na(FSCS_SEQ)),
            by = c("ros_entity_number", "ros_entity_name", "ros_physical_state", "ros_physical_address", "erate_substring", "FSCSKEY", "FSCS_SEQ")) %>% 
  select(all_of(x)) %>% 
  left_join(outlets_for_matching %>% 
              select(FSCSKEY, FSCS_SEQ, STABR, LIBID, LIBNAME, ADDRESS, SUBSTRING),
            by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  mutate(STABR = coalesce(STABR.x, STABR.y),
         LIBID = coalesce(LIBID.x, LIBID.y),
         LIBNAME = coalesce(LIBNAME.x, LIBNAME.y),
         ADDRESS = coalesce(ADDRESS.x, ADDRESS.y),
         SUBSTRING = coalesce(SUBSTRING.x, SUBSTRING.y)) %>% 
  select(-STABR.x, -LIBID.x, -LIBNAME.x, -ADDRESS.x, -SUBSTRING.x, -STABR.y, -LIBID.y, -LIBNAME.y, -ADDRESS.y, -SUBSTRING.y) %>% 
  left_join(ae_for_matching %>% 
              select(FSCSKEY, FSCS_SEQ, STABR, LIBID, LIBNAME, ADDRESS, SUBSTRING),
            by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  mutate(STABR = coalesce(STABR.x, STABR.y),
         LIBID = coalesce(LIBID.x, LIBID.y),
         LIBNAME = coalesce(LIBNAME.x, LIBNAME.y),
         ADDRESS = coalesce(ADDRESS.x, ADDRESS.y),
         SUBSTRING = coalesce(SUBSTRING.x, SUBSTRING.y)) %>% 
  select(-STABR.x, -LIBID.x, -LIBNAME.x, -ADDRESS.x, -SUBSTRING.x, -STABR.y, -LIBID.y, -LIBNAME.y, -ADDRESS.y, -SUBSTRING.y)

# The geo_matches_full dataset contains many duplicate libraries because multiple libraries from IMLS matched the distance specifications, 
# thus duplicating libraries in the USAC data. We need to choose the best of the multiple matches. We'll create a custom algorithm for this.
geo_string_match <- geo_matches_full %>% 
  mutate(ros_physical_address = str_to_lower(str_trim(ros_physical_address, side = "both")),
         ADDRESS = str_to_lower(str_trim(ADDRESS, side = "both")),
         # Add string distance calculations
         sub_dist = stringdist::stringdist(erate_substring, SUBSTRING, method = "jw"),
         name_dist = stringdist::stringdist(ros_entity_name, LIBNAME, method = "jw"),
         add_dist = stringdist::stringdist(ros_physical_address, ADDRESS, method = "jw"),
         sum_distances = sub_dist + name_dist + add_dist) %>%
  group_by(ros_entity_number) %>% 
  arrange(sum_distances) %>% 
  # The next line keeps the rows where sum_distances is na see https://stackoverflow.com/a/44015049/5593458
  slice(unique(c(which.min(sum_distances), which(is.na(sum_distances))))) %>% 
  filter(is.na(sum_distances) | sum_distances < 1.2 | add_dist < 0.1)

# What didn't end up matching?
rosnomatch <- base::setdiff(unique(erate_libs$ros_entity_number),
                       geo_string_match$ros_entity_number)

# Make the list into a dataframe
non_matches <- data.frame(ros_entity_number = matrix(unlist(rosnomatch), nrow=length(rosnomatch), byrow=T), stringsAsFactors=FALSE)

non_matches <- non_matches %>% 
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
  # Add in additional information as the dataset is currently just ros_entity_numbers
  left_join(erate_libs %>%
              mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
              select(ros_entity_number, ros_entity_name, ros_entity_type, 
                     ros_subtype, ros_physical_address, ros_physical_city, ros_physical_state),
            by = "ros_entity_number") %>%
  group_by(ros_entity_number) %>% 
  slice(1)

# Create a new df to use that starts with the non-matches, adds in columns from erate_libs_for_matching, 
# cleans strings, and adds city_state col
# Create a new df to use that starts with the non-matches, adds in columns from erate_libs_for_matching, cleans strings
fuzzy_string_test <- non_matches %>%
  filter(ros_physical_state != "PR") %>% # pr will not match with imls
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>% 
  left_join(erate_libs_for_matching %>%
              select(ros_entity_number, ros_entity_name, ros_physical_zipcode, ros_entity_name_processed, erate_substring), 
            by = c("ros_entity_number", "ros_entity_name")) %>%
  mutate(ros_physical_state = str_to_lower(str_trim(ros_physical_state, side = "both")),
         ros_physical_city = str_to_lower(str_trim(ros_physical_city, side = "both")))

# Get the list of zip codes that exist in both datasets
outletszip <-  intersect(fuzzy_string_test$ros_physical_zipcode, outlets_for_matching$ZIP)
# string matching between erate data and outlets data
name_list = list()

# match on substrings by zipcode
for (i in 1:length(outletszip)) {
    name_list[[i]] <- fuzzy_string_test %>% 
      filter(ros_physical_zipcode == outletszip[i]) %>%
      stringdist_left_join(outlets_for_matching %>% 
                             filter(ZIP == outletszip[i]),
                           by = c("erate_substring" = "SUBSTRING"),
                           max_dist = 0.3,
                           method = "jw",
                           distance_col = "substring_dist")
}

name_matches_zip_outlets <- bind_rows(name_list)

# Get the list of zip codes that exist in both datasets
aezip <-  intersect(fuzzy_string_test$ros_physical_zipcode, ae_for_matching$ZIP)
# string matching between erate data and outlets data
name_list = list()

for (i in 1:length(aezip)) {
    name_list[[i]] <- fuzzy_string_test %>% 
      filter(ros_physical_zipcode == aezip[i]) %>%
      stringdist_left_join(ae_for_matching %>% 
                             filter(ZIP == aezip[i]),
                           by = c("erate_substring" = "SUBSTRING"),
                           max_dist = 0.3,
                           method = "jw",
                           distance_col = "substring_dist")
}

name_matches_zip_aes <- bind_rows(name_list)

more_matched <- merge(name_matches_zip_outlets, name_matches_zip_aes, all = T)

more_matched <- more_matched %>%
  group_by(ros_entity_number) %>%
  slice_min(substring_dist) %>%
  select(ros_entity_number:ZIP, LIBNAME_PROCESSED:substring_dist)

# Write out the matches to a df
geo_string_match <- merge(geo_string_match %>%
                select(-erate_substring, -LIBID, -SUBSTRING, -miles_apart, -sub_dist, -name_dist, -add_dist, -sum_distances),
              more_matched %>%
                select(ros_entity_number, ros_entity_name, ros_physical_state, ros_physical_address, STABR, 
                       FSCSKEY, FSCS_SEQ, LIBNAME, ADDRESS),
              by = c("ros_entity_number", "ros_entity_name", "ros_physical_state", "ros_physical_address", "STABR", 
                        "FSCSKEY", "FSCS_SEQ", "LIBNAME", "ADDRESS"), 
              all = T)

# Remove duplicate rows
geo_string_match <- unique(geo_string_match) %>% 
  # Now to attend to the duplicated ros_entity_numbers that happened for a variety of reasons
  distinct(ros_entity_number, ros_physical_state, .keep_all = T)

# Write to s3 bucket
s3write_using(geo_string_match, FUN = write.csv, bucket = "erate-data/data", object = "USAC_IMLS_MATCHED.csv")

print("Rosetta stone written to s3")

tmp <- setdiff(non_matches$ros_entity_number, more_matched$ros_entity_number)

non_matches_2 <- data.frame(ros_entity_number = matrix(unlist(tmp), nrow=length(tmp), byrow=T), stringsAsFactors=FALSE)
# This will include PR states because the non_matches dataframe includes them
non_matches_2 <- non_matches_2 %>%
  left_join(non_matches,
            by = "ros_entity_number")

# Write to s3 bucket
s3write_using(non_matches_2, FUN = write.csv, bucket = "erate-data/data", object = "USAC_Libs_Not_Matched_to_IMLS.csv")

print("Non matches written to s3")

# Merge USAC and IMLS
# Left join so that we only keep ERate entities
erate_imls <- erate_libs %>%
  # this join adds FSCS codes to the erate_libs dataset
  left_join(geo_string_match %>%
              select(ros_entity_number, FSCSKEY, FSCS_SEQ),
            by = c("ros_entity_number")) %>%
  mutate(FSCSKEY = coalesce(FSCSKEY.x, FSCSKEY.y),
         FSCS_SEQ = coalesce(FSCS_SEQ.x, FSCS_SEQ.y)) %>% 
  select(-FSCSKEY.x, -FSCSKEY.y,-FSCS_SEQ.x, -FSCS_SEQ.y) %>% 
  # this join adds matching IMLS outlets records
  left_join(imls_outlets,
            by = c('FSCSKEY', 'FSCS_SEQ')) %>%
  # this join adds matching IMLS ae records
  left_join(imls_ae,
            by = c("STABR", "FSCSKEY", "FSCS_SEQ", "C_FSCS", "LIBID", "LIBNAME", "ADDRESS", "CITY", "ZIP", "ZIP4", "CNTY", "PHONE", "YR_SUB", "OBEREG", "STATSTRU", "STATNAME", "STATADDR", "LONGITUD", "LATITUDE", "INCITSST", "INCITSCO", "GNISPLAC", "CNTYPOP", "CENTRACT", "CENBLOCK", "CDCODE", "CBSA", "MICROF", "GEOMATCH")) %>% 
  mutate(LOCALE_ADD_DESCR = case_when(
                LOCALE_ADD == 11 ~ "City Large",
                LOCALE_ADD == 12 ~ "City Midsize",
                LOCALE_ADD == 13 ~ "City Small",
                LOCALE_ADD == 21 ~ "Suburban Large",
                LOCALE_ADD == 22 ~ "Suburban Midsize",
                LOCALE_ADD == 23 ~ "Suburban Small",
                LOCALE_ADD == 31 ~ "Town Fringe",
                LOCALE_ADD == 32 ~ "Town Distant",
                LOCALE_ADD == 33 ~ "Town Remote",
                LOCALE_ADD == 41 ~ "Rural Fringe",
                LOCALE_ADD == 42 ~ "Rural Distant",
                LOCALE_ADD == 43 ~ "Rural Remote",
                LOCALE == 11 ~ "City Large",
                LOCALE == 12 ~ "City Midsize",
                LOCALE == 13 ~ "City Small",
                LOCALE == 21 ~ "Suburban Large",
                LOCALE == 22 ~ "Suburban Midsize",
                LOCALE == 23 ~ "Suburban Small",
                LOCALE == 31 ~ "Town Fringe",
                LOCALE == 32 ~ "Town Distant",
                LOCALE == 33 ~ "Town Remote",
                LOCALE == 41 ~ "Rural Fringe",
                LOCALE == 42 ~ "Rural Distant",
                LOCALE == 43 ~ "Rural Remote")
           )

# Write to s3 bucket
s3write_using(erate_imls, FUN = write.csv, bucket = "erate-data/data", object = "erate_imls.csv")

print("erate_imls written to s3")

# Fully join the IMLS datasets and then left join erate_libs
imls_erate <- imls_outlets %>% 
  full_join(imls_ae,
            by = c("STABR", "FSCSKEY", "FSCS_SEQ", "C_FSCS", "LIBID", "LIBNAME", "ADDRESS", "CITY", "ZIP", "ZIP4", "CNTY", "PHONE", "YR_SUB", "OBEREG", "STATSTRU", "STATNAME", "STATADDR", "LONGITUD", "LATITUDE", "INCITSST", "INCITSCO", "GNISPLAC", "CNTYPOP", "CENTRACT", "CENBLOCK", "CDCODE", "CBSA", "MICROF", "GEOMATCH")) %>% 
  left_join(geo_string_match %>% 
           select('ros_entity_number', 'FSCSKEY', 'FSCS_SEQ'),
           by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  left_join(erate_libs %>% 
              select(-FSCSKEY, -FSCS_SEQ),
            by = c("ros_entity_number")) %>% 
  # Create a Description column for LOCALE_ADD and fill based on condition
  mutate(LOCALE_ADD_DESCR = case_when(
                LOCALE_ADD == 11 ~ "City Large",
                LOCALE_ADD == 12 ~ "City Midsize",
                LOCALE_ADD == 13 ~ "City Small",
                LOCALE_ADD == 21 ~ "Suburban Large",
                LOCALE_ADD == 22 ~ "Suburban Midsize",
                LOCALE_ADD == 23 ~ "Suburban Small",
                LOCALE_ADD == 31 ~ "Town Fringe",
                LOCALE_ADD == 32 ~ "Town Distant",
                LOCALE_ADD == 33 ~ "Town Remote",
                LOCALE_ADD == 41 ~ "Rural Fringe",
                LOCALE_ADD == 42 ~ "Rural Distant",
                LOCALE_ADD == 43 ~ "Rural Remote",
                LOCALE == 11 ~ "City Large",
                LOCALE == 12 ~ "City Midsize",
                LOCALE == 13 ~ "City Small",
                LOCALE == 21 ~ "Suburban Large",
                LOCALE == 22 ~ "Suburban Midsize",
                LOCALE == 23 ~ "Suburban Small",
                LOCALE == 31 ~ "Town Fringe",
                LOCALE == 32 ~ "Town Distant",
                LOCALE == 33 ~ "Town Remote",
                LOCALE == 41 ~ "Rural Fringe",
                LOCALE == 42 ~ "Rural Distant",
                LOCALE == 43 ~ "Rural Remote")
           )

# Write to s3 bucket
s3write_using(imls_erate, FUN = write.csv, bucket = "erate-data/data", object = "imls_erate.csv")

print("imls_erate written to s3")

imls_outlet_erate <- imls_outlets %>% 
  left_join(geo_string_match %>% 
           select('ros_entity_number', 'FSCSKEY', 'FSCS_SEQ'),
           by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  left_join(erate_libs %>% 
              select(-FSCSKEY, -FSCS_SEQ),
            by = c("ros_entity_number")) %>% 
  mutate(LOCALE_ADD_DESCR = case_when(
                LOCALE == 11 ~ "City Large",
                LOCALE == 12 ~ "City Midsize",
                LOCALE == 13 ~ "City Small",
                LOCALE == 21 ~ "Suburban Large",
                LOCALE == 22 ~ "Suburban Midsize",
                LOCALE == 23 ~ "Suburban Small",
                LOCALE == 31 ~ "Town Fringe",
                LOCALE == 32 ~ "Town Distant",
                LOCALE == 33 ~ "Town Remote",
                LOCALE == 41 ~ "Rural Fringe",
                LOCALE == 42 ~ "Rural Distant",
                LOCALE == 43 ~ "Rural Remote")
           )

# Write to s3 bucket
s3write_using(imls_outlet_erate, FUN = write.csv, bucket = "erate-data/data", object = "imls_outlet_erate.csv")

print("imls_outlet_erate written to s3")

imls_ae_erate <- imls_ae %>% 
  left_join(geo_string_match %>% 
           select('ros_entity_number', 'FSCSKEY', 'FSCS_SEQ'),
           by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  left_join(erate_libs %>% 
              select(-FSCSKEY, -FSCS_SEQ),
            by = c("ros_entity_number")) %>% 
  mutate(LOCALE_ADD_DESCR = case_when(
                LOCALE_ADD == 11 ~ "City Large",
                LOCALE_ADD == 12 ~ "City Midsize",
                LOCALE_ADD == 13 ~ "City Small",
                LOCALE_ADD == 21 ~ "Suburban Large",
                LOCALE_ADD == 22 ~ "Suburban Midsize",
                LOCALE_ADD == 23 ~ "Suburban Small",
                LOCALE_ADD == 31 ~ "Town Fringe",
                LOCALE_ADD == 32 ~ "Town Distant",
                LOCALE_ADD == 33 ~ "Town Remote",
                LOCALE_ADD == 41 ~ "Rural Fringe",
                LOCALE_ADD == 42 ~ "Rural Distant",
                LOCALE_ADD == 43 ~ "Rural Remote")
           )

# Write to s3 bucket
s3write_using(imls_ae_erate, FUN = write.csv, bucket = "erate-data/data", object = "imls_ae_erate.csv")

print("imls_ae_erate written to s3")

# Make a smaller dataset for use on Shiny dashboard
erate_imls_compact <- erate_imls %>%
  select(ros_entity_number:original_allocation, discount_by_ros, ros_allocation,
         FSCSKEY, FSCS_SEQ, STABR, LIBID, LIBNAME, ADDRESS, CITY, ZIP, CNTY, LOCALE_ADD_DESCR)

# Write to s3 bucket
s3write_using(erate_imls_compact, FUN = write.csv, bucket = "erate-data/data", object = "erate_imls_compact.csv")

print("erate_imls_compact written to s3")

print(Sys.time())

rm(list = ls())
