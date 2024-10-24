library(DBI)
library(duckdb)
library(dplyr)
library(dm)

fs::dir_create("sec")

# https://www.sec.gov/files/structureddata/data/form-d-data-sets/2023q4_d.zip

sec_paths <- fs::dir_ls("sec")

if (FALSE) {
  purrr::walk(sec_paths, ~ unzip(.x, exdir = "sec-unzipped"))
}

if (fs::file_exists("formd.duckdb")) {
  fs::file_delete("formd.duckdb")
}

# Form D ------------------------------------------------------------------------

duckdb_con <- dbConnect(duckdb())

form_d <- duckdb::tbl_file(
  duckdb_con,
  "sec-unzipped/2023Q4_d/FORMDSUBMISSION.tsv"
)

# explore column names
try(names(form_d))
colnames(form_d) |>
  writeLines()

# duplicates check
form_d |>
  rename_with(tolower) |>
  summarise(.by = accessionnumber, n = n()) |>
  filter(n > 1) |>
  count()

# Issuers ----------------------------------------------------------------------

issuers <- duckdb::tbl_file(
  duckdb_con,
  "sec-unzipped/2023Q4_d/ISSUERS.tsv"
)

# explore column names
colnames(issuers) |>
  writeLines()

# duplicates check
issuers |>
  rename_with(tolower) |>
  summarise(.by = accessionnumber, n = n()) |>
  filter(n > 1) |>
  count()

issuers |>
  rename_with(tolower) |>
  summarise(.by = c(accessionnumber, issuer_seq_key), n = n()) |>
  filter(n > 1) |>
  count()


# Offering ---------------------------------------------------------------------

offering <- duckdb::tbl_file(
  duckdb_con,
  "sec-unzipped/2023Q4_d/OFFERING.tsv"
)

# explore column names
colnames(offering) |>
  writeLines()

# duplicates check
offering |>
  rename_with(tolower) |>
  summarise(.by = accessionnumber, n = n()) |>
  filter(n > 1) |>
  count()

# Recipients -------------------------------------------------------------------

recipients <- duckdb::tbl_file(
  duckdb_con,
  "sec-unzipped/2023Q4_d/RECIPIENTS.tsv"
)

# explore column names
colnames(recipients) |>
  writeLines()

recipients |>
  rename_with(tolower) |>
  summarise(.by = accessionnumber, n = n()) |>
  filter(n > 1) |>
  count()

recipients |>
  rename_with(tolower) |>
  summarise(.by = c(accessionnumber, recipient_seq_key), n = n()) |>
  filter(n > 1) |>
  count()

# dm ---------------------------------------------------------------------------

dm_formd_set_pk_fk <- function(dm) {

  stopifnot(is_dm(dm))

  dm |>
    dm_add_pk(form_d, ACCESSIONNUMBER, check = TRUE) |>
    dm_add_pk(issuers, c(ACCESSIONNUMBER, ISSUER_SEQ_KEY)) |>
    dm_add_pk(offering, ACCESSIONNUMBER) |>
    dm_add_pk(recipients, c(ACCESSIONNUMBER, RECIPIENT_SEQ_KEY)) |>
    dm_add_fk(issuers, ACCESSIONNUMBER, form_d) |>
    dm_add_fk(offering, ACCESSIONNUMBER, form_d) |>
    dm_add_fk(recipients, ACCESSIONNUMBER, form_d)

}

formd_dm_keys <-
  dm(form_d, issuers, offering, recipients) |>
  dm_formd_set_pk_fk()

dm_draw(formd_dm_keys)

dm_examine_constraints(formd_dm_keys)

# Analyze ----------------------------------------------------------------------

base_dat <-
  formd_dm_keys |>
  dm_flatten_to_tbl(.start = issuers) |> # help(dm_flatten_to_tbl)
  rename_with(tolower) |>
  left_join(
    rename_with(pull_tbl(formd_dm_keys, offering), tolower),
    join_by(accessionnumber)
  ) |>
  mutate(
    filing_date = sql("STRPTIME(filing_date, '%d-%b-%Y')")
  ) |>
  mutate(
    filing_date = sql("CAST(filing_date AS DATE)")
  ) |>
  transmute(
    year = lubridate::year(filing_date),
    month = lubridate::month(filing_date),
    accessionnumber,
    entityname,
    stateorcountry,
    stateorcountrydescription,
    entitytype,
    federalexemptions_items_list,
    submissiontype,
    totalamountsold,
    totalofferingamount = as.numeric(
      sql("nullif(totalofferingamount, 'Indefinite')")
    )
  )

# submissiontype per month ----

type_dat <-
  base_dat |>
  count(year, month, submissiontype) |>
  collect() |>
  mutate(filing_date = lubridate::make_date(year, month)) |>
  arrange(year, month)

library(ggplot2)
type_dat |>
  mutate(dte = lubridate::make_date(year, month)) |>
  ggplot(aes(dte, n, fill = submissiontype)) +
  geom_col(position = "dodge") +
  theme_minimal() +
  labs(title = "Form D submission Q1")

# amount sold per state ----
base_dat |>
  summarise(
    .by = stateorcountrydescription,
    tot_sold = sum(totalamountsold, na.rm = TRUE),
    tot_offered = sum(totalofferingamount, na.rm = TRUE)
  ) |>
  filter(tot_sold > 0) |>
  collect() |>
  mutate(
    stateorcountrydescription = forcats::fct_reorder(
      stateorcountrydescription,
      tot_sold
    )
  ) |>
  ggplot(aes(stateorcountrydescription, tot_sold)) +
  geom_col() +
  coord_flip() +
  theme_minimal()


# rank ten best raising capital ----
base_dat |>
  dbplyr::window_order(totalamountsold) |>
  mutate(row_num = row_number()) |>
  filter(between(row_num, max(row_num) - 9L, max(row_num))) |>
  collect() |>
  arrange(desc(totalamountsold)) |>
  select(entityname, totalamountsold)

# Multi ------------------------------------------------------------------------

q_form_d <-
  "CREATE OR REPLACE TABLE form_d AS
   SELECT *
   FROM read_csv(
          'sec-unzipped/*/FORMDSUBMISSION.tsv',
          types={'FILING_DATE': 'VARCHAR'}
        );
  "

dbExecute(duckdb_con, q_form_d)

q_issuers <-
  "CREATE OR REPLACE TABLE issuers AS
   SELECT *
   FROM read_csv(
          'sec-unzipped/*/ISSUERS.tsv'
        );
  "

dbExecute(duckdb_con, q_issuers)

q_offering <-
  "CREATE OR REPLACE TABLE offering AS
   SELECT *
   FROM read_csv('sec-unzipped/*/OFFERING.tsv');
  "

dbExecute(duckdb_con, q_offering)

q_recipients <-
  "CREATE OR REPLACE TABLE recipients AS
   SELECT *
   FROM read_csv('sec-unzipped/*/RECIPIENTS.tsv');
  "

dbExecute(duckdb_con, q_recipients)

stopifnot(length(dbListTables(duckdb_con)) == 4L)

formd_dm_ts <-
  dm::dm_from_con(duckdb_con) |>
  dm_formd_set_pk_fk()

# resolve FILING_DATE format
formd_dm_ts <-
  formd_dm_ts |>
  dm_zoom_to(form_d) |>
  mutate(
    DATE_LONG = if_else(
      str_length(FILING_DATE) > 11L,
      FILING_DATE,
      NA_character_
    ),
    DATE_LONG = str_sub(DATE_LONG, 1L, 10L),
    DATE_LONG = sql("CAST(DATE_LONG AS DATE)")
  ) |>
  mutate(
    DATE_SHORT = if_else(
      str_length(FILING_DATE) <= 11L,
      FILING_DATE,
      NA_character_
    )
  ) |>
  mutate(
    DATE_SHORT = sql("STRPTIME(DATE_SHORT, '%d-%b-%Y')")
  ) |>
  mutate(DATE_SHORT = sql("CAST(DATE_SHORT AS DATE)")) |>
  mutate(FILING_DATE = coalesce(DATE_LONG, DATE_SHORT)) |>
  select(-starts_with("DATE")) |>
  dm_update_zoomed()

formd_dm_ts |>
  pull_tbl(form_d) |>
  select(FILING_DATE) |>
  pull() |>
  class()

states <- toupper(c("California", "Texas", "New York"))

n_sub_ts <-
  formd_dm_ts |>
  dm_flatten_to_tbl(.start = issuers) |>
  rename_with(tolower) |>
  filter(stateorcountrydescription %in% states) |>
  transmute(
    accessionnumber,
    filing_date,
    stateorcountrydescription,
    year = lubridate::year(filing_date),
    month = lubridate::month(filing_date)
  ) |>
  count(year, month, stateorcountrydescription) |>
  collect() |>
  mutate(dte = lubridate::make_date(year, month))

# number of submission per state
n_sub_ts |>
  ggplot(aes(dte, n, color = stateorcountrydescription)) +
  geom_line() +
  theme_minimal() +
  labs(title = "Form D submission per state across years")

dbDisconnect(duckdb_con)
