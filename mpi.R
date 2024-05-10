library(tidyverse)
library(httr2)
library(haven)
library(writexl)

# Data download ----
srcs <- 
  tribble(~survey,  ~year, ~id,  ~modules,
          "enaho",   2022,  784,  c("01", "02", "03", "04", "05", "84"),
          "enaho",   2018,  634,  c("01", "02", "03", "04", "05", "84"),
          # "enpove",  2022,  769,  c("1711", "1712", "1715", "1716", "1717", "1718"),
          "enpove", 2022, 769, c("1711", "1721"), # consolidated database
          "enpove",  2018,  642,  c("1416", "1417", "1421", "1422", "1423", "1426")) |> 
  unnest_longer(modules, values_to = "module")

if (!fs::dir_exists("data")) {
  requests <- map2(srcs$id, srcs$module,
                   \(x, y) request(glue::glue("https://proyectos.inei.gob.pe/iinei/srienaho/descarga/",
                                              "SPSS/{x}-Modulo{y}.zip")))
  archives <- map(seq_along(srcs$module),
                  \(x) fs::file_temp(ext = "zip")) |> unlist()
  exdirs <- map2(srcs$survey, srcs$year,
                 \(x, y) fs::path("data", x, y)) |> unlist()
  req_perform_parallel(requests, paths = archives, progress = "Downloading data")
  fs::dir_create(exdirs)
  walk2(archives, exdirs,
        \(x, y) unzip(x, exdir = y, junkpaths = TRUE, unzip = "unzip"))
}

# hacky, but gets the job done for now...
possibly(fs::file_delete)(fs::path("data", "enaho", 2018, "Enaho01a-2018-300A", ext = "sav"))

# Utils ----
dataf <- function(survey, year, module) {
  fs::dir_ls(fs::path("data", survey, year), 
             glob = glue::glue("*{module}*.sav")) |> 
    read_sav()
}

enaho <- function(...) dataf("enaho", ...)
enpove <- function(...) dataf("enpove", ...)

# ENAHO 2022 ----
enaho2022_data <- 
  list(enaho(2022, 400) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(402|403|409|419)")),
       enaho(2022, 300) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(301|302|306|307|308)"), eduwt = FACTORA07),
       enaho(2022, 500) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(510|511|513|518|519|520|521|556|558)|OCU500"), empwt = FAC500A),
       enaho(2022, 100) |> 
         select(CONGLOME, VIVIENDA, HOGAR, RESULT,
                matches("P(101|102|103|104|110|111|112|113|114)")),
       enaho(2022, "800A") |> 
         select(CONGLOME, VIVIENDA, HOGAR, 
                matches("P801")),
       enaho(2022, 200) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                AÑO, MES, DOMINIO, ESTRATO, P203:P209, hhwt = FACPOB07)) |> 
  reduce(full_join) |> 
  filter(RESULT %in% 1:2, P203 != 0, P204 != 2, !ESTRATO %in% 6:8) |> 
  fill(P101, P102, P103, P103A, P104) |> 
  add_count(CONGLOME, VIVIENDA, name = "vivpob")

enaho2022_mpi_data <- 
  enaho2022_data |> 
  zap_labels() |> 
  mutate(clustid = CONGLOME, vivid = VIVIENDA, hhid = HOGAR, pid = CODPERSO,
         hhwt, eduwt, empwt,
         age = P208A, sex = P207,
         location = if_else(DOMINIO==8, "Lima Metro", "Other Urban"),
         health_access = 
           (P4021|P4022|P4023|P4024) &
           ((P40310|P40311|P40313) | 
              (P40314 & !P4095)),
         health_insurance = P4191+P4192+P4193+P4194+P4195+P4196+P4197+P4198==16,
         education_participation =
           between(age, 5, 19) &
           (((P306==2 | (P306==1 & P307==2)) & P301A < 6) |
              (P306==1 & P307==1 & P308A %in% c(1:3, 7) &
                 ((age-(c(3,6,12,NA,NA,NA,6)[P308A]+coalesce(P308C, P308B))>2))) |
              ((P306==2 | (P306==1 & P307==2)) & P301A != 12)),
         education_attainment =
           (age >= 20) &
           (P301A < if_else(age >= (2022-(1994-13)), 4, 6)) &
           ((P306==2 |
               (P306==1 & P307==2) |
               (P301A %in% c(c(1,2,3,12)) & P302==2))), # ENPOVE doesn't cover adult education. should we keep this?
         housing_structure = 
           P102 %in% c(3, 4, 5, 6, 8, 9) |
           P103A %in% c(5, 6, 7, 8) |
           P103  %in% c(6, 7) |
           P101  %in% c(6, 7, 8),
         housing_overcrowding = vivpob/P104 >= 3,
         wash_water = P110 %in% c(4, 5, 6, 7, 8), # not covering service interruptions
         wash_sanitation = P111A %in% c(5, 6, 7, 9),
         energy_electricity = P1121 == 0,
         energy_fuel = P1138 == 0 & P113A %in% c(5, 6, 9, 7),
         employment_employment = 
           age >= 14 &
           (OCU500 %in% c(2, 3) | 
              (OCU500==1 & 
                 ((P519==1 & (P513+replace_na(P518,0))<35) | (P519==2 & P520<35)) &
                 P521==1 & P521A==1)),
         employment_contract = 
           age >= 14 &
           OCU500 == 1 &
           !is.na(P511A) &
           P511A == 7,
         # employment_pension = # ENPOVE doesn't cover pensions. but VENz aren't entitled. keep or remove?
         #   (between(age, 14, 64) & 
         #      OCU500==1 & 
         #      (P558A5==5 | (P558A5==0 & make_date(P558B2, P558B1)<(make_date(AÑO,MES)-years(1))))) |
         #   (age >= 65 &
         #      OCU500==1 &
         #      ((P558A5==5 | (P558A5==0 & make_date(P558B2, P558B1)<(make_date(AÑO,MES)-years(1)))) |
         #         (P5564A+P5565A+P5567A==6))) |
         #   (age >= 65 &
         #      OCU500 %in% c(2, 3) &
         #      (P5564A+P5565A+P5567A==6)),
         connectivity_participation = P801_19==19, # not excluding those who aren't interested
         connectivity_ict = P1145 == 1 | (!P1141 & !P1142 & !P1144),
         .keep = "none")

# ENPOVE 2022 ----
# FIXME: add occupational status variable OCU500 instead of using is.na()
# fixes the 70 people who responded to both employment and unemployment questions
enpove2022_data <- 
  left_join(enpove(2022, 100), enpove(2022, 200)) |> 
  filter(RESFIN %in% 1:2, P15==1, P208==1) |> 
  add_count(CONGLOMERADO, VIVIENDA, NHOGAR, name = "vivpob")

enpove2022_mpi_data <- 
  enpove2022_data |> 
  zap_labels() |> 
  mutate(clustid = CONGLOMERADO, vivid = VIVIENDA, hhid = NHOGAR, pid = P200_N,
         hhwt = factorfinal,
         age = if_else(is.na(P205_A), 0, P205_A), sex = P204,
         location = if_else(DEPARTAMENTO=="LIMA", "Lima Metro", "Other Urban"),
         health_access = 
           ((P405_1|P405_2|P405_3|P405_4) &
              ((P406_5|P406_6|P406_7) |
                 (P406_8 & !P407_5))),
         health_insurance = P401_5==1,
         education_participation = 
           between(age, 5, 19) &
           (((P505==2 | (P505==1 & P506==2)) & 
               ((P501A==3) | (P501A==1 & P501<6) | (P501A==2 & P501B<6))) |
              (P505==1 & P506==1 & P507_1<=4 & ((age-(c(3,6,12,6)[P507_1]+coalesce(P507_2, P507_3)))>2))),
         education_attainment = 
           age >= 20 &
           ((P501A==3) | (P501A==1 & P501<6) | (P501A==2 & P501B<6)) &
           (P505==2 | (P505==1 & P506==2)),
         housing_structure = 
           P102 %in% c(3, 4, 5, 6, 8, 9) |
           P103 %in% c(5, 6, 7, 8) |
           P104  %in% c(6, 7) |
           P101 %in% c(6, 7, 8),
         housing_overcrowding = vivpob/P105 >= 3,
         wash_water = P108_1 %in% c(4, 5),
         wash_sanitation = P108_2 == 5,
         energy_electricity = P108_3 != 1,
         energy_fuel = P109 == 4,
         employment_employment = 
           age >= 14 &
           ((!is.na(P626) & (P627 | (P629&P630))) |
              (!is.na(P612) & 
                 ((P616==1 & P615_T<35) | (P616==2 & P616A<35)) &
                 P617==1 & P618==1)),
         employment_contract =
           age >= 14 &
           !is.na(P613) &
           P613 == 2,
         # employment_pension = age >= 14, # FIXME: keep or remove?
         connectivity_participation = age >= 5 & P707_9==1,
         connectivity_ict = P108_4==2 & P110_6==2 & P110_7==2,
         .keep = "none")

# ENAHO 2018 ----
enaho2018_data <- 
  list(enaho(2018, 400) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(402|403|409|419)")),
       enaho(2018, 300) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(301|302|306|307|308)"), eduwt = FACTORA07),
       enaho(2018, 500) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                matches("P(510|511|513|518|519|520|521|556|558)|OCU500"), empwt = FAC500A),
       enaho(2018, 100) |> 
         select(CONGLOME, VIVIENDA, HOGAR, RESULT,
                matches("P(101|102|103|104|110|111|112|113|114)")),
       enaho(2018, "800A") |> 
         select(CONGLOME, VIVIENDA, HOGAR, 
                matches("P801")),
       enaho(2018, 200) |> 
         select(CONGLOME, VIVIENDA, HOGAR, CODPERSO, 
                AÑO, MES, DOMINIO, ESTRATO, P203:P209, hhwt = FACPOB07)) |> 
  reduce(full_join) |> 
  filter(RESULT %in% 1:2, P203 != 0, P204 != 2, !ESTRATO %in% 6:8) |> 
  fill(P101, P102, P103, P103A, P104) |> 
  add_count(CONGLOME, VIVIENDA, name = "vivpob")

enaho2018_mpi_data <- 
  enaho2018_data |> 
  zap_labels() |> 
  mutate(clustid = CONGLOME, vivid = VIVIENDA, hhid = HOGAR, pid = CODPERSO,
         hhwt, eduwt, empwt,
         age = P208A, sex = P207,
         location = if_else(DOMINIO==8, "Lima Metro", "Other Urban"),
         health_access = 
           (P4021|P4022|P4023|P4024) &
           ((P40310|P40311|P40313) | 
              (P40314 & !P4095)),
         health_insurance = P4191+P4192+P4193+P4194+P4195+P4196+P4197+P4198==16,
         education_participation =
           between(age, 5, 19) &
           (((P306==2 | (P306==1 & P307==2)) & P301A < 6) |
              (P306==1 & P307==1 & P308A %in% c(1:3, 7) &
                 ((age-(c(3,6,12,NA,NA,NA,6)[P308A]+coalesce(P308C, P308B))>2))) |
              ((P306==2 | (P306==1 & P307==2)) & P301A != 12)),
         education_attainment =
           (age >= 20) &
           (P301A < if_else(age >= (2022-(1994-13)), 4, 6)) &
           ((P306==2 |
               (P306==1 & P307==2) |
               (P301A %in% c(c(1,2,3,12)) & P302==2))), # ENPOVE doesn't cover adult education. should we keep this?
         housing_structure = 
           P102 %in% c(3, 4, 5, 6, 8, 9) |
           P103A %in% c(5, 6, 7, 8) |
           P103  %in% c(6, 7) |
           P101  %in% c(6, 7, 8),
         housing_overcrowding = vivpob/P104 >= 3,
         wash_water = P110 %in% c(4, 5, 6, 7, 8), # not covering service interruptions
         wash_sanitation = P111A %in% c(5, 6, 7, 9),
         energy_electricity = P1121 == 0,
         energy_fuel = P1138 == 0 & P113A %in% c(5, 6, 9, 7),
         employment_employment = 
           age >= 14 &
           (OCU500 %in% c(2, 3) | 
              (OCU500==1 & 
                 ((P519==1 & (P513+replace_na(P518,0))<35) | (P519==2 & P520<35)) &
                 P521==1 & P521A==1)),
         employment_contract = 
           age >= 14 &
           OCU500 == 1 &
           !is.na(P511A) &
           P511A == 7,
         # employment_pension = # ENPOVE doesn't cover pensions. but VENz aren't entitled. keep or remove?
         #   (between(age, 14, 64) & 
         #      OCU500==1 & 
         #      (P558A5==5 | (P558A5==0 & make_date(P558B2, P558B1)<(make_date(AÑO,MES)-years(1))))) |
         #   (age >= 65 &
         #      OCU500==1 &
         #      ((P558A5==5 | (P558A5==0 & make_date(P558B2, P558B1)<(make_date(AÑO,MES)-years(1)))) |
         #         (P5564A+P5565A+P5567A==6))) |
         #   (age >= 65 &
         #      OCU500 %in% c(2, 3) &
         #      (P5564A+P5565A+P5567A==6)),
         connectivity_participation = P801_19==19, # not excluding those who aren't interested
         connectivity_ict = P1145 == 1 | (!P1141 & !P1142 & !P1144),
         .keep = "none")

# ENPOVE 2018 ----
enpove2018_data <- 
  list(enpove(2018, 100),
       enpove(2018, 200),
       enpove(2018, 400),
       enpove(2018, 500),
       enpove(2018, 600),
       enpove(2018, 700)) |> 
  reduce(full_join) |> 
  filter(RESULTADO_FINAL_VIVIENDA==1, P207==1) |> 
  add_count(CONGLOMERADO, VIVIENDA, HOGAR, name = "vivpob")

enpove2018_mpi_data <- 
  enpove2018_data |> 
  zap_labels() |> 
  mutate(clustid = CONGLOMERADO, vivid = VIVIENDA, hhid = HOGAR, pid = CODPERSO,
         hhwt = FACTOR_DE_EXPANSION,
         age = P205_A, sex = P204,
         location = if_else(NOMBDEPA %in% c("LIMA", "CALLAO"), "Lima Metro", "Other Urban"),
         health_access =
           ((P405_1|P405_2|P405_3|P405_4) &
              ((P406_5|P406_6|P406_7) |
                 (P406_8 & !P407_5))),
         health_insurance = P401_5==1,
         education_participation = 
           between(age, 5, 19) &
           ((P505==2 & P501<6) |
              (P505==1 & P506_1<=3 & ((age-(c(3,6,12)[P506_1]+coalesce(P506_2, P506_3)))>2))),
         education_attainment = age >= 20 & P505==2 & P501<6,
         housing_structure = 
           P102 %in% c(3, 4, 5, 6, 8, 9) |
           P103 %in% c(5, 6, 7, 8) |
           P104  %in% c(6, 7) |
           P101 %in% c(6, 7, 8),
         housing_overcrowding = vivpob/P105 >= 3,
         wash_water = P110 >= 4,
         wash_sanitation = P112 >= 5,
         energy_electricity = P109 != 1,
         energy_fuel = P111 == 4,
         employment_employment =
           age >= 14 &
           (OCU600A %in% c(2, 3) |
              (OCU600A==1 & P611<35 & P611A==1)),
         employment_contract =
           age >= 14 &
           OCU600A == 1 &
           !is.na(P609) &
           P609 == 2,
         connectivity_participation = age >= 5 & P707_9==1,
         connectivity_ict = P113_6==2, # missing fixed telephony and internet connectivity...
         .keep = "none")

# MPI dashboard ----
# enaho2022_mpi_stats <- 
#   enaho2022_mpi_data |> 
#   mutate(across(c(eduwt, empwt), \(x) replace_na(x, 0))) |> 
#   summarize(across(health_access:health_insurance, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             education_participation = weighted.mean(education_participation, eduwt*between(age,5,19), na.rm = TRUE),
#             education_attainment = weighted.mean(education_attainment, eduwt*(age>=20), na.rm = TRUE),
#             across(housing_structure:energy_fuel, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             across(employment_employment:employment_contract, \(x) weighted.mean(x, empwt*(age>=14), na.rm = TRUE)),
#             across(connectivity_participation:connectivity_ict, \(x) weighted.mean(x, hhwt, na.rm = TRUE))) |> 
#   pivot_longer(everything(), names_to = c("dim", "ind"), names_sep = "_", values_to = "p")


enpove2018_mpi_stats <- 
  enpove2018_mpi_data |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")
  

# enpove2018_mpi_stats <- 
#   enpove2018_mpi_data |> 
#   summarize(across(health_access:health_insurance, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             education_participation = weighted.mean(education_participation, hhwt*between(age,5,19), na.rm = TRUE),
#             education_attainment = weighted.mean(education_attainment, hhwt*(age>=20), na.rm = TRUE),
#             across(housing_structure:energy_fuel, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             across(employment_employment:employment_contract, \(x) weighted.mean(x, hhwt*(age>=14), na.rm = TRUE)),
#             across(connectivity_participation:connectivity_ict, \(x) weighted.mean(x, hhwt, na.rm = TRUE))) |> 
#   pivot_longer(everything(), names_to = c("dim", "ind"), names_sep = "_", values_to = "p")

enpove2022_mpi_stats <- 
  enpove2022_mpi_data |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")


# enpove2022_mpi_stats <-
#   enpove2022_mpi_data |>
#   summarize(across(health_access:health_insurance, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             education_participation = weighted.mean(education_participation, hhwt*between(age,5,19), na.rm = TRUE),
#             education_attainment = weighted.mean(education_attainment, hhwt*(age>=20), na.rm = TRUE),
#             across(housing_structure:energy_fuel, \(x) weighted.mean(x, hhwt, na.rm = TRUE)),
#             across(employment_employment:employment_contract, \(x) weighted.mean(x, hhwt*(age>=14), na.rm = TRUE)),
#             across(connectivity_participation:connectivity_ict, \(x) weighted.mean(x, hhwt, na.rm = TRUE))) |>
#   pivot_longer(everything(), names_to = c("dim", "ind"), names_sep = "_", values_to = "p")

mpi_stats <- 
  bind_rows("2018" = enpove2018_mpi_stats, 
            "2022" = enpove2022_mpi_stats, 
            .id = "year") |> 
  rename("ENPOVE" = year) |>
  mutate(across('dim', str_replace, 'health', '01Salud')) |> 
  mutate(across('dim', str_replace, 'education', '02Educación')) |> 
  mutate(across('dim', str_replace, 'housing', '03Vivienda')) |> 
  mutate(across('dim', str_replace, 'wash', '04Agua y saneamiento')) |> 
  mutate(across('dim', str_replace, 'energy', '05Energía')) |> 
  mutate(across('dim', str_replace, 'employment', '06Empleo y provisión social')) |> 
  mutate(across('dim', str_replace, 'connectivity', '07Conectividad')) |> 
  mutate(ind = case_when(dim == "02Educación" & ind == "participation" ~ "01Asistencia y rezago escolar",
                         TRUE ~ ind)) |> 
  mutate(ind = case_when(dim == "07Conectividad" & ind == "participation" ~ "01Pertenencia a redes",
                         TRUE ~ ind)) |> 
  mutate(across('ind', str_replace, 'access', '01Atención médica')) |> 
  mutate(across('ind', str_replace, 'insurance', '02Seguro de salud')) |> 
  mutate(across('ind', str_replace, 'attainment', '02Logro educativo en adultos de 20 años o más')) |> 
  mutate(across('ind', str_replace, 'structure', '01Materiales de la vivienda')) |> 
  mutate(across('ind', str_replace, 'overcrowding', '02Hacinamiento')) |> 
  mutate(across('ind', str_replace, 'water', '01Agua')) |> 
  mutate(across('ind', str_replace, 'sanitation', '02Saneamiento')) |> 
  mutate(across('ind', str_replace, 'electricity', '01Electricidad')) |> 
  mutate(across('ind', str_replace, 'fuel', '02Combustible para cocinar')) |> 
  mutate(across('ind', str_replace, 'employment', '01Empleo')) |> 
  mutate(across('ind', str_replace, 'contract', '02Formalidad')) |> 
  mutate(across('ind', str_replace, 'ict', '02Tecnología de la información y la comunicación')) |> 
  arrange(dim, ind) |> 
  mutate(across(c(dim, ind), ~str_remove_all(., "[01234567]"))) |> 
  group_by(ENPOVE, dim, ind) |> 
  pivot_wider(names_from = type, values_from = p) |> 
  mutate(num = round(num, digits = 2)) |> 
  mutate(denom = round(denom, digits = 2)) |> 
  relocate(num, .before = denom)


# Chart 1
mpi_stats |> 
  select(ENPOVE:mean) |> 
  rename(p = mean) |> 
  ggplot() +
  geom_col(aes(p, ind, fill = ENPOVE), 
           position = position_dodge(width = 0.7),
           width = 0.6) +
  scale_y_discrete() +
  geom_text(aes(
    x = p,
    y = fct_rev(factor(ind)),
    group = as.character(ENPOVE),
    label = scales::label_percent(.1)(p)
  ),
  position = position_dodge(width = 0.7),
  hjust = -0.25, size = 8 / .pt
  ) +
  facet_wrap(vars(dim), ncol = 1, scales = "free_y") +
  
  scale_x_continuous(labels = scales::label_percent(),
                     limits = c(0, 1.1)) +
  scale_fill_manual(values = c("2022" = "#00786C", "2018" = "#66D1C1")) +
  guides(fill = guide_legend(nrow = 1)) +
  labs(x = "% privación", y = ""
#       title = "MPI Dashboard", subtitle = "Individual level information"
  ) +
  theme_minimal() +
  theme(legend.position = "top",
        panel.grid = element_blank(),
        axis.text.x = element_blank(),
        strip.text = element_text(face = "bold")
        )

# mpi_stats |> write_xlsx("Chart_1.xlsx", format_headers = FALSE)

# MPI dashboard (disaggregated) ----
# ENPOVE first

enpove2022_mpi_genpop <- 
  enpove2022_mpi_data |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "total")


enpove2022_mpi_sex <- 
  enpove2022_mpi_data |> 
  mutate(sex = if_else(sex == 1, "sex_M", "sex_F")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  group_by(sex) |> 
  arrange(sex) |> 
  filter(row_number()==1) |>   
  select(sex, ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = sex, values_from = p)


enpove2022_mpi_loc <- 
  enpove2022_mpi_data |> 
  mutate(location = if_else(location == "Lima Metro", "location_Lima", "location_Other")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  group_by(location) |> 
  arrange(location) |> 
  filter(row_number()==1) |> 
  select(location, ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = location, values_from = p) |> 
  relocate(location_Lima, .before = location_Other)
  
enpove2022_mpi_age <- 
  enpove2022_mpi_data |> 
  mutate(agegrp = case_when(between(age, 0, 4) ~ "0-4",
                         between(age, 5, 17) ~ "5-17",
                         between(age, 18, 59) ~ "18-59",
                         age >= 60 ~ "60+"),
         agegrp = str_c("age", agegrp, sep = "_")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  group_by(agegrp) |> 
  arrange(agegrp) |> 
  filter(row_number()==1) |> 
  select(agegrp, ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = agegrp, values_from = p) |> 
  mutate(across(starts_with("age"), \(x) if_else(dim %in% c("education", "employment"), NA, x))) |> 
  select(dim, ind, type, `age_0-4`, `age_5-17`, `age_18-59`, `age_60+`)
  

enpove2022_mpi_dashboard <- 
  list(enpove2022_mpi_genpop, enpove2022_mpi_loc, enpove2022_mpi_sex, enpove2022_mpi_age) |> 
  reduce(full_join)

# And now ENAHO
enaho2022_mpi_genpop <- 
  enaho2022_mpi_data |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}")) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}")) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "total")


enaho2022_mpi_sex <- 
  enaho2022_mpi_data |> 
  mutate(sex = if_else(sex == 1, "sex_M", "sex_F")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = sex) |> 
  group_by(sex) |> 
  arrange(sex) |> 
  filter(row_number()==1) |>   
  select(sex, ends_with(c("_mean", "_num", "_denom"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = sex, values_from = p)
  
  
enaho2022_mpi_loc <- 
  enaho2022_mpi_data |> 
  mutate(location = if_else(location == "Lima Metro", "location_Lima", "location_Other")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = location) |> 
  group_by(location) |> 
  arrange(location) |> 
  filter(row_number()==1) |> 
  select(location, ends_with(c("_mean", "_num", "_denom"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = location, values_from = p) |> 
  relocate(location_Lima, .before = location_Other)
  

enaho2022_mpi_age <- 
  enaho2022_mpi_data |> 
  mutate(agegrp = case_when(between(age, 0, 4) ~ "0-4",
                            between(age, 5, 17) ~ "5-17",
                            between(age, 18, 59) ~ "18-59",
                            age >= 60 ~ "60+"),
         agegrp = str_c("age", agegrp, sep = "_")) |> 
  mutate(across(health_access:health_insurance, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(education_participation, list(num = ~sum(hhwt * between(age,5,19) * ., na.rm = TRUE), denom = ~sum(hhwt * between(age,5,19), na.rm = TRUE), mean = ~weighted.mean(., hhwt*between(age,5,19), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(education_attainment, list(num = ~sum(hhwt * (age>=20) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=20), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=20), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |>
  mutate(across(housing_structure:energy_fuel, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  mutate(across(employment_employment:employment_contract, list(num = ~sum(hhwt * (age>=14) * ., na.rm = TRUE), denom = ~sum(hhwt * (age>=14), na.rm = TRUE), mean = ~weighted.mean(., hhwt * (age>=14), na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |>
  mutate(across(connectivity_participation:connectivity_ict, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "{.col}_{.fn}"), .by = agegrp) |> 
  group_by(agegrp) |> 
  arrange(agegrp) |> 
  filter(row_number()==1) |> 
  select(agegrp, ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(-1, names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p") |> 
  pivot_wider(names_from = agegrp, values_from = p) |> 
  mutate(across(starts_with("age"), \(x) if_else(dim %in% c("education", "employment"), NA, x))) |> 
  select(dim, ind, type, `age_0-4`, `age_5-17`, `age_18-59`, `age_60+`)
  
enaho2022_mpi_dashboard <- 
  list(enaho2022_mpi_genpop, enaho2022_mpi_loc, enaho2022_mpi_sex, enaho2022_mpi_age) |> 
  reduce(full_join)

# Table 1
mpi_db_20222 <- 
  full_join(enpove2022_mpi_dashboard |> rename_with(\(x) paste0(x, "_ven"), -c(dim, ind, type)),
            enaho2022_mpi_dashboard |> rename_with(\(x) paste0(x, "_per"), -c(dim, ind, type))) |> 
  select(dim, ind,
         type,
         total_ven, total_per,
         location_Lima_ven, location_Lima_per, location_Other_ven, location_Other_per,
         sex_M_ven, sex_M_per, sex_F_ven, sex_F_per,
         `age_0-4_ven`, `age_0-4_per`, `age_5-17_ven`, `age_5-17_per`,
         `age_18-59_ven`, `age_18-59_per`, `age_60+_ven`, `age_60+_per`)

 mpi_db_20222 |> write_xlsx("Table_1.xlsx", format_headers = FALSE)

# MPI index ----
mpi_index <- 
  bind_rows(ven2018 = enpove2018_mpi_data |> mutate(across(clustid:pid, as.character)),
            per2018 = enaho2018_mpi_data |> select(-c(eduwt, empwt)) |> mutate(across(clustid:pid, as.character)),
            ven2022 = enpove2022_mpi_data |> mutate(across(clustid:pid, as.character)),
            per2022 = enaho2022_mpi_data |> select(-c(eduwt, empwt)) |> mutate(across(clustid:pid, as.character)),
            .id = "pop") |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(pop, clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  mutate(across(mpi_poor, list(num = ~sum(hhwt * ., na.rm = TRUE), denom = ~sum(hhwt, na.rm = TRUE), mean = ~weighted.mean(., w = hhwt, na.rm = TRUE)), .names = "H_{.fn}"), .by = pop) |> 
  mutate(across(deprivations, list(num = ~sum(hhwt * mpi_poor * (./14), na.rm = TRUE), denom = ~sum(hhwt * mpi_poor, na.rm = TRUE), mean = ~weighted.mean(./14, hhwt*mpi_poor, na.rm = TRUE)), .names = "A_{.fn}"), .by = pop) |> 
  mutate(M0_mean = H_mean*A_mean, .by = pop) |> 
  group_by(pop) |> 
  arrange(pop) |> 
  filter(row_number()==1) |> 
  select(pop, ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
    mutate(year = parse_number(pop),
         pop = str_match(pop, "(ven|per)")[,2],
         .before = everything()) |> 
  rename(H = H_mean) |> 
  rename(A = A_mean) |> 
  rename(M0 = M0_mean) |> 
  ungroup() |> 
  mutate(across(pop, str_replace, 'per', 'Peruana')) |> 
  mutate(across(pop, str_replace, 'ven', 'Venezolana')) |> 
  rename("Población" = pop)
  
  
  
  

# Charts 2, 3, 4

# H
mpi_index |> 
  ggplot(aes(as_factor(year), H, group = `Población`)) +
  geom_col(aes(fill = `Población`), position = position_dodge()) +
  geom_label(aes(label = scales::label_percent(.1)(H)),
              position = position_dodge(.9),
             fill = NA,
             label.size = NA,
             vjust = -0.1) +
  scale_fill_manual(values = c(`Peruana` = "black", `Venezolana` = "#0072BC")) +
  scale_y_continuous(labels = scales::label_percent()) +
  labs(
      # title = "Multidimensional Poverty Index",
      #  subtitle = "Headcount ratio (H)",
       x = NULL, y = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        panel.grid = element_blank(),
        axis.text.y = element_blank())
# 
# ggsave("out/mpi_H.png", bg = "white")

 # write_xlsx(mpi_index, "Chart_234.xlsx")

# A
mpi_index |> 
  ggplot(aes(as_factor(year), A, group = `Población`)) +
  geom_col(aes(fill = `Población`), position = position_dodge()) +
  geom_label(aes(label = scales::label_percent(.1)(A)),
             position = position_dodge(.9),
             fill = NA,
             label.size = NA,
             vjust = -0.1) +
  scale_fill_manual(values = c(`Peruana` = "black", `Venezolana` = "#0072BC")) +
  scale_y_continuous(labels = scales::label_percent()) +
  labs(
       # title = "Multidimensional Poverty Index",
       # subtitle = "Poverty Intensity (A)",
       x = NULL, y = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        panel.grid = element_blank(),
        axis.text.y = element_blank())

#ggsave("out/mpi_A.png", bg = "white")

# M0
mpi_index |> 
  ggplot(aes(as_factor(year), M0, group = `Población`)) +
  geom_col(aes(fill = `Población`), position = position_dodge()) +
  geom_label(aes(label = round(M0, 2)),
             position = position_dodge(.9),
             fill = NA,
             label.size = NA,
             vjust = -0.1) +
  scale_fill_manual(values = c(`Peruana` = "black", `Venezolana` = "#0072BC")) +
#  scale_y_continuous(labels = scales::label_percent()) +
  labs(
       # title = "Multidimensional Poverty Index",
       # subtitle = "Adjusted headcount ratio (M0)",
       x = NULL, y = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        panel.grid = element_blank(),
        axis.text.y = element_blank())

#ggsave("out/mpi_M0.png", bg = "white")

# Subgroup analysis ----
enpove2022_subgroup_data <- 
  enpove2022_mpi_data |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  select(clustid, vivid, hhid, mpi_poor) |> 
  left_join(enpove2022_mpi_data) |> 
  mutate(agegrp = case_when(between(age, 0, 4) ~ "0-4",
                            between(age, 5, 17) ~ "5-17",
                            between(age, 18, 59) ~ "18-59",
                            age >= 60 ~ "60+"),
         sex = if_else(sex == 1, "M", "F")) |> 
  select(agegrp, sex, location, mpi_poor, hhwt)

popcnt <- sum(enpove2022_subgroup_data$hhwt)
poorcnt <- sum(enpove2022_subgroup_data$hhwt*replace_na(enpove2022_subgroup_data$mpi_poor, 0))

enpove2022_subgroup_age <- 
  enpove2022_subgroup_data |> 
  summarize(pop = sum(hhwt)/popcnt, 
            pop_num = sum(hhwt),
            pop_denom = popcnt,
            poor = sum(hhwt*mpi_poor, na.rm = TRUE)/poorcnt, 
            poor_num = sum(hhwt*mpi_poor, na.rm = TRUE),
            poor_denom = poorcnt,
            .by = agegrp)

enpove2022_subgroup_sex <- 
  enpove2022_subgroup_data |> 
  summarize(pop = sum(hhwt)/popcnt, 
            pop_num = sum(hhwt),
            pop_denom = popcnt,
            poor = sum(hhwt*mpi_poor, na.rm = TRUE)/poorcnt,
            poor_num = sum(hhwt*mpi_poor, na.rm = TRUE),
            poor_denom = poorcnt,
            .by = sex)

enpove2022_subgroup_location <- 
  enpove2022_subgroup_data |> 
  summarize(pop = sum(hhwt)/popcnt, 
            pop_num = sum(hhwt),
            pop_denom = popcnt,
            poor = sum(hhwt*mpi_poor, na.rm = TRUE)/poorcnt, 
            poor_num = sum(hhwt*mpi_poor, na.rm = TRUE),
            poor_denom = poorcnt,
            .by = location)

enpove2022_subgroup_stats <- 
  bind_rows(age = enpove2022_subgroup_age |> rename(lvl = agegrp),
            sex = enpove2022_subgroup_sex |> rename(lvl = sex),
            loc = enpove2022_subgroup_location |> rename(lvl = location),
            .id = "dim") |> 
  mutate(lvl = fct_relevel(lvl, "0-4", "5-17", "18-59", "60+",
                           "M", "F",
                           "Lima Metro", "Other Urban")) |> 
  arrange(dim, lvl) |> 
  pivot_longer(cols = c(pop, pop_num, pop_denom, poor, poor_num, poor_denom), names_to = "Tipo", values_to = "Total") |> 
  mutate(across('dim', str_replace, 'age', 'Edad')) |> 
  mutate(across('dim', str_replace, 'loc', 'Ubicación')) |> 
  mutate(across('dim', str_replace, 'sex', 'Sexo')) |> 
  mutate(lvl = case_when(lvl == "M" ~ "Hombre",
                         TRUE ~ lvl)) |> 
  mutate(across('lvl', str_replace, '5-17', '05-17')) |> 
  mutate(across('lvl', str_replace, 'F', 'Mujer')) |>   
  mutate(across('lvl', str_replace, 'Other Urban', 'Otro Urbano'))

enpove2022_subgroup_stats_details <- enpove2022_subgroup_stats |> 
  pivot_wider(names_from = Tipo, values_from = Total) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2)))
  
# write_xlsx(enpove2022_subgroup_stats_details, "Chart_6.xlsx")

# Chart 6
enpove2022_subgroup_stats |> 
  filter(Tipo == "pop" | Tipo == "poor") |> 
  ggplot(aes(lvl, Total, group = Tipo)) +
  geom_col(aes(fill = Tipo), position = position_dodge()) +
  geom_label(aes(label = scales::label_percent(.1)(Total)),
             position = position_dodge(.9),
             fill = NA,
             label.size = NA,
             vjust = -0.1) +
  scale_fill_manual(values = c(poor = "#122947", pop = "#74879F"), labels = c("% de pobres", "% de población"), name = NULL) +
  scale_y_continuous(labels = scales::label_percent()) +
  facet_wrap(vars(dim), scales = "free_x", nrow = 1) +
  labs(
    # title = "Multidimensional Poverty Index",
    # subtitle = "Poverty Intensity (A)",
    x = NULL, y = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        panel.grid = element_blank(),
        axis.text.y = element_blank(), 
        strip.text = element_text(face = "bold")
  )

#ggsave("out/subgroups.png", bg = "white")
#enpove2022_subgroup_stats |> write_xlsx("out/subgroups.xlsx", format_headers = FALSE)

# Dimensional contribution ----
enpove2022_dim_contrib <- 
  enpove2022_mpi_data |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  mutate(across(health_access:connectivity_ict, list(num = ~sum(. * hhwt * mpi_poor, na.rm = TRUE), denom = ~sum(deprivations * hhwt * mpi_poor, na.rm = TRUE), mean = ~sum(. * hhwt * mpi_poor, na.rm = TRUE)/sum(deprivations * hhwt * mpi_poor, na.rm = TRUE)))) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")

enpove2018_dim_contrib <- 
  enpove2018_mpi_data |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  mutate(across(health_access:connectivity_ict, list(num = ~sum(. * hhwt * mpi_poor, na.rm = TRUE), denom = ~sum(deprivations * hhwt * mpi_poor, na.rm = TRUE), mean = ~sum(. * hhwt * mpi_poor, na.rm = TRUE)/sum(deprivations * hhwt * mpi_poor, na.rm = TRUE)))) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")

enaho2022_dim_contrib <- 
  enaho2022_mpi_data |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  mutate(across(health_access:connectivity_ict, list(num = ~sum(. * hhwt * mpi_poor, na.rm = TRUE), denom = ~sum(deprivations * hhwt * mpi_poor, na.rm = TRUE), mean = ~sum(. * hhwt * mpi_poor, na.rm = TRUE)/sum(deprivations * hhwt * mpi_poor, na.rm = TRUE)))) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")

enaho2018_dim_contrib <- 
  enaho2018_mpi_data |> 
  summarize(across(where(is.logical), any),
            hhwt = first(hhwt)*n(),
            .by = c(clustid, vivid, hhid)) |> 
  mutate(deprivations = pick(where(is.logical)) |> rowSums(),
         mpi_poor = deprivations/14 >= 3/7) |> 
  mutate(across(health_access:connectivity_ict, list(num = ~sum(. * hhwt * mpi_poor, na.rm = TRUE), denom = ~sum(deprivations * hhwt * mpi_poor, na.rm = TRUE), mean = ~sum(. * hhwt * mpi_poor, na.rm = TRUE)/sum(deprivations * hhwt * mpi_poor, na.rm = TRUE)))) |> 
  slice(1) |> 
  select(ends_with(c("_mean", "_denom", "_num"))) |> 
  mutate(across(ends_with("num"), ~ round(., 2))) |> 
  mutate(across(ends_with("denom"), ~ round(., 2))) |> 
  pivot_longer(everything(), names_to = c("dim", "ind", "type"), names_sep = "_", values_to = "p")

dim_contrib_details <-
  bind_rows(per2018 = enaho2018_dim_contrib,
            ven2018 = enpove2018_dim_contrib,
            per2022 = enaho2022_dim_contrib,
            ven2022 = enpove2022_dim_contrib,
            .id = "pop") |> 
  pivot_wider(names_from = type, values_from = p) |> 
  mutate(num = round(num, digits = 2)) |> 
  mutate(denom = round(denom, digits = 2)) |> 
  mutate(year = parse_number(pop),
         pop = str_match(pop, "(ven|per)")[,2],
         .before = everything()) |> 
  rename("Población" = pop) |>
  mutate(across('Población', str_replace, 'per', 'Peruana')) |> 
  mutate(across('Población', str_replace, 'ven', 'Venezolana')) |> 
  mutate(across('dim', str_replace, 'health', 'Salud')) |> 
  mutate(across('dim', str_replace, 'education', 'Educación')) |> 
  mutate(across('dim', str_replace, 'housing', 'Vivienda')) |> 
  mutate(across('dim', str_replace, 'wash', 'Agua y saneamiento')) |> 
  mutate(across('dim', str_replace, 'energy', 'Energía')) |> 
  mutate(across('dim', str_replace, 'employment', 'Empleo y provisión social')) |> 
  mutate(across('dim', str_replace, 'connectivity', 'Conectividad'))
  
  
# write_xlsx(dim_contrib_details, "Chart_5.xlsx")

dim_contrib <- 
  bind_rows(per2018 = enaho2018_dim_contrib,
            ven2018 = enpove2018_dim_contrib,
            per2022 = enaho2022_dim_contrib,
            ven2022 = enpove2022_dim_contrib,
            .id = "pop") |> 
  filter(type == "mean") |> 
  select(pop, dim, ind, p) |> 
  summarize(p = sum(p),
            .by = c(pop, dim)) |> 
  mutate(year = parse_number(pop),
         pop = str_match(pop, "(ven|per)")[,2],
         .before = everything()) |> 
  rename("Población" = pop) |>
  mutate(across('Población', str_replace, 'per', 'Peruana')) |> 
  mutate(across('Población', str_replace, 'ven', 'Venezolana')) |> 
  mutate(across('dim', str_replace, 'health', '01Salud')) |> 
  mutate(across('dim', str_replace, 'education', '02Educación')) |> 
  mutate(across('dim', str_replace, 'housing', '03Vivienda')) |> 
  mutate(across('dim', str_replace, 'wash', '04Agua y saneamiento')) |> 
  mutate(across('dim', str_replace, 'energy', '05Energía')) |> 
  mutate(across('dim', str_replace, 'employment', '06Empleo y provisión social')) |> 
  mutate(across('dim', str_replace, 'connectivity', '07Conectividad'))

# Chart 5
dim_contrib |> 
  ggplot(aes(`Población`, p, group = dim)) +
  geom_col(aes(fill = dim), position = position_stack()) +
  geom_label(aes(label = scales::label_percent(.1)(p)),
             position = position_stack(.5),
             fill = NA,
             label.size = NA) +
  # geom_label(aes(label = scales::label_percent(.1)(p)),
  #            position = position_stack(.5)) +
  facet_wrap(vars(year), nrow = 1) +
  scale_y_continuous(labels = scales::label_percent()) +
  scale_fill_manual(values = thematic::okabe_ito(7)) +
  labs(x = NULL,y = NULL
  #     title = "Dimensional Contributions"
       ) +
  theme_minimal() +
  theme(panel.grid = element_blank(),
        axis.text.y = element_blank(), 
        strip.text = element_text(face = "bold")
  )


# library(unhcrthemes)
# library(ggrepel)
# 
# ggplot(dim_contrib, aes(year, p, group = dim
#  )) +
#   geom_line(
#     size = 0.75,
#     color = unhcr_pal(n = 1, "pal_blue")
#   ) +
#   geom_text_repel(
#     data = dim_contrib |> filter(year == 2018),
#     aes(label = paste(
#       dim, scales::label_percent(.1)(p)
#       )
#     ),
#     size = 8 / .pt,
#     hjust = 1,
#     direction = "y",
#     nudge_x = -0.3
#   ) +
#   geom_text_repel(
#     data = dim_contrib |> filter(year == 2022),
#     aes(label = paste(
#       dim, scales::label_percent(.1)(p)
#       )
#     ),
#     size = 8 / .pt,
#     hjust = 0,
#     direction = "y",
#     nudge_x = 0.3
#   ) +
#   geom_point(
#     size = 2.5,
#     color = unhcr_pal(n = 1, "pal_blue")
#   ) +
#   facet_wrap(vars(Población), ncol = 1, scales = "free_y") +
#   
#   # labs(
#   #   title = "Evolution of refugee population in East and\nHorn of Africa region | 2016 vs 2021",
#   #   caption = "Source: UNHCR Refugee Data Finder\n© UNHCR, The UN Refugee Agency"
#   # ) +
#   scale_x_continuous(
#     breaks = c(2018, 2022),
#    limits = c(2016, 2024)
#   ) +
# #  scale_y_continuous(limits = c(-2e5, NA)) +
#   theme_unhcr(
#     grid = "X",
#     axis = FALSE,
#     axis_title = FALSE,
#     axis_text = "X"
#   )
# 
# library(scales)
# 
# ggplot(
#   dim_contrib,
#   aes(
#     x = round(p, 2),
#     y = reorder(dim, desc(dim))
#   )
# ) +
#   geom_line(
#     aes(group = dim),
#     size = 0.75,
#     color = unhcr_pal(n = 1, "pal_grey")
#   ) +
#   geom_point(
#     aes(color = fct_rev(factor(year))),
#     size = 2.5
#   ) +
#   scale_color_unhcr_d(
#     palette = "pal_unhcr",
#     labels = c("2018", "2022"),
#     direction = -1
#   ) +
#   scale_x_continuous(
#     limit = c(0, max(dim_contrib$p)),
#     label = scales::label_percent()
#   ) +
#   facet_wrap(vars(Población), ncol = 1, scales = "free_y") +
#   geom_text_repel(
#     aes(label = scales::label_percent(.1)(p), color = fct_rev(factor(year))),
#     size = 8 / .pt
#   ) +
#   scale_y_discrete(label = scales::wrap_format(25)) +
#   theme_unhcr(
#     grid = "XY",
#     axis = FALSE,
#     axis_title = FALSE
#   )

#ggsave("out/dimcontrib.png", bg = "white")
