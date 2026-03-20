#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(haven)
})

script_path <- tryCatch(normalizePath(sys.frame(1)$ofile), error = function(e) "")
if (is.na(script_path) || !nzchar(script_path)) {
  script_path <- ""
}
if (nzchar(script_path)) {
  script_dir <- dirname(script_path)
} else {
  candidate_dir <- file.path(getwd(), "exploratory")
  script_dir <- if (dir.exists(candidate_dir)) candidate_dir else getwd()
}
project_root <- normalizePath(file.path(script_dir, ".."), mustWork = FALSE)
cfg_path <- file.path(project_root, "config", "2018.toml")

if (!file.exists(cfg_path)) {
  project_root <- normalizePath(script_dir, mustWork = FALSE)
  cfg_path <- file.path(project_root, "config", "2018.toml")
}

if (!file.exists(cfg_path)) {
  stop("Config not found: ", cfg_path)
}

# Minimal TOML parsing to extract data_file.
read_lines <- readLines(cfg_path, warn = FALSE)
key_line <- read_lines[grepl("^data_file", read_lines)]
if (length(key_line) != 1) {
  stop("Expected exactly one data_file entry in 2018.toml")
}

# data_file = "..."
raw_path <- sub("^data_file\\s*=\\s*\"(.*)\"\\s*$", "\\1", key_line)
if (raw_path == key_line) {
  stop("Failed to parse data_file from 2018.toml")
}

data_file <- if (grepl("^/", raw_path)) raw_path else file.path(project_root, raw_path)
if (!file.exists(data_file)) {
  stop("Data file not found: ", data_file)
}

df <- read_sav(data_file)

if (!"Q1513" %in% names(df)) {
  stop("Q1513 not found in data; cannot build PT")
}

pt <- as.numeric(df$Q1513)

# Match load_and_prepare_e2018 logic for PT
pt[pt %in% c(96, 97, 98)] <- 99
pt <- ifelse(is.na(pt), NA_real_, ifelse(pt < 5, 0, ifelse(pt <= 10, 1, 99)))

n_total <- length(pt)

n_missing <- sum(is.na(pt))
n_99 <- sum(!is.na(pt) & pt == 99)

n_missing_or_99 <- n_missing + n_99
n_valid <- n_total - n_missing_or_99

prop_missing <- n_missing / n_total
prop_99 <- n_99 / n_total
prop_missing_or_99 <- n_missing_or_99 / n_total
prop_valid <- n_valid / n_total

counts <- table(pt, useNA = "no")

report_path <- file.path(script_dir, "pt_missing_2018_report.md")

lines <- c(
  "# PT missingness in 2018",
  "",
  "Source: `load_and_prepare_e2018` logic (Q1513 recode).",
  "",
  "## Counts",
  sprintf("- total rows: %d", n_total),
  sprintf("- missing (NA): %d", n_missing),
  sprintf("- code 99 (non-response bucket): %d", n_99),
  sprintf("- missing or 99: %d", n_missing_or_99),
  sprintf("- valid (0/1): %d", n_valid),
  "",
  "## Proportions",
  sprintf("- missing (NA): %.2f%%", prop_missing * 100),
  sprintf("- code 99 (non-response bucket): %.2f%%", prop_99 * 100),
  sprintf("- missing or 99: %.2f%%", prop_missing_or_99 * 100),
  sprintf("- valid (0/1): %.2f%%", prop_valid * 100),
  "",
  "## Value distribution (non-missing)",
  sprintf("- 0: %d", ifelse("0" %in% names(counts), counts[["0"]], 0)),
  sprintf("- 1: %d", ifelse("1" %in% names(counts), counts[["1"]], 0)),
  sprintf("- 99: %d", ifelse("99" %in% names(counts), counts[["99"]], 0))
)

writeLines(lines, report_path)
cat("Wrote report to:", report_path, "\n")
