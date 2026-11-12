# Data Engineering Assignment - Compport

All data in this pack is synthetic. No real employee or customer data is included.

## Background

Compport receives employee and salary data from each customer's HRMS and loads it into our
compensation platform. Every customer sends it in a different shape - different column names,
date formats, salary structures, sometimes CSV and sometimes Excel. Before a customer's increment
cycle can run, this data has to be mapped, transformed, loaded correctly, and we have to be able
to prove it.

Our real problem is not loading one file. It is onboarding the next customer's format without
writing a new pipeline each time. This assignment follows that problem in three parts.

## What is in the pack

| File | What it is |
| --- | --- |
| `data/hrms_a_day1.csv` | Full employee extract from HRMS "A", day 1 |
| `data/hrms_b_day1.csv` | Full employee extract from HRMS "B", day 1 (different format) |
| `data/hrms_a_day2.csv` | Full employee extract from HRMS "A", the next day |
| `data/hrms_c_export.xlsx` | Export from a third customer system that you have not seen before (used in Part B) |
| `data/manifest.csv` | Control totals sent by the source systems for each file |
| `data/field_dictionary.csv` | Description of every target field: meaning, type, allowed values, synonyms |
| `data/fx_rates.csv` | Currency rates to convert local salary to USD |
| `data/grade_mapping.csv` | How HRMS "B" grade codes map to the target grades |
| `data/country_codes.csv` | Country code to country name |
| `target_schema.sql` | The tables you load into |

Treat the files the way you would treat real customer data. Do not assume they are clean.

## Part A - Pipeline with mapping and transformation as configuration (about 3.5 hours)

1. Load `hrms_a_day1.csv` and `hrms_b_day1.csv` into the target schema. Use Python or PySpark,
   and any database you like (SQLite, DuckDB, Postgres are all fine).
2. The mapping from source columns to target fields, and the transformation applied to each
   (rename, date parsing, unit conversion, code lookup, merge or split, currency conversion and so
   on), must live in a mapping file per source (YAML or JSON), not in the pipeline code. The
   pipeline code should be the same for A and B.
3. Validate the data. Rows that should not be loaded go into the `reject` table with a clear
   reason. Decide for yourself what is a reject, what can be cleaned and loaded, and what should
   stop the load completely. Write down your rules.
4. Load `hrms_a_day2.csv` as the next day's run. The employee and salary tables should reflect
   what changed. Running the same file a second time must not change anything or create duplicates.
5. Produce a reconciliation report for each run that a non-technical implementation consultant
   could read and decide "is this load safe to use or not".

## Part B - Onboard a format you have not seen (about 2.5 hours)

`hrms_c_export.xlsx` comes from a new customer. Nobody has told you what the columns mean.

1. Write a small tool that takes any source file plus `field_dictionary.csv` and proposes a
   mapping: for each source column, the target field it most likely maps to, the transformation
   needed, a confidence level, and one line of evidence for why. Columns with no target should be
   listed as unmapped with a reason. Use whatever approach you think is right - rules, profiling
   of the values, an LLM, or a mix. If you use an LLM, say what data you send to it.
2. Run the tool on HRMS C. Review its output, correct it where it is wrong, and save the approved
   mapping file. Keep both the proposed and the approved versions in the repo.
3. Load HRMS C through the same pipeline you built in Part A, using only the new mapping file.
   If you had to change pipeline code to make C work, that is fine, but list exactly what you
   changed and why it could not be configuration.
4. Run the tool on HRMS A and B as well, and report how many columns it got right compared to the
   mappings you wrote by hand in Part A.

## Part C - Analysis (about 1 hour)

Use the data you loaded.

1. The customer's HR head asks: "Are women paid less than men here?" Give your answer with a
   number and say how confident you are in it.
2. Flag employees whose salary looks wrong and explain the method you used.

Half a page of writing is enough. A notebook or script that produces the numbers should be in
the repo.

## What to submit

A private git repository (GitHub or GitLab), shared with the ID in the covering mail, containing:

- Your code, with commits as you worked. Please do not squash into one commit.
- The mapping files for A, B and C, and the proposed-vs-approved mapping for C.
- A `README.md` with: how to run it, your validation rules, your list of supported
  transformations, your design choices and assumptions, anything you found in the data that we
  should know about, and what you would do differently if this had to onboard 300 customers.
- The reconciliation reports your pipeline produced.
- Your Part C write-up.
- A short note on time spent per part and where you stopped, if you did not finish everything.

## Rules

- Total effort should be around 7 hours. Please do not spend more than 9. If you are short of
  time, do Part A fully, then Part B, then Part C. An unfinished submission with honest notes is
  better than a polished one that took a week.
- This is meant to be your own work. If you used an AI tool or anyone's help for any part, say
  which part in the README.
- In the interview you will share your screen, run the pipeline, and walk us through the code.
  We will pick parts of it and ask you to explain them, and we will ask you to make a change
  live. So submit only what you fully understand.
- If something in the assignment is unclear, make a reasonable assumption and write it down.
  That is part of what we are looking at.
