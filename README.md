Here's a `README.md` file for your Job Tracker ETL project.

# Job Tracker ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline for tracking job listings from various online job portals. It's designed to extract raw job data, normalize and transform it into a consistent format, and then load it into a database.

## Features

  * **Multi-Source Extraction:** Gathers job data from:
      * JobNet.mm (Myanmar)
      * JobsDB.sg (Singapore)
      * JobsDB.th (Thailand)
      * Foundit.sg (Singapore)
      * JobStreet.my (Malaysia)
  * **Data Normalization:** Converts raw extracted data into a standardized format.
  * **Data Transformation:** Applies source-specific transformations for refined data, including categorization based on `categories.json`[cite: 1].
  * **Database Loading:** Uploads both raw and transformed data to a PostgreSQL database.
  * **Daily Scraper:** Includes a `daily_scraper.py` script for automated daily job extraction and incremental loading of new jobs into a combined `IT_jobs.IT` table.

## Setup

### Prerequisites

  * Python 3.8+
  * pip

### Environment Variables

Before running the script, you need to set the following environment variables:

  * `DATABASE_URL`: The connection string for your PostgreSQL database (e.g., `postgresql://user:password@host:port/database_name`).
  * `JOBNET_EMAIL`: Your email for JobNet.mm (if extracting from JobNet.mm).
  * `JOBNET_PASSWORD`: Your password for JobNet.mm (if extracting from JobNet.mm).

You can set these in a `.env` file in the root directory of your project, or directly in your shell environment.

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/job-tracker-etl.git
    cd job-tracker-etl
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

The core dependencies include:

  * `selenium`
  * `webdriver-manager`
  * `pandas`
  * `sqlalchemy`
  * `psycopg2-binary`

## Usage

### One-Time Extraction and Transformation

The `main.py` script can be run from the command line, specifying the job source you want to process. This will extract, transform, and (optionally, as currently commented out in `main.py`) load the data for that specific source into `<source_name>_raw` and `<source_name>_transformed` tables.

```bash
python main.py --source <source_name>
```

Replace `<source_name>` with one of the following:

  * `jobnetmm`
  * `jobsdbsg`
  * `jobsdbth`
  * `founditsg`
  * `jobstreetmalay`

#### Examples

**Extract, Transform, and Load data from JobsDB Singapore (for its specific tables):**

```bash
python main.py --source jobsdbsg
```

### Daily Automated Scraping and Combined Load

The `daily_scraper.py` script is designed to run daily, fetching new job listings from all configured sources, checking for duplicates against existing records, assigning custom job IDs, and appending only the new jobs to a central `IT_jobs.IT` table in your database.

To run the daily scraping process:

```bash
python daily_scraper.py
```

### Combining Transformed Data into a Single Table

The `combine_load.py` script is used to consolidate the transformed data from all individual source tables (e.g., `jobnetmm_transformed`, `jobsdbsg_transformed`) into a single `IT_jobs.IT` table. It also generates custom job IDs for the combined dataset.

To run the combined load:

```bash
python combine_load.py
```

## Project Structure

  * `main.py`: The main entry point for one-time ETL operations for specific sources.
  * `daily_scraper.py`: Automates the daily extraction, deduplication, and incremental loading of new jobs from all sources into a combined table.
  * `combine_load.py`: Combines all transformed data from individual source tables into a single `IT_jobs.IT` table.
  * `extract/`: Contains modules responsible for extracting raw job data from various sources.
      * `jobnetmm.py`: Scraper for JobNet.mm.
      * `jobdbsg.py`: Scraper for JobsDB.sg.
      * `jobsdbth.py`: Scraper for JobsDB.th.
      * `jobstreetmalay.py`: Scraper for JobStreet.my.
      * `founditSG.py`: Scraper for Foundit.sg.
  * `transform/`: Contains modules for transforming the extracted data. Each source typically has its own transformation logic.
      * `founditsg_t.py`: Transformer for Foundit.sg data.
      * `jobnetmm_t.py`: Transformer for JobNet.mm data.
      * `jobsdbsg_t.py`: Transformer for JobsDB.sg data.
      * `jobsdbth_t.py`: Transformer for JobsDB.th data.
      * `jobstreetmalay_t.py`: Transformer for JobStreet.my data.
  * `utils/`: Contains utility functions, such as data normalization and primary key generation.
      * `data_normalizer.py`: Handles the normalization of job data across different sources.
      * `pkey_gen.py`: Contains the `custom_job_id` function for generating unique job IDs.
  * `categories.json`: A JSON file defining job categories and associated keywords with weights for classification[cite: 1].
  * `requirements.txt`: Lists the Python dependencies required for the project.

## Database Schema

The `main.py` script (if load operations are uncommented) loads data into two tables for each source:

  * `<source_name>_raw`: Stores the directly extracted, raw data.
  * `<source_name>_transformed`: Stores the processed and normalized data after transformation.

For example, running with `--source jobsdbsg` will create `jobsdbsg_raw` and `jobsdbsg_transformed` tables in your database.

The `daily_scraper.py` and `combine_load.py` scripts load processed job data into a schema named `IT_jobs` and a table named `IT`. This `IT` table includes a `job_id` column generated by the `custom_job_id` function.

## Contributing

Feel free to fork the repository, make improvements, and submit pull requests.

## 📁 Project Structure

```bash
.
jobs_tracker_etl/
│
├── extract/ # Source-specific scrapers
├── transform/ # Job title cleaning, categorization, deduplication
├── utils/ # Helper functions
├── main.py # Main ETL script
├── pkey_gen.py # Primary key generation logic
├── daily_scraper.py # Script for daily scraping (parallelizable)
├── combine_load.py # Loader for combining data and pushing to DB
├── run_parallel.py # Multi-source concurrent runner
└── requirements.txt # Project dependencies

bash
Copy
Edit

KaungMyatKyaw(egg)
May Thazin Htun
Sandi Sharoi

