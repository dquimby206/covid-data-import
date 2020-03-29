# covid-data-import

Import .csv files from the John Hopkins COVID-19 repository of daily reports
into a sqlite database. The repository can be cloned from
https://github.com/CSSEGISandData/COVID-19.git. Once cloned, cd to
./COVID-19/csse_covid_19_data/csse_covid_19_daily_reports and execute
the command:

`python <path_to_this_script>/import_covid_csv_to_covid_db.py`

The sqlite database containing the the imported data will be in the
same directory as the .csv daily reports and named covid-19.db

Data imported by this script is copyright 2020 Johns Hopkins University,
all rights reserved, is provided to the public strictly for educational and
academic research purposes.
