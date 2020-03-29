import sqlite3
from sqlite3 import Error
import glob, os
import re
import csv
import sys, traceback
from datetime import datetime as dt
# Import .csv files from the John Hopkins COVID-19 repository of daily reports
# into a sqlite database. The repository can be cloned from
# https://github.com/CSSEGISandData/COVID-19.git. Once cloned, cd to
# ./COVID-19/csse_covid_19_data/csse_covid_19_daily_reports and execute
# the command:
#
# python <path_to_this_script>/import_covid_csv_to_covid_db.py
#
# The sqlite database containing the the imported data will be in the
# same directory as the .csv daily reports and named covid-19.db
#
# Data imported by this script is copyright 2020 Johns Hopkins University,
# all rights reserved, is provided to the public strictly for educational and
# academic research purposes.


# Stack of directory names used by the functions
# pushdir() and popdir()
# see: https://stackoverflow.com/questions/6194499/pushd-through-os-system/13847807
pushstack=list()

# Directory where the local copy of John Hopkins Covid daily reports live. The
# data is cloned from https://github.com/CSSEGISandData/COVID-19.git
# and the daily reports are in ./csse_covid_19_data/csse_covid_19_daily_reports
# relative to the root of the repository.
#
# TODO: add command line parsing which allows a different daily report directory
# to be defined.
#
# For now assume the current working directory.
covid19_dir='./'

# see: https://stackoverflow.com/questions/6194499/pushd-through-os-system/13847807
def pushdir(dirname):
    global pushstack
    pushstack.append(os.getcwd())
    os.chdir(dirname)

# see: https://stackoverflow.com/questions/6194499/pushd-through-os-system/13847807
def popdir():
    global pushstack
    os.chdir(pushstack.pop())

# Create connection to a sqlite database. As defined by
# https://www.sqlitetutorial.net/sqlite-python/create-tables/
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return conn
 
# Create a table in a sqlite database. As defined by
# https://www.sqlitetutorial.net/sqlite-python/creating-database/
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

# Derive the date for the current recordset from the
# recordset's filename. The file name is in
# mm-dd-yy.csv. The output format will be
# yyyy-mm-dd
def convert_date(csv_name):

    src_date = re.sub('\.csv$', '', csv_name)
    return dt.strptime(src_date, "%m-%d-%Y").strftime('%Y-%m-%d')

# If the given filename doesn't already have a record in the
# covid_src_file table, then add a new record to the
# covid_src_file table for the specified file_name.
#
# based on https://www.sqlitetutorial.net/sqlite-python/insert/ create_task()
#
# @return -1: table already contains a record for specified file_name.
#          >0: file_name added to table. The return value corresponds
#              to the primary key of the row added.
def add_file(conn, file_name):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    retval = -1;
    sql = ''' INSERT INTO covid_src_file(src_file_name)
              VALUES(?) '''

    try:
        cur = conn.cursor()
        cur.execute(sql, [file_name])
        retval = cur.lastrowid
    except:
        retval = -1
        
    return retval

# Add the provided covid record to the covid_daily_record table.
# based on https://www.sqlitetutorial.net/sqlite-python/insert/ create_task()
def add_covid_record( conn, covid_record ):

    sql = ''' INSERT INTO covid_daily_record(file_key, report_date, fips, admin2, province_state, country_region, last_update, latitude, longitude, confirmed, deaths, recovered, active )
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''

    try:
        cur = conn.cursor()
        cur.execute(sql, covid_record)
        retval = cur.lastrowid
    except:
        traceback.print_exc(file=sys.stdout)
        retval = -1
        
    return retval

# For the provided pattern identify the key (from the headers in .csv
# file's first row). The returned key is the first column header which
# contains pattern as a substring that is within the column header.
#
# @param row The first row which contains the column headers of a .csv file
# @param pattern The pattern to search for within each of the header columns
#        to find a match.
#
# @return The key which is the column header which contains the substring
#         specified by pattern. If no match is found for the pattern, then
#         None is returned.
def get_key(row,pattern):

    retval = None
    keys=list(row)
    for k in keys:
        if pattern in k:
            retval = k
            break

    return retval;


# Process all .csv files that are in the the John Hopkins covid daily report
# at https://github.com/CSSEGISandData/COVID-19.git in directory
# csse_covid_19_data/csse_covid_19_daily_reports. Note the repository
# has been cloned to a local directory on this machine at path specfied
# by the global variable covid19_dir. For daily updates, perform a
# git pull from the local clone and then re-run this script to
# add latest data to the database.
def process_files(conn):
    global covid19_dir
    dirlist = os.listdir(covid19_dir)
    dirlist.sort()
    for f in dirlist:
        if f.endswith(".csv"):
            path = os.path.join(covid19_dir, f)
            print "path: " + path;
            file_key = add_file(conn,f)
            print "file_key: ", file_key

            # add_file will return -1 if the file name already exists in the
            # covid_src_file directory, Only process records for files that
            # are new ... thus any file which has a key > 0.
            if ( file_key > 0 ):

                with open(path , mode='r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    line_count = 0
                    
                    for row in csv_reader:
                        if line_count == 0:
                            # Keys in the data set changed over time. However, even over time, they
                            # retained some commonality at a substring level. For the current
                            # .csv file, find the key in the header row which matches the common
                            # substring. e.g. Early data sets had column header "Country/Region" whereas
                            # later datasets had the column header "Country_Region".
                            key_state= get_key( row, "State" )
                            key_country= get_key( row, "Country" )
                            key_city=get_key( row, "dmin2" )
                            key_lat=get_key( row, "Lat" )
                            key_lon=get_key( row, "Long" )
                            key_fips=get_key( row, "FIPS" )
                            key_last=get_key( row, "Last")
                            line_count += 1
                        else:
                            # Get the values which are avaiable for the keys that
                            # exist within the dataset
                            try:
                                country = row[key_country]
                            except:
                                country = None
                                
                            try:
                                state = row[key_state]
                            except:
                                state = None

                            try:
                                fips = row[key_fips]
                            except:
                                fips = None

                            try:
                                city = row[key_city]
                            except:
                                city = None

                            try:
                                lat = row[key_lat]
                            except:
                                lat = None

                            try:
                                lon = row[key_lon]
                            except:
                                lon = None

                            try:
                                last = row[key_last]
                            except:
                                last = None

                            try:
                                confirmed = row["Confirmed"]
                            except:
                                confirmed = None

                            try:
                                dead = row["Deaths"]
                            except:
                                dead = None

                            try:
                                recovered = row["Recovered"]
                            except:
                                recovered = None

                            try:
                                active = row["Active"]
                            except:
                                active = None

                            # Display the normalized row tha will be placed into the database
                            print "country: " + str(country) + ", state: " + str(state) + ", city: " + str( city ) + ", lat: " + str( lat ) + ", lon: " + str( lon ) + ", confirmed: " + str(confirmed) + ", deaths: " + str( dead )

                            # Add the record to the database
                            covid_record = (file_key, convert_date(f), fips, city, state, country, last, lat, lon, confirmed, dead, recovered, active )
                            add_covid_record( conn, covid_record )

                            line_count += 1
                    print "Processed " + str(line_count) + " lines."

                # All rows made it in OK, so commit
                conn.commit()
            
    
def main():
    database = r"./covid-19.db"
 
    sql_create_covid19_table = """ CREATE TABLE IF NOT EXISTS covid_daily_record (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        file_key integer NOT NULL,
                                        report_date text NOT NULL,
                                        FIPS integer,
                                        Admin2 text,
                                        province_state text,
                                        country_region text NOT NULL,
                                        last_update text NOT NULL,
                                        latitude real,
                                        longitude real,
                                        confirmed integer NOT NULL,
                                        deaths integer NOT NULL,
                                        recovered integer NOT NULL,
                                        active integer
                                    ); """
    
    sql_create_covid19_files_table = """ CREATE TABLE IF NOT EXISTS covid_src_file (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        src_file_name text NOT NULL,
                                        UNIQUE(src_file_name)
                                    ); """

    # create a database connection
    conn = create_connection(database)
 
    # create tables and populate
    if conn is not None:
        # create covid 19 records table
        create_table(conn, sql_create_covid19_files_table)
        create_table(conn, sql_create_covid19_table)

        # populate tables
        process_files(conn)

        # Close DB connection
        conn.close();
    else:
        print("Error! cannot create the database connection.")
 
 
if __name__ == '__main__':
    main()
 
