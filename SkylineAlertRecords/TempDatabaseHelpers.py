# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 17:57:03 2023

@author: marca
"""

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from psycopg2 import sql
from datetime import datetime, timedelta
import re
import os
import subprocess


def create_monthly_tables(db_name, db_user, db_password, db_host, db_port, year):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Create tables for each month
    for month in range(1, 13):
        month = str(month)
        month = month.zfill(2)
        table_name = f"temperature_{month}_{year}"
        cur.execute(
            "CREATE TABLE IF NOT EXISTS {} (date DATE, temperature FLOAT, windchill FLOAT, humidity FLOAT, high FLOAT, low FLOAT, barometer FLOAT)".format(
                table_name
            )
        )
    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


def rename_tables(db_name, db_user, db_password, db_host, db_port, year):

    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    tables = cur.fetchall()

    for table in tables:
        if "temperature_" in table[0]:
            current_month = table[0].split("_")[1]
            if int(current_month) < 10:
                new_month = str(current_month.zfill(2))
                year = str(year)
                new_table_name = f"temperature_{new_month}_{year}"
                rename_table_query = "ALTER TABLE {} RENAME TO {}".format(
                    table[0], new_table_name
                )
                cur.execute(rename_table_query)
    conn.commit()
    cur.close()
    conn.close()


def insert_weather_data(
    temp, wind, hum, hi, lo, baro, db_name, db_user, db_password, db_host, db_port
):

    print("Loading temperature into Skyline temp database...")
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current month and year
    today = datetime.now()
    today_date = today.strftime("%Y-%m-%d")
    current_month = today.strftime("%m")
    current_day = today.strftime("%d")
    current_year = today.strftime("%Y")

    # Determine table name based on current month and year
    table_name = "temperature_" + str(current_month) + "_" + str(current_year)

    # Check if a row already exists for today
    cur.execute("SELECT date FROM {} WHERE date = %s".format(table_name), (today_date,))
    row = cur.fetchone()
    if row is not None:
        print("Today's weather already recorded")
    else:
        # Insert data into table
        cur.execute(
            "INSERT INTO {} (date, temperature, windchill, humidity, high, low, barometer) VALUES (%s, %s, %s, %s, %s, %s, %s)".format(
                table_name
            ),
            (today_date, temp, wind, hum, hi, lo, baro),
        )

        # Commit the changes to the database
        conn.commit()
        print("Weather added to database")

    # Close the cursor and connection
    cur.close()
    conn.close()


def delete_all_tables(db_name, db_user, db_password, db_host, db_port):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get a list of all tables
    cur.execute(
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
    """
    )
    tables = cur.fetchall()

    # Iterate through the tables and drop them
    for table in tables:
        cur.execute("DROP TABLE IF EXISTS {};".format(table[0]))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()
    print("All tables deleted")


def add_columns_to_monthly_tables(db_name, db_user, db_password, db_host, db_port):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current year
    current_year = datetime.date.today().year
    # Get a list of all tables
    cur.execute(
        """
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' and tablename like 'temperature_%%_%s'
            """
        % current_year
    )
    tables = cur.fetchall()

    # Iterate through the tables and add columns
    for table in tables:
        cur.execute("ALTER TABLE {} DROP COLUMN high;".format(table[0]))
        cur.execute("ALTER TABLE {} DROP COLUMN low;".format(table[0]))
        cur.execute("ALTER TABLE {} ADD COLUMN windchill FLOAT;".format(table[0]))
        cur.execute("ALTER TABLE {} ADD COLUMN humidity FLOAT;".format(table[0]))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()
    print("All Tables Altered")


def set_date_as_primary_key(db_name, db_user, db_password, db_host, db_port):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        cursor = conn.cursor()
        for month in range(1, 13):
            month = str(month)
            month = month.zfill(2)
            table_name = f"temperature_{month}_2023"
            cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (date)")
        conn.commit()
        print("Date column set as primary key for all monthly tables.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error setting date as primary key: {error}")
        conn.rollback()
    finally:
        cursor.close()


def drop_date_primary_key(database_name, username, password, host, port):

    conn = psycopg2.connect(
        database=database_name, user=username, password=password, host=host, port=port
    )
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        tables = cursor.fetchall()

        pattern = re.compile(r"temperature_\d_\d{4}")
        for table in tables:
            table_name = table[0]
            if pattern.match(table_name):
                cursor.execute(
                    f"ALTER TABLE {table_name} DROP CONSTRAINT {table_name}_pkey"
                )
                conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def generate_monthly_report(db_name, db_user, db_password, db_host, db_port):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current year
    current_year = datetime.date.today().year
    # Get a list of all tables
    cur.execute(
        """
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' and tablename like 'temperature_%%_%s'
            """
        % current_year
    )
    tables = cur.fetchall()
    results = {}
    # Iterate through the tables and generate report
    for table in tables:
        cur.execute(
            "SELECT AVG(temperature), AVG(windchill), AVG(humidity) FROM {};".format(
                table[0]
            )
        )
        res = cur.fetchone()
        month = table[0].split("_")[1]
        results[month] = {"avgtemp": res[0], "avgwindchill": res[1], "avghum": res[2]}
    # Create dataframe from results
    df = pd.DataFrame.from_dict(results, orient="index")
    df.index.name = "Month"
    df.reset_index(inplace=True)
    # Export dataframe to csv
    df.to_csv("temperature_report_%s.csv" % current_year)


def plot_monthly_temperature(db_name, db_user, db_password, db_host, db_port):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current year
    current_year = datetime.date.today().year
    # Get a list of all tables
    cur.execute(
        """
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' and tablename like 'temperature_%%_%s'
            """
        % current_year
    )
    tables = cur.fetchall()

    colors = plt.cm.rainbow(np.linspace(0, 1, len(tables)))
    # Iterate through the tables and plot temperature
    for i, table in enumerate(tables):
        cur.execute("SELECT date, temperature FROM {};".format(table[0]))
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["date", "temperature"])
        df["date"] = pd.to_datetime(df["date"])
        df["day"] = df["date"].dt.day
        plt.plot(
            df["day"], df["temperature"], color=colors[i], label=table[0].split("_")[1]
        )
    plt.xlabel("Day of the Month")
    plt.ylabel("Temperature")
    plt.legend()
    plt.show()
    cur.close()
    conn.close()


def create_daily_table(db_name, db_user, db_password, db_host, db_port):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    today = datetime.now()
    current_month = today.strftime("%m")
    current_day = today.strftime("%d")
    current_year = today.strftime("%Y")
    table_name = f"daily_{current_month}_{current_day}"
    # Determine table name based on current month and year
    monthly_table_name = f"temperature_{current_month}_{current_year}"

    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                day INT,
                date_id DATE REFERENCES {monthly_table_name}(date),
                time INT,
                temperature FLOAT,
                windchill FLOAT,
                humidity FLOAT,
                barometer FLOAT
            )"""
        )
        conn.commit()
        print("Table created successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating table", error)
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def insert_daily_data(
    temp, wind, hum, baro, db_name, db_user, db_password, db_host, db_port
):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        cur = conn.cursor()

        current_time = datetime.now()
        year = current_time.strftime("%Y")
        month = current_time.strftime("%m")
        day = current_time.strftime("%d")
        hour = current_time.strftime("%H")
        time = current_time.strftime("%H") + current_time.strftime("%m")

        # Get date_id from monthly table for the given year and month
        cur.execute(
            f"SELECT date FROM temperature_{month}_{year} WHERE EXTRACT(DAY FROM date) = {day};"
        )
        result = cur.fetchone()
        if result is None:
            print(f"No data found for year={year}, month={month}, day={day}")
            return

        # Insert new row into the appropriate daily table
        date_id = result[0]
        cur.execute(f"SELECT id FROM daily_{month}_{day} ORDER BY id DESC LIMIT 1")
        result = cur.fetchone()
        new_id = 1 if result is None else result[0] + 1

        SQL = (
            f"INSERT INTO daily_{month}_{day} (id, day, date_id, time, temperature, windchill, humidity, barometer) "
            f"VALUES ({new_id}, {int(day)}, '{date_id}', {time}, {temp}, {wind}, {hum}, {baro})"
        )
        cur.execute(SQL)
        conn.commit()
        print(f"Data inserted into daily_{month}_{day} table")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


def backup_database(db_name, db_user, db_password, db_host, db_port, backup_directory):
    """
    Backs up a PostgreSQL database to a specified directory.

    :param db_name: The name of the database to back up
    :param db_user: The username to use for the database connection
    :param db_password: The password to use for the database connection
    :param db_host: The hostname of the database server
    :param db_port: The port number of the database server
    :param backup_directory: The directory to save the backup file to
    """
    # Create a filename for the backup file based on the current date and time
    now = datetime.datetime.now()
    filename = f"{db_name}.backup"

    # Build the pg_dump command
    cmd = [
        "pg_dump",
        "--dbname=postgresql://"
        + db_user
        + ":"
        + db_password
        + "@"
        + db_host
        + ":"
        + db_port
        + "/"
        + db_name,
        "--format=custom",
        "--file=" + os.path.join(backup_directory, filename),
    ]

    # Call pg_dump to perform the backup
    subprocess.call(cmd)

    print("Database backup saved to " + os.path.join(backup_directory, filename))
