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
            "CREATE TABLE IF NOT EXISTS {} (date DATE, temperature FLOAT, windchill FLOAT, humidity FLOAT)".format(
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
    temp, wind, hum, db_name, db_user, db_password, db_host, db_port
):

    print("Loading temperature into Skyline temp database...")
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current month and year
    today = datetime.today()
    current_month = str(today.month).zfill(2)
    current_year = today.year

    # Determine table name based on current month and year
    table_name = "temperature_" + str(current_month) + "_" + str(current_year)

    # Insert data into table
    cur.execute(
        "INSERT INTO {} (date, temperature, windchill, humidity) VALUES (%s, %s, %s, %s)".format(
            table_name
        ),
        (today, temp, wind, hum),
    )

    # Commit the changes to the database
    conn.commit()

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


def create_daily_table(db_name, db_user, db_password, db_host, db_port, year):

    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    # Get current date and month
    today = datetime.now().date()
    month = today.strftime("%m")

    # Create daily table name
    daily_table = "daily_" + str(today)

    # Check if the current monthly table exists
    monthly_table = f"temperature_{month}_{year}"
    print(monthly_table)
    try:
        cur.execute(f"SELECT * FROM {monthly_table} LIMIT 1")
    except Exception:
        print("The current monthly table does not exist.")
        return

    # Create daily table with a foreign key column
    create_table = f"""
        CREATE TABLE {daily_table} (
            id SERIAL PRIMARY KEY,
            date_id DATE NOT NULL,
            hour INT NOT NULL,
            temperature FLOAT NOT NULL,
            windchill FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            FOREIGN KEY (date_id) REFERENCES {monthly_table}(date)
        );
    """
    try:
        cur.execute(create_table)
        conn.commit()
        print(
            f"Table {daily_table} created successfully with a foreign key to {monthly_table}(date)."
        )

    except:
        print("Could not create table.")

    finally:
        cur.close()
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
