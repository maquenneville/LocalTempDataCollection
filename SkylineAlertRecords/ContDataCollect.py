# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 22:31:55 2023

@author: marca
"""
# import necessary libraries
import os
from io import BytesIO
import requests
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError
import smtplib
import threading
import psycopg2
import datetime
import time
import re
from TempDatabaseHelpers import (
    create_daily_table,
    insert_daily_data,
    insert_weather_data,
    backup_database,
)

#Set ImageFile to accept truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Declare tesseract.exe
tesseract_path = "C:\Program Files\Tesseract-OCR\tesseract.exe"
pytesseract.tesseract_cmd = tesseract_path

source_url = ""
source_image_path = ""
source_image_pathHiLo = ""


# Set text messaging variables
m_email = "email"
m_password = "password"


recipients = {
    "Person1": "number@vtext.com",
    "Person2": "number@vtext.com",
    "Person3": "number@vtext.com",
    "Person4": "number@vtext.com",
}


backup_directory = ""

# Declare database credentials
db_host = "host"
db_name = "SkylineTemps"
db_user = "user"
db_password = "password"
db_port = "5432"


def download_image():
    print("Weather Image Downloading...")
    url = source_url
    response = requests.get(url)
    print(response.status_code)
    soup = BeautifulSoup(response.text, "html.parser")
    image_url = source_url + soup.find_all("img")[0]["src"]
    response = requests.get(image_url)
    imageFile = open(source_image_path, "wb")
    for chunk in response.iter_content(100000):
        imageFile.write(chunk)
    imageFile.close()
    weather_image = Image.open(source_image_path)
    width, height = weather_image.size
    triple_size = weather_image.resize(
        (int(width * 3), int(height * 3)), Image.ANTIALIAS
    )
    triple_size = ImageOps.grayscale(triple_size)
    triple_size = ImageOps.autocontrast(triple_size)
    hi_lo_image = triple_size.crop((452, 0, 678, 360))
    hi_lo_image = ImageOps.grayscale(hi_lo_image)
    hi_lo_image = ImageOps.autocontrast(hi_lo_image)
    triple_size.save(source_image_path)
    hi_lo_image.save(source_image_pathHiLo)

    return source_image_path, source_image_pathHiLo


def send_text(number, name, text):
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(m_email, m_password)
    message = text
    server.sendmail(m_email, number, message)
    server.quit()
    print(f"Text sent to {name}")



def extract_text(image_file_path, hi_lo_file_path):
    print("Extracting text...")
    retries = 0
    error_to_catch = UnidentifiedImageError
    retry_limit = 10
    ite = 30
    
    columns = ["Temperature", 
        "Wind chill",
        "Humidity",
        "Barometer",
        "Hi",
        "Lo"
    ]
    
    results = {key: None for key in columns}
    
    
    while ite > 0:
        while retries < retry_limit:
            try:
                text_raw = pytesseract.image_to_string(Image.open(image_file_path))
                text_hi_lo = pytesseract.image_to_string(Image.open(hi_lo_file_path))
                break
            except error_to_catch as e:
                print("Could not read file, trying again...", flush=True)
                retries += 1
                time.sleep(600)
                image_file_path, hi_lo_file_path = download_image()
                if retries == retry_limit:
                    send_text(recipients["Marc"], "Marc", "Weather collector couldn't unfuck itself")
                    raise e

        text_list = text_raw.split("\n")
        hi_lo_list = text_hi_lo.split("\n")

        patterns = {
            "Temperature": r"Temperature\s*(\d{1,3}\.\d{1,2})",
            "Wind chill": r"Wind chill\s*(\d{1,3}\.\d{1,2})",
            "Humidity": r"Humidity\s*(\d{1,2})\s?%",
            "Barometer": r"Barometer\s*(\d{1,3}\.\d{1,3})",
            "Hi": r"Hi\s*(\d{1,3}\.\d{1,2})",
            "Lo": r"Lo\s*(\d{1,3}\.\d{1,2})"
        }

        

        for line in text_list + hi_lo_list:
            for key, pattern in patterns.items():
                match = re.search(pattern, line)
                if match:
                    results[key] = float(match.group(1))
                    

        print(f"Temp: {results['Temperature']}, Windchill: {results['Wind chill']}, Hum: {results['Humidity']}, High: {results['Hi']}, Low: {results['Lo']}, Baro: {results['Barometer']}", flush=True)

        if all(results.values()):
            break

        ite -= 1
        time.sleep(600)
        image_file_path, hi_lo_file_path = download_image()

    return (
        f"Skyview Weather Today: {results['Temperature']} deg, Wind Chill {results['Wind chill']} deg, Humidity {results['Humidity']} perc",
        results['Temperature'],
        results['Wind chill'],
        results['Humidity'],
        results['Hi'],
        results['Lo'],
        results['Barometer'],
    )


def send_alert(recipients, message):
    threads = []
    for name, number in recipients.items():
        t = threading.Thread(
            target=send_text,
            args=(number, name, message),
        )
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

def main():


    cur_time = datetime.datetime.now()
    print(cur_time, flush=True)
    today = int(cur_time.strftime("%d"))
    hour = int(cur_time.strftime("%H"))

    sent_to = set()

    while True:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
        )
        try:
            image_path, hi_lo = download_image()
            m_text, temp, wind, hum, hi, lo, baro = extract_text(image_path, hi_lo)

            if hour in [7, 8, 22, 23]:

                if temp < 32.0 and 'freezing' not in sent_to:
                    message = f"Freezing Temp Alert!\n{m_text}"
                    send_alert(recipients, message)
                    sent_to.add('freezing')

                if temp > 90.0 and 'heat' not in sent_to:
                    message = f"Extreme Heat Alert!\n{m_text}"
                    send_alert(recipients, message)
                    sent_to.add('heat')

            if hour == 9:
                sent_to = set()

            insert_weather_data(
                temp,
                wind,
                hum,
                hi,
                lo,
                baro,
                conn,
            )

            create_daily_table(conn)

            insert_daily_data(
                temp,
                wind,
                hum,
                baro,
                conn,
            )

            backup_database(
                db_name,
                db_user,
                db_password,
                db_host,
                db_port,
                backup_directory,
            )
            
            conn.close()
            now = datetime.datetime.now()
            wait_time = (60 - now.minute) * 60
            time.sleep(wait_time)
            

        except requests.exceptions.ConnectionError:
            print("Connection error after max attempts, retrying...", flush=True)
            continue


if __name__ == "__main__":

    main()
