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

    ite = 30
    t_text, w_text, h_text, hi_text, lo_text, b_text = (
        None,
        None,
        None,
        None,
        None,
        None,
    )

    error_to_catch = UnidentifiedImageError
    retry_limit = 10
    retries = 0

    while ite > 0:
        
        
        while retries < retry_limit:
            try:
                text_raw = pytesseract.image_to_string(Image.open(image_file_path))
                text_hi_lo = pytesseract.image_to_string(Image.open(hi_lo_file_path))
                # If the action is successful, break out of the loop
                break
            except error_to_catch as e:
                print("Could not read file, trying again...")
                retries += 1
                time.sleep(600)
                image_file_path, hi_lo_file_path = download_image()
                text_raw = pytesseract.image_to_string(Image.open(image_file_path))
                text_hi_lo = pytesseract.image_to_string(Image.open(hi_lo_file_path))
                # If the retry limit is reached, raise the error again
                if retries == retry_limit:
                    send_text(recipients["Marc"], "Marc", "Weather collector failed to scrape image")
                    raise e

        text_list = text_raw.split("\n")
        hi_lo_list = text_hi_lo.split("\n")

        hum_regex = r"(\d{1,2})\s?\%"
        temp_regex = r"(\d{1,3}\.\d{1,2})"
        baro_regex = r"(\d{1,3}\.\d{1,3})"
        hi_regex = r"Hi\s*\d{1,3}\.\d{1,2}"
        lo_regex = r"Lo\s*\d{1,3}\.\d{1,2}"

        print(text_list)
        print(hi_lo_list)

        for i in text_list:
            if "Temperature" in i:
                match = re.search(temp_regex, i)
                if match:
                    t_text = float(match.group(1))

                elif t_text:
                    pass

                else:
                    t_text = None

            if "Wind chill" in i or "wind chill" in i:
                match = re.search(temp_regex, i)
                if match:
                    w_text = float(match.group(1))

                elif w_text:
                    pass

                else:
                    w_text = None

            if "Humidity" in i or "humidity" in i:
                match = re.search(hum_regex, i)
                if match:
                    h_text = float(match.group(1))

                elif h_text:
                    pass

                else:
                    h_text = None

            if "Barometer" in i or "barometer" in i:
                match = re.search(baro_regex, i)
                if match:
                    b_text = float(match.group(1))

                elif b_text:
                    pass

                else:
                    b_text = None

        for i in hi_lo_list:

            if "Hi" in i:
                match = re.search(hi_regex, i)
                if match:
                    hi_text = float(match.group(0)[2:])

                elif hi_text:
                    pass

                else:
                    hi_text = None

            if "Lo" in i:
                match = re.search(lo_regex, i)
                if match:
                    lo_text = float(match.group(0)[2:])

                elif lo_text:
                    pass

                else:
                    lo_text = None

        print(
            f"Temp: {t_text}, Windchill: {w_text}, Hum: {h_text}, High: {hi_text}, Low: {lo_text}, Baro: {b_text}"
        )

        if t_text and w_text and h_text and hi_text and lo_text and b_text:

            ite = 0
            break

        else:

            ite -= 1
            time.sleep(600)
            image_file_path, hi_lo_file_path = download_image()

    return (
        f"Skyview Weather Today: {t_text} deg, Wind Chill {w_text} deg, Humidity {h_text} perc",
        float(t_text),
        float(w_text),
        float(h_text),
        float(hi_text),
        float(lo_text),
        float(b_text),
    )


def main():

    while True:

        cur_time = datetime.datetime.now()
        print(cur_time)
        today = int(cur_time.strftime("%d"))
        hour = int(cur_time.strftime("%H"))

        sent = False

        try:
            image_path, hi_lo = download_image()
            m_text, temp, wind, hum, hi, lo, baro = extract_text(image_path, hi_lo)

            if hour in [7, 8, 22, 23]:

                if not sent:

                    if temp < 32.0:
                        # start multiple threads to send the text
                        threads = []
                        for name, number in recipients.items():
                            me_text = f"Freezing Temp Alert!\n{m_text}"
                            t = threading.Thread(
                                target=send_text,
                                args=(
                                    number,
                                    name,
                                    me_text,
                                ),
                            )
                            threads.append(t)
                            t.start()

                        # wait for all threads to finish
                        for thread in threads:
                            thread.join()

                        sent = True

                    if temp > 90.0:
                        # start multiple threads to send the text
                        threads = []
                        for name, number in recipients.items():
                            me_text = f"Extreme Heat Alert!\n{m_text}"
                            t = threading.Thread(
                                target=send_text,
                                args=(
                                    number,
                                    name,
                                    me_text,
                                ),
                            )
                            threads.append(t)
                            t.start()

                        # wait for all threads to finish
                        for thread in threads:
                            thread.join()

                        sent = True
            if hour == 9:
                sent = False

            insert_weather_data(
                temp,
                wind,
                hum,
                hi,
                lo,
                baro,
                db_name,
                db_user,
                db_password,
                db_host,
                db_port,
            )
            create_daily_table(db_name, db_user, db_password, db_host, db_port)
            insert_daily_data(
                temp, wind, hum, baro, db_name, db_user, db_password, db_host, db_port
            )
            backup_database(
                db_name, db_user, db_password, db_host, db_port, backup_directory
            )

            # Get the current time
            now = datetime.datetime.now()

            # Calculate the amount of time to wait (in seconds)
            wait_time = (60 - now.minute) * 60

            # Wait until the next full hour
            time.sleep(wait_time)

        except requests.exceptions.ConnectionError:
            print("Connection error after max attempts, retrying...")
            continue


if __name__ == "__main__":

    main()
    print("Alerts Sent and Database Updated!")
