# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 22:27:16 2023

@author: marca
"""

# import necessary libraries
import os
from io import BytesIO
import requests
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image
import smtplib
import threading
import psycopg2
import datetime
from TempDatabaseHelpers import insert_weather_data
import time
import re


# Declare tesseract.exe
tesseract_path = "C:\Program Files\Tesseract-OCR\tesseract.exe"
pytesseract.tesseract_cmd = tesseract_path

source_url = ""
source_image_path = ""


# Set text messaging variables
m_email = "email"
m_password = "password"


recipients = {
    "Person1": "number@vtext.com",
    "Person2": "number@vtext.com",
    "Person3": "number@vtext.com",
    "Person4": "number@vtext.com",
}

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
    return source_url


def send_text(number, name, text):
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(m_email, m_password)
    message = text
    server.sendmail(m_email, number, message)
    server.quit()
    print(f"Text sent to {name}")


def extract_text(image_file_path):
    print("Extracting text...")

    ite = 30
    t_text, w_text, h_text, hi_text, lo_text = None, None, None, None, None

    while ite > 0:
        try:
            text_raw = pytesseract.image_to_string(Image.open(image_file_path))
        except:
            print("Could not read file, trying again...")
            try:
                time.sleep(600)
                image_file_path = download_image()
                text_raw = pytesseract.image_to_string(Image.open(image_file_path))
            except:
                print("Could not read file, trying once more...")
                time.sleep(600)
                image_file_path = download_image()
                text_raw = pytesseract.image_to_string(Image.open(image_file_path))

        text_list = text_raw.split("\n")

        hum_regex = r"(\d{1,2})\s?\%"
        temp_regex = r"(\d{1,3}\.\d{1,2})"
        hi_regex = r"Hi\s*\d{3}"
        lo_regex = r"Lo\s*\d{3}"

        print(text_list)

        for i in text_list:
            if "Temperature" in i:
                match = re.search(temp_regex, i)
                if match:
                    t_text = float(match.group(1))

                elif t_text:
                    pass

                else:
                    t_text = None

            if "Wind chill" in i:
                match = re.search(temp_regex, i)
                if match:
                    w_text = float(match.group(1))

                elif w_text:
                    pass

                else:
                    w_text = None
                # w_text = i.split(' ')[-1][:-2].strip()
            if "Humidity" in i:
                match = re.search(hum_regex, i)
                if match:
                    h_text = float(match.group(1))

                elif h_text:
                    pass

                else:
                    h_text = None

            if "Hi" in i:
                match = re.search(hi_regex, i)
                if match:
                    number_string = match.group(1)
                    first_two_numbers = number_string[:2]
                    last_number = number_string[2:]
                    hi_text = float(first_two_numbers + "." + last_number)

                elif hi_text:
                    pass

                else:
                    hi_text = None

            if "Lo" in i:
                match = re.search(lo_regex, i)
                if match:
                    number_string = match.group(1)
                    first_two_numbers = number_string[:2]
                    last_number = number_string[2:]
                    lo_text = float(first_two_numbers + "." + last_number)

                elif lo_text:
                    pass

                else:
                    lo_text = None

        print(t_text, w_text, h_text, hi_text, lo_text)

        if t_text and w_text and h_text and hi_text and lo_text:

            ite = 0
            break

        else:

            ite -= 1
            time.sleep(600)
            image_file_path = download_image()

    return (
        f"Skyview Weather Today: {t_text} deg, Wind Chill {w_text} deg, Humidity {h_text} perc",
        float(t_text),
        float(w_text),
        float(h_text),
        float(hi_text),
        float(lo_text),
    )


def main():

    image_path = download_image()
    m_text, temp, wind, hum = extract_text(image_path)

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

    insert_weather_data(
        temp, wind, hum, db_name, db_user, db_password, db_host, db_port
    )


if __name__ == "__main__":

    main()
    print("Alerts Sent and Database Updated!")
