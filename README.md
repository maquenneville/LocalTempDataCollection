for Windows

# LocalTempDataCollection
This project takes weather data from a neighbor's weather station (posted on his website) and records it in Postgres.  It also sends text alerts for weather to my family.

*I have permission to use the weather data as I wish, but do not have permission to post the source URL*

This project first downloads the JPG file from the source URL, which is updated every ten minutes from a personal weather station.  It then uses pytesseract to extract the text from the image.  Using some string parsing and regexes, the script attempts to find the temperature, humidity and windchill.  Since the extraction is... less than perfect, and the extracted string format changes each time (oftentimes omitting the target data entirely, the script will attempt to extract the target variables and if it can't, it will retry every ten minutes until it accumulates the all necessary data.  The text alerts are then generated and sent to a custom dictionary of recipients.  Finally, my Postgres database is updated with the days weather info as a row, choosing the appropriate monthly table based on the current date.

This is meant to be run each day automatically through Task Scheduler.

Ideally, this program will be a reliable recordkeeper, as well as a reliable source of custom text alerts.  For the recordkeeping, I'm continuing to build out my helper functions for interacting with the database with as much ease as possible, and catching any parsing/extracion issues to correct for them, while working on visualizations for the data once a reasonable amount is collected.  For the text alerts, I'll likely only send them for extreme temps once I'm closer to finished the initial build.  Overall, I'm hoping to be able to build/package this up enough to be an actual utility for the other people on my hill.

Dependencies:

pandas, psycopg2, matplotlib, numpy, re, io, requests, bs4, pytesseract, pillow, smtplib

Notes:

Here's the pytesseract docs, as it can be tricky to download correctly: https://github.com/tesseract-ocr/tessdoc  
