#!/usr/bin/env python
# coding: utf-8

# Weather Data Webscraper
# 
# Script to read in weather data for past 24 hours from http://weather.gc.ca 

import os
import sys
import itertools
import urllib
import lxml
from lxml import html
import requests
import datetime
from platform import python_version
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
from logging.handlers import TimedRotatingFileHandler


# Set up logging to log file
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "log-prev-24hrs.log"

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler
def get_file_handler():
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler
def get_logger(logger_name):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger

# Start logger
logger = get_logger("my module name")
logger.info(f"log-prev-24hrs.py started")
logger.info(f"Python version {python_version()}")


station_code = 'vou'
logger.info(f"Station_code: {station_code}")

url = f'https://weather.gc.ca/past_conditions/index_e.html?station={station_code:s}'

data_dir = 'data'
filename = f'past-24-hr-{station_code}-data.csv'

logger.info(f"Data directory: {data_dir}")
logger.info(f"Data filename: {filename}")


# LXML Tutorial is here: https://docs.python-guide.org/scenarios/scrape/
page = requests.get(url)
# Use page.content rather than page.text because 
# html.fromstring expects bytes as input.
tree = html.fromstring(page.content)

# This data is used by function remove_nonprintable
nonprintable = itertools.chain(range(0x00,0x20), range(0x7f,0xa0))
nonprintable = {c:None for c in nonprintable}

def remove_nonprintable(text, nonprintable=nonprintable):
    """Remove all non-printable characters from string."""
    return text.translate(nonprintable)

def read_element_text(element, empty='Missing'):
    text = element.text_content()
    if text:
        text = ' '.join(remove_nonprintable(text).split())
    else:
        text = ''
        text = empty
    return text


# Find the data table within the webpage
results = tree.xpath('//table[@id="past24Table"]')
assert len(results) == 1
past_24_table = results[0]
past_24_table

logger.info("Reading data from past 24-hour table...")
datetime_label = 'Date / Time PST'
table_header = past_24_table.xpath('thead')[0]
table_headers = [item for item in table_header.xpath('tr/th')]
logger.info("Table headings:")
count = 0
time_col = None
col_labels = []
for i, item in enumerate(table_headers):
    # Header id ends in 'm' for metric or 'i' for imperial
    if item.attrib['id'].endswith('i'):
        continue
    label = item.text
    if label:
            label = ' '.join(label.split())
    else:
        label = ''
    children = item.getchildren()
    if len(children) > 0:
        list_of_strs = [label]
        for c in children:
            if c.text:
                list_of_strs.append(c.text.strip())
        label = ' '.join([s for s in list_of_strs if s is not ''])
    if label == '':
        label = f'Header{count}'
    logger.info(f"{count:2d}: '{label}'")
    col_labels.append(label)
    if label.startswith('Date / Time'):
        time_col = count
    count += 1
n_columns = count
assert time_col is not None, "time column not recognized"

# Read data from table body
table_body = past_24_table.xpath('tbody')[0]
rows = table_body.getchildren()
logger.info(f"Table has {len(rows)} rows.")
logger.info("Table rows:")
date = datetime.datetime.now().date()
data = []
for i, row in enumerate(rows):
    if row.xpath('th'):
        items = row.xpath('th')
        if len(items) == 1 and items[0].attrib['class'] == 'wxo-th-bkg table-date':
            date = items[0].text
            date = pd.to_datetime(date).date()
        logger.info(f"{i:3d}: Date {date}")

    if row.xpath('td'):
        count = 0
        items = row.xpath('td')
        row_data = []
        for item in items:
            # Header id ends in 'm' for metric or 'i' for imperial
            if item.attrib['headers'].endswith('i') or 'imperial' in item.attrib['class']:
                continue
            text = read_element_text(item)
            row_data.append(text)
            count += 1
        assert count == n_columns, 'Failed to read table row data'
        logger.info(f"{i:3d}: {row_data}")
        
        # Add date time time column
        time = datetime.datetime.strptime(row_data[time_col], "%H:%M").time()
        dt = datetime.datetime.combine(date, time)
        row_data[time_col] = dt.strftime('%Y-%m-%d %H:%M')
        data.append(row_data)
        
# Convert data into Pandas dataframe
df = pd.DataFrame(data, columns=col_labels).set_index(datetime_label).sort_index()

def read_data_from_file(data_dir, year, filename):
    filepath = os.path.join(data_dir, f"{year:d}", filename)
    df = pd.read_csv(filepath)
    return df

def save_data_to_file(df, data_dir, year, filename):
    filepath = os.path.join(data_dir, f"{year:d}", filename)
    df = pd.read_csv(filepath)
    return df

year = date.year
filepath = os.path.join(data_dir, f"{year:d}", filename)

if not os.path.exists(filepath):
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    path = os.path.join(data_dir, f"{year:d}")
    if not os.path.exists(path):
        os.mkdir(path)
    df.to_csv(filepath)
    logger.info(f"Data saved to {filepath}")
else:
    logger.info("Existing file found")
    df_existing = pd.read_csv(filepath, index_col=0, dtype=str)
    assert df_existing.index.name == datetime_label
    df_existing = df_existing.sort_index()
    # Add existing records to current dataframe
    df = pd.concat(
        [df_existing.loc[df_existing.index < df.index[0]], df],
        axis=0
    )
    assert sum(df.index.duplicated()) == 0
    df.to_csv(filepath)
    logger.info(f"Data merged and saved to {filepath}")
