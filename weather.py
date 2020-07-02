#!/Users/billtubbs/anaconda/bin/python
""" Set of functions to scrape weather data
from a government 7-day forecast website """


# Default executable path /usr/bin/env python


import urllib
from bs4 import BeautifulSoup
import re
import unicodedata
import datetime as dt
import string
import os
import numpy as np
import pandas as pd

file_col_heads = ("Datetime", "Loc_code", "Location", \
                  "Date_string", "Conditions", "Temperature", \
                  "Pressure", "Humidity", "Wind", "Visibility", \
                  "Forecast_high1", "Forecast_high2", "Forecast_high3", \
                  "Forecast_high4", "Forecast_high5", "Forecast_high6", \
                  "Forecast_low1", "Forecast_low2", "Forecast_low3", \
                  "Forecast_low4", "Forecast_low5", "Forecast_low6", \
                  "Forecast_precip1", "Forecast_precip2", "Forecast_precip3", \
                  "Forecast_precip4", "Forecast_precip5", "Forecast_precip6", \
                  "Forecast_detailed", \
                  "Yesterday_max", "Yesterday_min", "Yesterday_precip")

dtype = [
           ("Datetime", 'datetime64[s],i4'), \
           ("Loc_code", 'a6'), \
           ("Location", 'a50'), \
           ("Date_string", 'a50'), \
           ("Conditions", 'a320'), \
           ("Temperature", 'f4'), \
           ("Pressure", 'f4'), \
           ("Humidity", 'f4'), \
           ("Wind", 'a20'), \
           ("Visibility", 'f4'), \
           ("Forecast_high0", 'f4'), \
           ("Forecast_high1", 'f4'), \
           ("Forecast_high2", 'f4'), \
           ("Forecast_high3", 'f4'), \
           ("Forecast_high4", 'f4'), \
           ("Forecast_high5", 'f4'), \
           ("Forecast_low0", 'f4'), \
           ("Forecast_low1", 'f4'), \
           ("Forecast_low2", 'f4'), \
           ("Forecast_low3", 'f4'), \
           ("Forecast_low4", 'f4'), \
           ("Forecast_low5", 'f4'), \
           ("Forecast_precip0", 'f4'), \
           ("Forecast_precip1", 'f4'), \
           ("Forecast_precip2", 'f4'), \
           ("Forecast_precip3", 'f4'), \
           ("Forecast_precip4", 'f4'), \
           ("Forecast_precip5", 'f4'), \
           ("Forecast_detailed0", 'a320'), \
           ("Forecast_detailed1", 'a250'), \
           ("Forecast_detailed2", 'a250'), \
           ("Forecast_detailed3", 'a250'), \
           ("Forecast_detailed4", 'a250'), \
           ("Forecast_detailed5", 'a250'), \
           ("Yesterday_max", 'f4'), \
           ("Yesterday_min", 'f4'), \
           ("Yesterday_precip", 'f4') \
        ]


def parse_string_for_numbers(aString):
    """Extract numbers from a string"""

    matches = re.findall(r'[+-]?[0-9.]+', aString)
    return matches

# TO-DO: could use matches = filter(startswith, lines)
def find_line(search_str, lines, offset):
    """Search a list of strings for the first one
    that starts with search_str, then return a subsequent
    string offset by a fixed number of rows."""

    for j, line in enumerate(lines):
        if line.startswith(search_str):
            return lines[j + offset].rstrip()
    print "Warning: Couldn't find parameter '" + search_str + "'"
    return None

def find_float(search_str, lines, offset):
    """Search a list of strings for the first one that
    starts with search_str, then return a list of the
    numbers parsed from the subsequent string."""

    line = find_line(search_str, lines, offset)
    if line == 'Trace':
        x = 0.1
    elif line != None:
        s = parse_string_for_numbers(line)
        try:
            x = float(s[0])
        except:
            print "Error converting string '" + line + "' to float"
            return None
        return x

def scrape_weather_forecast(loc_code='sk-32'):
    """Scrape current weather conditions from the
    Government of Canada's 7-day forecast website"""

    url = 'http://weather.gc.ca/city/pages/' + loc_code + '_metric_e.html'
    content = urllib.urlopen(url).read()

    # record current time
    now = dt.datetime.today()

    # parse html
    soup = BeautifulSoup(content, 'html.parser')

    title = soup.title.string
    title = filter(lambda x: x in string.printable, title)
    print now.strftime('%d/%m/%y %H:%M'), "Data read from " + title

    # This is the html id of the div tag that
    # contains the current conditions data
    id_name = "wxo-conditiondetails"
    div = soup.find("div", {"id": id_name}).text

    # convert to a list of lines
    lines = div.splitlines()

    # Use the following lines for debugging
    # print '\n', lines, '\n'
    # raw_input("press enter to continue...")

    # remove blank lines
    lines = [i for i in lines if i[:-1]]

    # remove non-printable characters
    lines = [filter(lambda x: x in string.printable, i) for i in lines]
    # TO-DO replace filter function with a list comprehension

    # create new dataframe to store data
    dataframe = pd.DataFrame(np.zeros(0, dtype=dtype))

    new = {}

    new['Loc_code'] = loc_code
    new['Datetime'] = now
    new['Location'] = find_line('Observed at:', lines, 1)
    new['Date_string'] = find_line('Date:', lines, 1)
    new['Conditions'] = find_line('Condition:', lines, 1)
    new['Temperature'] = find_float('Temperature:', lines, 1)
    new['Pressure'] = find_float('Pressure:', lines, 1)
    new['Humidity'] = find_float('Humidity:', lines, 1)
    new['Visibility'] = find_float('Visibility:', lines, 1)
    new['Wind'] = find_line('Wind:', lines, 1)

    # Now scrape the 7 day forecast data
    id_name = "wxo-cityforecast"
    forecast = soup.find("section", {"id": id_name}).text

    # convert to a list of lines
    lines = forecast.splitlines()

    # remove blank lines
    lines = [i for i in lines if i[:-1]]

    # remove non-printable characters
    lines = [filter(lambda x: x in string.printable, i) for i in lines]
    # TO-DO replace filter function with a list comprehension

    # Loop through the forecast for the next 6 days

    # Use the added statement to offset the time (e.g. back 3 hours)
    # if the site doesn't update exactly at midnight

    day = now    # - dt.timedelta(hours=3)

    for i in range(6):
        day = day + dt.timedelta(days=1)
        day_name = day.strftime('%a')
        line_text = find_line(day_name, lines, 2)

        if line_text.find("%") >= 0:
            j = 3
            new['Forecast_precip' + str(i)] = find_float(day_name, lines, 2)
        else:
            j = 2
            new['Forecast_precip' + str(i)] = 0

        new['Forecast_high' + str(i)] = find_float(day_name, lines, j)
        new['Forecast_low' + str(i)] = find_float(day_name, lines, j + 1)


    # Loop through the detailed forecast for the next 6 days

    day = now - dt.timedelta(hours=3)
    for i in range(6):
        day = day + dt.timedelta(days=1)
        date_string = day.strftime('%a,') + day.strftime('%d').lstrip('0') +  day.strftime('%b')
        new['Forecast_detailed' + str(i)] = find_line(date_string, lines, 1)

    # now scrape the historical conditions
    id_name = "historicaldata"
    forecast = soup.find("section", {"id": id_name}).text

    # convert to a list of lines
    lines = forecast.splitlines()

    # remove blank lines
    lines = [i for i in lines if i[:-1]]

    # remove non-printable characters
    lines = [filter(lambda x: x in string.printable, i) for i in lines]
    # TO-DO replace filter function with a list comprehension

    new['Yesterday_max'] = find_float('Max:', lines, 1)
    new['Yesterday_min'] = find_float('Min:', lines, 1)
    new['Yesterday_precip'] = find_float('Precip:', lines, 1)

    # Now add the data to the data frame

    dataframe = dataframe.append(new, ignore_index=True)

    return dataframe



# If this script is executed, then the following
# code will run and download the current website
# contents, print the data on the screen, and
# save it to a text file.

if __name__ == "__main__":

    # Set the location code for the Canadian
    # city you want to see the weather data for

    loc_codes = {'Victoria': 'bc-85', \
        'Vancouver': 'bc-74', \
        'Prince George': 'bc-79', \
        'Whitehorse': 'yt-16', \
        'Calgary': 'ab-52', \
        'Edmonton': 'ab-50', \
        'Yellowknife': 'nt-24', \
        'Regina': 'sk-32', \
        'Winnipeg': 'mb-38', \
        'Thunder Bay': 'on-100', \
        'Toronto': 'on-143', \
        'Ottawa': 'on-118', \
        'Iqualuit': 'nu-21', \
        'Montreal': 'qc-147', \
        'Quebec': 'qc-133', \
        'Fredericton': 'nb-29', \
        'Halifax': 'ns-19', \
        "St. John's": 'nl-24'}

    filename = 'weather_data_ca.csv'
    f = None

    for city in loc_codes:

        weather = scrape_weather_forecast(loc_codes[city])

        # Print all the data values
        # for i in range(len(dtype)):
        #    print dtype[i][0] + ":", weather[dtype[i][0]][0]

        # Save to text file
        # First check if text file exists
        if not os.path.isfile(filename):
            weather.to_csv(filename, index=False)
        elif f == None:
            f = open(filename, 'a')
        else:
            weather.to_csv(f, header=False, index=False)

        time_now = dt.datetime.today()
        print time_now.strftime('%d/%m/%y %H:%M'), "Data saved to " + filename

