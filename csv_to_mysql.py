#!/usr/bin/env python3
#=================================================
#
# Copyright (C) 2018 Joshua Vega
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#-------------------------------------------------
#
# download_data.py: Connect to the RBN website and download the raw data
# files. Clean up the data and insert it into a MySQL database for processing.
#
#=================================================

import datetime
import pandas as pd
import requests
import sys

from sqlalchemy import create_engine

if len(sys.argv) != 2:
    print('Usage: ./csv_to_mysql.py <filename>')

# Load the CSV file into a DataFrame.
df = pd.read_csv(sys.argv[1], header=0, parse_dates=['date'], usecols=[
    'callsign', 'freq', 'band', 'dx', 'mode', 'db', 'date', 'speed', 'tx_mode'
])

# Ignore the last line.
df = df[0:-1]

# Rename some columns.
df = df.rename(columns={
    'callsign': 'de',
    'date': 'timestamp'
})

engine = create_engine('mysql+mysqlconnector://rbn_data:ionosphere@localhost/rbn_data')
df.to_sql('spots', con=engine, if_exists='append', index=False, chunksize=100)
