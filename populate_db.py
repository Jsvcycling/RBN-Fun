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
# populate_db.py: Download raw data from RBN and insert to database.
#
#=================================================

import pandas as pd
import subprocess
import sys

from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text

BASE_URL = "http://www.reversebeacon.net/raw_data/dl.php?f={}"
SQL_INSERT = text("""
INSERT INTO spots (de,freq,band,dx,mode,db,timestamp,speed,tx_mode)
VALUES (:de, :freq , :band, :dx, :mode, :db, :timestamp, :speed, :tx_mode)
""")

if len(sys.argv) != 3:
    print('Usage: ./populate_db.py <start_day> <end_day>')

start_date = datetime.strptime(sys.argv[1], '%Y%m%d').date()
end_date   = datetime.strptime(sys.argv[2], '%Y%m%d').date()

curr_date = start_date

engine = create_engine('mysql+mysqlconnector://rbn_data:ionosphere@localhost/rbn_data')

while curr_date <= end_date:
    url = BASE_URL.format(curr_date.strftime('%Y%m%d'))
    print(url)

    print('Parsing...')
    df = pd.read_csv(url, compression='zip', header=0, parse_dates=['date'], usecols=[
        'callsign', 'freq', 'band', 'dx', 'mode', 'db', 'date', 'speed', 'tx_mode'
    ])

    # Ignore the last line.
    df = df[0:-1]
    
    # Rename some columns.
    df = df.rename(columns={
        'callsign': 'de',
        'date': 'timestamp'
    })

    print('Inserting...')

    conn = engine.connect()

    for idx, row in df.iterrows():
        conn.execute(SQL_INSERT, **row)
    
    # df.to_sql('spots', con=engine, if_exists='append', index=False, chunksize=100)

    curr_date = curr_date + timedelta(days=1)
    del df
