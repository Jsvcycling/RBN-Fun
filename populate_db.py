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

import csv
import subprocess
import sys

from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_USERNAME = 'rbn_data'
DB_PASSWORD = 'ionosphere'
DB_HOSTNAME = 'localhost'
DB_DATABASE = 'rbn_data'

BASE_URL = "http://www.reversebeacon.net/raw_data/dl.php?f={}"

SQL_INSERT = text("""
INSERT INTO spots (de,freq,band,dx,mode,db,timestamp,speed,tx_mode)
VALUES (:callsign, :freq , :band, :dx, :mode, :db, :date, :speed, :tx_mode)
""")

if len(sys.argv) != 3:
    print('Usage: ./populate_db.py <start_day> <end_day>')

start_date = datetime.strptime(sys.argv[1], '%Y%m%d').date()
end_date   = datetime.strptime(sys.argv[2], '%Y%m%d').date()

curr_date = start_date

engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(
    DB_USERNAME, DB_PASSWORD, DB_HOSTNAME, DB_DATABASE
))

conn = engine.connect()

while curr_date <= end_date:
    curr_str = curr_date.strftime('%Y%m%d')

    tmp_zip = '/tmp/{}.zip'.format(curr_str)
    tmp_csv = '/tmp/{}.csv'.format(curr_str)
    
    url = BASE_URL.format(curr_str)
    print(url)

    # Download the compressed CSV file.
    subprocess.run("wget -O {} {}".format(tmp_zip, url), shell=True)

    # Decompress the CSV file.
    subprocess.run("unzip -u {} -d /tmp".format(tmp_zip), shell=True)

    # Remove the summary line from the file.
    subprocess.run("sed -i '$d' {}".format(tmp_csv), shell=True);

    with open(tmp_csv, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile)

        for row in csvreader:
            row['date'] = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
            conn.execute(SQL_INSERT, **row)

    curr_date = curr_date + timedelta(days=1)
