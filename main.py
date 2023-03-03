import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, MetaData,func
from sqlalchemy.ext.declarative import declarative_base
import csv
from datetime import datetime, timedelta
import pandas as pd

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

#creating metadata to reflect the schema of our database hawaii.sqlite
metadata = MetaData()
metadata.reflect(bind=engine)
base = automap_base(metadata=metadata)
base.prepare()

#checking how many tables are there in our database
for name, cls in base.classes.items():
    print(name)

#from base.classes we get the object of our classes that is being reflected by the database hawaii.sqlite
station = base.classes.station
measurment=base.classes.measurement

Session = sessionmaker(bind=engine)
session = Session()

#connection between python and database
# Query the station table in hawaii.sqlite database and print the results
stations = session.query(station).all()
for temp_stations in stations:
    print(temp_stations.name, temp_stations.elevation)

# Query the measurment table in hawaii.sqlite database and print the results
# measure = session.query(measurment).all()
# for temp_measurement in measure:
#     print(temp_measurement.date, temp_measurement.prcp)

#-------------------------------------------
#Now the Precipitation Analysis:
#-------------------------------------------


#getting most recent date from dataset
#Method no 1:

#fetching all the data from dataset
measure = session.query(measurment).all()

# Initialize the most recent date to None
most_recent_date = None
for temp_measurement in measure:
    date_str=temp_measurement.date
    date = datetime.strptime(date_str, '%Y-%m-%d')

    ##Convert most_recent_date to a datetime object if it's a string
    if isinstance(most_recent_date, str):
        most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')

    ## Update the most recent date if necessary
    if most_recent_date is None or date > most_recent_date:
        most_recent_date = date_str
print(most_recent_date.strftime('%Y-%m-%d'))

#second way:
most_recent_date2 = session.query(measurment.date).order_by(measurment.date.desc()).first()[0]
print(most_recent_date2)

start_date = datetime.strptime(most_recent_date2, '%Y-%m-%d') - timedelta(days=365)

previous_year_results = session.query(measurment.date, measurment.prcp).filter(measurment.date >= start_date)\
                  .filter(measurment.date <= most_recent_date)\
                  .all()
for date, prcp in previous_year_results:
    print(f"Date: {date}, Precipitation: {prcp}")

#*****************its for filling ************************
# Open the file for reading
# with open('Resources/hawaii_measurements.csv', 'r') as f:
#     reader = csv.reader(f)
#
#     # Read the first line to get the header
#     header = next(reader)
#
#     # Initialize the most recent date to None
#     most_recent_date = None
#
#     # Iterate over each record in the file
#     for row in reader:
#         # Extract the date from the row
#
#         date_str = row[1]# 1 because the index is one from left in data
#         date = datetime.strptime(date_str, '%Y-%m-%d')
#
#         ## Convert most_recent_date to a datetime object if it's a string
#         if isinstance(most_recent_date, str):
#             most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
#
#
#         # Update the most recent date if necessary
#         if most_recent_date is None or date > most_recent_date:
#             most_recent_date = date_str
# print(most_recent_date.strftime('%Y-%m-%d'))

