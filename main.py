import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, MetaData, func, desc
from sqlalchemy.ext.declarative import declarative_base
import csv
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

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

# ************************ ************************
#Now the Precipitation Analysis:
# ************************ ************************


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

#************ second way *****************:
most_recent_date2 = session.query(measurment.date).order_by(measurment.date.desc()).first()[0]
print(most_recent_date2)

start_date = datetime.strptime(most_recent_date2, '%Y-%m-%d') - timedelta(days=365)

previous_year_results = session.query(measurment.date, measurment.prcp).filter(measurment.date >= start_date)\
                  .filter(measurment.date <= most_recent_date)\
                  .all()
for date, prcp in previous_year_results:
    print(f"Date: {date}, Precipitation: {prcp}")

# ************************ ************************
# its for file reading
# ************************ ************************
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



#****************************************************************
# now using pandas:
#****************************************************************

# Load the query results into a pandas DataFrame
df = pd.DataFrame(previous_year_results, columns=['date', 'precipitation'])

# Set the index to the "date" column
df.set_index('date', inplace=True)


#using the DataFrame plot method
# df_sorted = df.sort_values('date')
#
# df_sorted.plot()
# plt.show()

#using Pandas to print the summary statistics for the precipitation data.
precipitation_stats = df['precipitation'].describe()
print(precipitation_stats)


# ************************ ************************
#Now the station Analysis:
# ************************ ************************

#Query to count the number of rows in the "station" table
station_count = session.query(func.count(station.id)).scalar()
print(station_count)

#Query for counting the observation count for every station.
observation_count_results = session.query(measurment.station, func.count(measurment.station),measurment.date).\
            group_by(measurment.station).\
            order_by(desc(func.count(measurment.station))).all()

# for one_row in observation_count_results:
#     print(one_row)

station_id_with_most_observations =observation_count_results[0][0]
station_with_most_observations_count =observation_count_results[0][1] #because it is already sorted in desc order

statistics_of_most_observation = session.query(func.min(measurment.tobs), func.max(measurment.tobs), func.avg(measurment.tobs)) \
                 .filter(measurment.station == station_id_with_most_observations) \
                 .all()

print(f"Lowest Temperature: {statistics_of_most_observation[0][0]}")
print(f"Highest Temperature: {statistics_of_most_observation[0][1]}")
print(f"Average Temperature: {statistics_of_most_observation[0][2]}")



date_of_having_most_observation_station =observation_count_results[0][2]
print (date_of_having_most_observation_station)
last_year_date = datetime.strptime(date_of_having_most_observation_station, '%Y-%m-%d') - timedelta(days=365)
previous_year_results_for_tob = session.query(measurment.station, measurment.tobs).filter(measurment.date >= last_year_date)\
                  .filter(measurment.date <= date_of_having_most_observation_station)\
                  .all()
# for station, tobs in previous_year_results_for_tob:
#     print(f"station: {station}, TOBS: {tobs}")


temperatures = []
for row in previous_year_results_for_tob:
    temperatures.append(row[1])

# Plot the histogram
plt.hist(temperatures, bins=12)
plt.xlabel("Temperature ")
plt.yticks(range(0, 10, 1))
plt.xticks(range(50,85,5))
plt.ylabel("Frequency")
plt.title(f"Temperature Observations for Station")
plt.show()


#closing the session:
session.close()
