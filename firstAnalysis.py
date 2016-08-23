
# The task of this analysis is to find the average number of passengers in each airline of US Region


#set up the hadoop configuration in Bluemix

def set_hadoop_config(credentials):
    prefix = "fs.swift.service." + credentials['name'] 
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", credentials['auth_url']+'/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", credentials['project_id'])
    hconf.set(prefix + ".username", credentials['user_id'])
    hconf.set(prefix + ".password", credentials['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", credentials['region'])
    hconf.setBoolean(prefix + ".public", True)
	
#Load the dataset into hadoop

credentials = {}
credentials['name'] = 'books'
credentials['auth_url'] = 'https://identity.open.softlayer.com'
credentials['project_id'] = 'e4a7c55b22a140ddadd510872b3414a3'
credentials['region'] = 'dallas'
credentials['user_id'] = 'c984c9f8643c46b8819787e4e1894098'
credentials['password'] = 'o-4w_=!~!.~t2uMR'

set_hadoop_config(credentials)

# Read the file
flight = sc.textFile("swift://notebooks.books/flight.csv")

# print the total number of records in the datasets
print "Total number of lines in the  dataset:", flight.count()

# print the first line of the data
print "the first line of the data:", flight.first()

# Split the Data
flightParse = flight.map(lambda line : line.split(","))

# view the first data
flightParse.first()

# print the first column of the datasets
flightParse.first()[0]

# filter only the US region flights
flightCountry = flightParse.filter(lambda x: x[6] == "US")

# Total number of records contain US as Region
flightCountry.count()

# inspect the first line of the flightCountry
flightCountry.first()

# Find the average of passenger count of each airline in the US Region
# x[1] is operating airline
# x[11] is the passenger count
passengerCountbyAirline = flightCountry.map(lambda x : (x[1], (int(x[11]), 1)))

# Record Count
passengerCountbyAirline.count()

# inspect the first airline
passengerCountbyAirline.first()

# Sum of passesnger count in each station
averagePassengerCountbyairline = passengerCountbyAirline.reduceByKey(lambda v1,v2 : (v1[0]+v2[0], v1[1]+v2[1]))

# inspect the first element
averagePassengerCountbyairline.first()

# find the averge number of passengers by each airline
airlineAverages = averagePassengerCountbyairline.map(lambda k: (k[0], k[1][0] / float(k[1][1] ) ) )

# inspect the output
airlineAverages.first()

# print the first 10 records of the airline averages
airlineAverages.top(10)

# print the top 10 airlines with the passenger count
airlineTop10=[]
countTop10=[]
for pair in airlineAverages.map(lambda (x,y) : (y,x)).top(10):
    airlineTop10.append(pair[0])
    countTop10.append(pair[1])
print "Airlines in the US %s had average passenge count of %f" % (pair[1],pair[0])

# Interactive plots to show the results
%matplotlib inline
import numpy as np
import matplotlib.pyplot as plt
N = 10
index = np.arange(N)
bar_width = 0.5
plt.bar(index, airlineTop10, bar_width,
color='r')
plt.xlabel('Airlines')
plt.ylabel('Passenger Count')
plt.title('Top 10 Airlines in the US region from March 2005 - March 2015')
plt.xticks(index + bar_width, countTop10, rotation=90)
plt.show()

# filter the results with only Euro Region
euroRegion = flightParse.filter(lambda x: x[6]=="Europe")
euroRegion.count()

# To make complex aggregations in python, we can use SparkSql libray
from pyspark.sql import SQLContext, Row
from datetime import datetime
# instantiate SQLContext object
sqlContext = SQLContext(sc)

# Convert each line of euroRegion RDD into a Row object
euroRows= euroRegion.map(lambda p: Row(
                                        airline=p[1], 
                                        GeoSummary=p[5], 
                                        GeoRegion=p[6],
                                        PriceCategory=p[8],
                                        passengercount=int(p[11]),
                                        Year=int(p[14]),
                                        Month=p[15],
                                    )
                         )

# Apply Row schema
euroSchema = sqlContext.createDataFrame(euroRows)

# Register 'euroRegion' table with 7 columns: airline, summary, region, price category, passenger count, year, month
euroSchema.registerTempTable("euroRegion")
sqlContext.cacheTable("euroRegion")

# average passenger count of airline in the month of june in euro region
passengerCountbyEachMonth = sqlContext.sql("SELECT airline, AVG(passengercount)FROM euroRegion WHERE Month = 'June' GROUP BY airline").collect()

# print the results
print passengerCountbyEachMonth

# average passenger count of virgin atlantic airline in the every year
avgPassengerCountVirginAirline = sqlContext.sql("SELECT Year,AVG(passengercount)FROM euroRegion WHERE airline='Virgin Atlantic'  GROUP BY Year").collect()

print avgPassengerCountVirginAirline

# average passenger count of united airlines airline in the every year
avgPassengerCountUnitedAirline = sqlContext.sql("SELECT Year,AVG(passengercount)FROM euroRegion WHERE airline='United Airlines'  GROUP BY Year").collect()

print avgPassengerCountUnitedAirline

# average passenger count of virgin lufthansa in the every year
avgPassengerCountLufthansaAirline = sqlContext.sql("SELECT Year,AVG(passengercount)FROM euroRegion WHERE airline='Lufthansa German Airlines'  GROUP BY Year").collect()

print avgPassengerCountLufthansaAirline

