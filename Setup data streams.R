
###########################################################################
################# GETTING DATA FROM MULTIPLE DATA SOURCES #################



################# AWS S3 #################

library("RCurl") 

data <- read.table(textConnection(getURL(
  "https://west.amazonaws.com/abc.csv")), 
  sep=",", header=FALSE)

head(data)


################# STREAMING DATA SETUP #################

library(devtools)
library(stream)
library(ff)


#Set up a data stream

data <- as.ffdf(iris)
data <- data[1:5, ]
mystream <- DSD_FFDFstream(x = data, k = 100, loop=TRUE) 

mystream


################# EXTRACT DATA FROM APIs #################

raw.result <- GET(url = url, path = path)



# If Twitter API 

library(twitteR)
getTwitterOAuth(consumer_key, consumer_secret)

searchTwitter('building', geocode='41.8212,-69.7701, 5mi',  n=5000, retryOnRateLimit=1)
