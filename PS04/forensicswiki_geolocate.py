#
# You are on your own!
#
import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##
    sc     = SparkContext( appName="Geolocate" )
    infile1 =  's3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv'
    infile2 =  's3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv'
   
    lines1 = sc.textFile(infile1)
    lines2 = sc.textFile(infile2)
    
    inlines1 = lines1.zipWithIndex().filter(lambda x : x[1]>0).map(lambda x : x[0])
    inlines2 = lines2.zipWithIndex().filter(lambda x : x[1]>0).map(lambda x : x[0])
    
    join1 = lines1.map(lambda x: (x[1], x.split(',')[0]).split('/')[0])
    join2 = lines2.map(lambda x: (x[0], x[-1]))
    joined = join1.join(join2)
    ip_country = joined.map(lambda x : x[1])
    ip_country.collect()
    

