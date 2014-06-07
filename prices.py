#!/usr/bin/python

from xml.etree import ElementTree
import psycopg2
import datetime
import urllib2
import time
import csv

class db_connection(object):
	def __init__(self):
		with open('/home/essbase/credentials.txt', 'rb') as csvfile:
			reader = csv.reader(csvfile)
			for row in reader:
				credentials = row[0]
				conn = psycopg2.connect(credentials)
				try:
					self.conn = conn
				except:
					print "I am unable to connect to 'edw'"
	
	def cities(self):
		sql = "select distinct a.scity, a.sstate, concat('&city=', regexp_replace(a.scity, ' ', '%20', 'g'), '&state=', a.sstate) as location from staging_essex.property a, staging_essex.listprop b where a.hmy = b.hproperty and b.hproplist = 357 and a.scity is not null"

		cur = self.conn.cursor()
		cur.execute(sql)

		self.rows = []
		self.rows = cur.fetchall()

		return self.rows
		self.conn.close()
		
class trulia(db_connection):

	# API key to access Trulia as biESSEX
	api_key = "1234ab5cdef678ghi9j0klmn"
	
	# date variables
	start_date = "2014-05-01"
	end_date = "2014-05-23"
	
	def query(self):
		for c in self.rows:
			scity = c[0]
			sstate = c[1]
			location = c[2]
		
			# build the URL
			# example = ("http://api.trulia.com/webservices.php?library=TruliaStats&function=getCityStats&city=%s&state=%s&startDate=%s&endDate=%s&apikey=%s" %
			#	(self.city, self.state, self.start_date, self.end_date, self.api_key))
			
			url = ("http://api.trulia.com/webservices.php?library=TruliaStats&function=getCityStats%s&startDate=%s&endDate=%s&apikey=%s" %
				(location, self.start_date, self.end_date, self.api_key))

			# make the request and parse xml
			req = urllib2.Request(url)
			data = urllib2.urlopen(req)
			root = ElementTree.parse(data).getroot()
		
			for a in root.findall('response/TruliaStats/listingStats/listingStat'):
				date = a.find('weekEndingDate').text
				for b in a.findall('listingPrice/subcategory'):
					type = b.find('type').text
					number = b.find('numberOfProperties').text
					median = b.find('medianListingPrice').text
					average = b.find('averageListingPrice').text
				
					row = date, scity, sstate, type, number, median, average
					#self.writer.writerow(row)

					sql = "INSERT INTO trulia.prices (date, city, state, type, number, median, average) values (%s, %s, %s, %s, %s, %s, %s)"
			
					cur = self.conn.cursor()
					cur.execute(sql, row)
					self.conn.commit()
					
			# delay the next call for six seconds, can only make up to 30 calls a minute
			time.sleep(3)
		# close the db connection
		self.conn.close()				
				
### main ###
real_estate = trulia()
real_estate.cities()
real_estate.query()
