import robotparser
import urllib2
import httplib
import datetime
import os
import time
import threading

filename = "top-1m.csv"
urlHeader = "http://www."
data_path = "./data"
max_sites = 50000
site = ""
url = ""
rank = ""

class Downloader():
	def readFile(self,f):
		global site
		global url
		global rank
		f = open(filename)
		lines = f.readlines()

		for i in range(0, max_sites):
			line = lines[i]
			rank = line[:line.find(',')]
			url = line[line.find(',')+1:].strip()
			site = urlHeader + url
			t = threading.Thread(target=self.download_site)
			t.daemon = True
			t.start()
			t.join(timeout = 15)
				
		f.close()

	def redownload(self):
		dir = os.listdir(data_path)

		for f in dir:
			if os.path.getsize(data_path+"/"+f) == 0:
				rank = f[:f.find(' ')]
				url = f[f.find(' ')+1:f.find('.txt')]
				site = urlHeader + url
				t = threading.Thread(target=download_site)
				t.daemon = True
				t.start()
				t.join(timeout = 50)
		
	def download_site(self):
		global site
		global url
		global rank
		try:
			rp = robotparser.RobotFileParser()
			rp.set_url(site)
			rp.read()
				
			if rp.can_fetch("*", url+'/') and rp.errcode == 200:
				file = open(data_path+"/"+rank+" "+url+".txt",'w')
				req = urllib2.Request(site)
				res = urllib2.urlopen(req)
					
				file.write(str(req.header_items()).strip('[]')+'\n\n\n')
				file.write(str(res.info())+'\n\n')
				file.write(res.read())
					
				print "Finished writing"
				res.close()
				file.close()
				print "Success with " + site
			else:
				print "Could not fetch data for " + site
		except IOError:
			print "IOError"
			pass

	def main(self):
		global data_path
		
		t = os.path.getmtime(filename)
		u = str(datetime.datetime.fromtimestamp(t)).replace(':','.')
		print u
		
		data_path = data_path+' '+u
		
		try:
			os.makedirs(data_path)
		except OSError:
			pass


Downloader().main()