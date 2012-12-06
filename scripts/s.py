import robotparser
import urllib2
import httplib
import os
import time
import datetime
from Queue import Queue
from threading import Thread
import sys
import sqlite3

db_name = "resource.db"

filename = "top-1m.csv"
urlHeader = "http://www."
data_path = "./data"
max_sites = 50000
site = ""
url = ""
rank = ""

etag_header = "ETag: "
set_cookie_header = "Set-Cookie: "
server_header = "Server: "

collection_path = "./data 2012-10-14 18.57.12.259013/"

etag_map = {}
server_map = {}
collision_list = []

class SQLite():
	def __init__(self):
		self.conn = None
		self.cur = None
		
	def create(self):
		self.conn = sqlite3.connect(db_name)
		self.cur = self.conn.cursor();
		self.cur.execute('''create table top50k (etag text, set_cookie text,
			rank real, site text, server text)''')
		self.conn.commit()
		self.conn.close()
	
	def getEtagsByServer(self,server,count):
		print "Fetching etags and number of shared etags with count > " + str(count)
		self.cur.execute('select etag, count(etag) from top50k where server=? group by etag having count(etag)>?;',(server,count))
		for row in self.cur:
			print row
		
	def getAllServerInfo(self):
		print "Showing server information..."
		self.cur.execute('select server, count(server) as c from top50k group by server order by c asc')
		for row in self.cur:
			print row
	
	def getServerInfo(self,server):
		print "Showing server information..."
		self.cur.execute('select server, count(server) as c from top50k where server=? group by server order by c asc',(server,))
		for row in self.cur:
			print row
	
	def main(self):
		self.conn = sqlite3.connect(db_name)
		self.cur = self.conn.cursor();

		sys.stdout = open('db_info.txt','w')
		
		#self.getEtagsByServer("Microsoft-IIS/7.5",2)
		#self.getEtagsByServer("Microsoft-IIS/7.5",10)
		#self.getAllServerInfo()
		self.getEtagsByServer("Apache",1)
		self.getServerInfo("Apache")
		
		self.conn.commit()
		self.conn.close()

		
class Collector():
	def __init__(self):
		self.num_etags = 0
		self.num_set_cookies = 0
		self.num_max_threads = 20
		self.db_data = []
		self.enclosure_queue = Queue()

	def collect(self,f):
		etag = set_cookie = server = ""
		name = os.path.splitext(os.path.basename(f))[0]
		rank = name[:name.find(' ')]
		site = name[name.find(' ')+1:]
		
		file = open(f,'r')
		for i in range(0,30):
			line = file.readline()
			if line.startswith(etag_header):
				self.num_etags+=1
				etag = line[len(etag_header):].strip()
			if line.startswith(set_cookie_header):
				if not set_cookie:
					self.num_set_cookies+=1
				set_cookie += line[len(set_cookie_header):].strip() + ' '
			if line.startswith(server_header):
				server = line[len(server_header):].strip()
		
		if etag:
			self.db_data.append((etag,set_cookie,rank,site,server))
			if etag not in etag_map:
				etag_map[etag] = (site, set_cookie.strip(), server)
			else:
				v1, v2, v3 = etag_map[etag]
				#print "Etag map collision detected."
				#print "Stored site " + str(v1) + " conflicts with " + site + " with Etag " + etag
		
		if server not in server_map:
			if etag:
				server_map[server] = (1,0)
			else:
				server_map[server] = (1,1)
		else:
			v1, v2 = server_map[server]
			if etag:
				server_map[server] = (v1+1,v2)
			else:
				server_map[server] = (v1+1,v2+1)

	def distributeJobs(self,q):
		while True:
			file = q.get()
			self.collect(collection_path+file)
			q.task_done()

	def setupThreads(self):
		# Set up threads to fetch the enclosures
		for i in range(self.num_max_threads):
			worker = Thread(target=self.distributeJobs, args=(self.enclosure_queue,))
			worker.setDaemon(True)
			worker.start()

		for file in os.listdir(collection_path):
			self.enclosure_queue.put(file)
			# Wait for the queue to empty, indicating we have processed all downloads
			self.enclosure_queue.join()
			
	def main(self):
		global conn
		global cur
		
		conn = sqlite3.connect(db_name)
		cur = conn.cursor();
		cur.execute('''create table top50k (etag text, set_cookie text,
			rank real, site text, server text)''')
		
		self.setupThreads()

		print "Number of ETags: " + str(self.num_etags)
		print "Number set cookies: " +  str(self.num_set_cookies)
		
		for a,b,c,d,e in self.db_data:
			cur.execute('insert into top50k values (?,?,?,?,?)',(a,b,c,d,e,))
		
		conn.commit()
		conn.close()


class Downloader():
	def readFile(self,f):
		global site
		global url
		global rank
		f = open(filename)
		lines = f.readlines()

		for i in range(28876, max_sites):
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
			print "Creating robot parser for " + site
			rp = robotparser.RobotFileParser()
			print "Setting url"
			rp.set_url(site)
			print "Reading url"
			rp.read()
			print "scanning " + site
				
			if rp.can_fetch("*", url+'/') and rp.errcode == 200:
				print "fetched " + site
				file = open(data_path+"/"+rank+" "+url+".txt",'w')
				print "Setting up request"
				req = urllib2.Request(site)
				print "Sending request"
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

		
	
if __name__ == '__main__':
	#Downloader().main()
	#Collector().main()
	SQLite().main()