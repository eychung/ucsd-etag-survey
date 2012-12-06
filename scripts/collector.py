import os
import sqlite3
from Queue import Queue
from threading import Thread

db_name = "resource.db"

etag_header = "ETag: "
set_cookie_header = "Set-Cookie: "
server_header = "Server: "

collection_path = "./data 2012-10-14 18.57.12.259013/"
write_file = "list_etags.txt"

class Collector():
	def __init__(self):
		self.num_etags = 0
		self.num_set_cookies = 0
		self.num_max_threads = 50
		self.enclosure_queue = Queue()
		self.db_data = []
		self.etag_files = []
		
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
		
		if set_cookie:
			#self.db_data.append((etag,set_cookie,rank,site,server))
			self.etag_files.append((os.path.basename(f).strip(),etag))

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
		conn = sqlite3.connect(db_name)
		cur = conn.cursor()
		#cur.execute('drop table if exists etag_and_cookie;')
		#cur.execute('''create table etag_and_cookie (etag text, set_cookie text,
		#	rank real, site text, server text)''')
		
		self.setupThreads()

		print "Number of ETags: " + str(self.num_etags)
		print "Number set cookies: " +  str(self.num_set_cookies)
		
		file = open(write_file,'w')
		for item in self.etag_files:
			file.write(str(item)+'\n')
		file.close()
		
		#for a,b,c,d,e in self.db_data:
		#	cur.execute('insert into etag_and_cookie values (?,?,?,?,?)',(a,b,c,d,e,))
		
		conn.commit()
		conn.close()
		
		
		

Collector().main()