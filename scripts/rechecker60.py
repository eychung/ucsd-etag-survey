import robotparser
import openanything
import sqlite
import urllib2
import httplib
import time
from Queue import Queue
from threading import Thread
#from multiprocessing import Pool
import multiprocessing

urlHeader = "http://www."
write_file = "list_suspicious_range_0_to_100.txt"

num_errors = 0

# Robot finds and sets url of robot.txt in http://www.sitename/robots.txt
# Robot fetches url formatted by http://www.sitename/
class Rechecker60():
	def __init__(self):
		self.list = []
		self.db = sqlite.SQLite()
		self.enclosure_queue = Queue()
		self.num_max_threads = 20
		self.etag_changed = 0
		self.etag_unchanged = 0
		self.cookie_304 = 0
		self.cookie_changed = 0
		self.cookie_unchanged = 0
		self.same_etag = []
		self.same_set_cookie = []
		self.error_site = []
	
	def requestPage(self,etag,set_cookie,rank,site,server):
		global num_errors
		
		print "Checking site: " + str(urlHeader+site)
		robot_url = urlHeader+site+"/robots.txt"
		time.sleep(10)
		rp = robotparser.RobotFileParser()
		rp.set_url(robot_url)
		rp.read()
				
		if rp.can_fetch("*", urlHeader+site+'/') and rp.errcode == 200:
			time.sleep(10)
			
			opener = urllib2.build_opener(openanything.DefaultErrorHandler())
			req = urllib2.Request(urlHeader+site)
			res = opener.open(req)
			try:
				etag1 = res.info()['ETag']
				set_cookie1 = res.info()['Set-Cookie']
				
				print "Trying case 1"
				time.sleep(10)
				# 1. Use set-cookie given by initial connection
				req = urllib2.Request(urlHeader+site, headers={"Cookie" : set_cookie1})
				print "setting res"
				res = opener.open(req)
				print "before etag2"
				etag2 = res.info()['ETag']
				if etag1 == etag2:
					print "ETags are same."
					etag_unchanged += 1
					#self.same_etag.append((site,etag1,set_cookie1))
				else:
					etag_changed += 1
					print "ETags are different."
				
				print "Trying case 2"
				time.sleep(10)
				# 2. Use ETag given by initial connection
				req = urllib2.Request(urlHeader+site, headers={"If-None-Match" : etag1})
				res = opener.open(req)
				if res.getcode() == 304:
					print "Returned 304 Not Modified"
					cookie_304 += 1
				else:
					try:
						set_cookie2 = res.info()['Set-Cookie']
						if set_cookie1 == set_cookie2:
							print "Set the same coookie!"
							cookie_unchanged += 1
							self.same_set_cookie.append((site,etag1,set_cookie1))
						else:
							print "Receieved new cookie."
							cookie_changed += 1
					except:
						print "Does not have Set-Cookie header."
						num_errors += 1
						self.error_site.append(site)
			except:
				print "Problem acquiring either ETag or Set-Cookie in response packet."
				num_errors += 1
				self.error_site.append(site)
		else:
			num_errors += 1
			self.error_site.append(site)
	
	def distributeJobs(self,q,i):
		while True:
			a,b,c,d,e = q.get()
			print "In thread " + str(i)
			self.requestPage(a,b,c,d,e)
			q.task_done()
	
	def setupThreads(self):
		# Set up threads to fetch the enclosures
		for i in range(self.num_max_threads):
			worker = Thread(target=self.distributeJobs, args=(self.enclosure_queue,i,))
			worker.setDaemon(True)
			worker.start()
			#worker.join(timeout = 230)

		for i in range(0,100):
			self.enclosure_queue.put(self.list[i])
			# Wait for the queue to empty, indicating we have processed all downloads
			self.enclosure_queue.join()
	
	def main(self):
		self.db.connect()
		# List contains: etag, set_cookie, rank, site, server
		print "Loading list from database..."
		self.list = self.db.getUnfilteredList()
		self.list = self.list[:100]
		print len(self.list)
		
		for a,b,c,d,e in self.list:
			self.requestPage(a,b,c,d,e)
		
		#print "Setting up threads..."
		#self.setupThreads()
		
		print "Setting up pool..."
		#p = Pool(5)
		#p.map(self.requestPage, self.list)
		#pool = multiprocessing.Pool(processes=20)
		#result = [pool.apply_async(self.requestPage(a,b,c,d,e,)) for a,b,c,d,e in self.list]
		
		f = open(write_file,'w')
		f.write("Sites with same ETags:\n")
		for item in self.same_etag:
			f.write(str(item)+'\n')
		f.write("\n\n")
		f.write("Sites with same Set-Cookie:\n")
		for item in self.same_set_cookie:
			f.write(str(item)+'\n')
		f.write("\n\n")
		f.write("Sites with problems:\n")
		for item in self.error_site:
			f.write(str(item)+'\n')
		f.write("\n\n")
		f.write("# ETag changed: " + str(etag_changed) + '\n')
		f.write("# ETag unchanged: " + str(etag_unchanged) + '\n')
		f.write("# HTTP status 304: " + str(cookie_304) + '\n')
		f.write("# Cookie changed: " + str(cookie_changed) + '\n')
		f.write("# Cookie unchanged: " + str(cookie_unchanged) + '\n')
		f.close()
		
		self.db.disconnect()
		
Rechecker60().main()