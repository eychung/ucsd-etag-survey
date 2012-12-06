import robotparser
import openanything
import sqlite
import eventlet
from eventlet.green import urllib2
import httplib
import time
import multiprocessing
import sys

urlHeader = "http://www."
write_file = "list_suspicious7.txt"

num_errors = 0

# Robot finds and sets url of robot.txt in http://www.sitename/robots.txt
# Robot fetches url formatted by http://www.sitename/
class Rechecker():
	def __init__(self):
		self.list = []
		self.db = sqlite.SQLite()
		self.cookie_count = 0
		self.etag_changed = 0
		self.etag_unchanged = 0
		self.cookie_304 = 0
		self.cookie_changed = 0
		self.cookie_unchanged = 0
		self.err_robot = 0
		self.err_permission = 0
		self.same_etag = []
		self.same_set_cookie = []
		self.error_site = []
	
	def requestPage(self,etag,set_cookie,rank,site,server):
		try:
			global num_errors
			
			print "Checking site: " + str(urlHeader+site)
			sys.stdout.flush()
			
			robot_url = urlHeader+site+"/robots.txt"

			rp = robotparser.RobotFileParser()
			rp.set_url(robot_url)
			rp.read()
					
			if rp.errcode == 404 or (rp.can_fetch("*", urlHeader+site+'/') and rp.errcode == 200):
				try:
					time.sleep(60)
					opener = urllib2.build_opener(openanything.DefaultErrorHandler())
					req = urllib2.Request(urlHeader+site)
					res = opener.open(req)
					
					etag1 = res.info()['ETag']
					set_cookie1 = res.info()['Set-Cookie']
					print str(etag1) + " and " + str(set_cookie1)
					
					print "Trying case 1" + str(urlHeader+site)
					time.sleep(60)
					# 1. Use set-cookie given by initial connection
					req = urllib2.Request(urlHeader+site, headers={"Cookie" : set_cookie1})
					res = opener.open(req)
					etag2 = res.info()['ETag']
					if etag1 == etag2:
						print "ETags are same."
						self.same_etag.append((site,etag1,set_cookie1))
						self.etag_unchanged += 1
					else:
						print "ETags are different."
						self.etag_changed += 1
					
					time.sleep(60)
					print "Trying case 2" + str(urlHeader+site)
					# 2. Use ETag given by initial connection
					req = urllib2.Request(urlHeader+site, headers={"If-None-Match" : etag1})
					res = opener.open(req)
					if res.getcode() == 304:
						print "Returned 304 Not Modified"
						self.cookie_304 += 1
					else:
						try:
							set_cookie2 = res.info()['Set-Cookie']
							if set_cookie1 == set_cookie2:
								print "Set the same coookie!"
								self.same_set_cookie.append((site,etag1,set_cookie1))
								self.cookie_unchanged += 1
							else:
								print "Receieved new cookie."
								self.cookie_changed += 1
						except:
							print "Does not have Set-Cookie header."
							self.error_site.append(site)
							num_errors += 1
				except:
					print "Problem acquiring either ETag or Set-Cookie in response packet."
					self.error_site.append(site)
					num_errors += 1
			else:
				print "No permissions given to robot."
				self.error_site.append(site)
				self.err_permission += 1
		except:
			print "Error connecting to robot."
			self.error_site.append(site)
			self.err_robot += 1
		return site
	
	def main(self):
		self.db.connect()
		# List contains: etag, set_cookie, rank, site, server
		self.list = self.db.getUnfilteredList()
		self.list = self.list[600:700]
		
		for a,b,c,d,e in self.list:
			self.requestPage(a,b,c,d,e)
		
		#pool = eventlet.GreenPool(200)
		#for site in pool.imap(self.requestPage, self.list):
		#	print "Done with " + str(site)
		
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
		f.write("# ETag changed: " + str(self.etag_changed) + '\n')
		f.write("# ETag unchanged: " + str(self.etag_unchanged) + '\n')
		f.write("# HTTP status 304: " + str(self.cookie_304) + '\n')
		f.write("# Cookie changed: " + str(self.cookie_changed) + '\n')
		f.write("# Cookie unchanged: " + str(self.cookie_unchanged) + '\n')
		f.write("# Err general: " + str(num_errors) + '\n')
		f.write("# Err robot: " + str(self.err_robot) + '\n')
		f.write("# Err permission: " + str(self.err_permission) + '\n')
		f.close()
		
		self.db.disconnect()
		
Rechecker().main()