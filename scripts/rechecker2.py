import sqlite
import urllib2
import httplib
import threading

urlHeader = "http://www."

class Rechecker():
	def __init__(self):
		self.list = []
		self.db = sqlite.SQLite()
		self.cookie_count = 0
		self.etag_changed = 0
		self.etag_unchanged = 0
		self.suspiciousList = []
	
	def requestPage(self,etag,set_cookie,rank,site,server):
		print "Checking site: " + str(site)
		try:
			req = urllib2.Request(urlHeader+site)
			res = urllib2.urlopen(req)
			try:
				new_etag = res.info()['ETag']
				print str(new_etag) + " and " + str(etag)
				if new_etag == etag:
					self.etag_unchanged += 1
				else:
					self.etag_changed += 1
					try:
						new_set_cookie = res.info()['Set-Cookie']
						self.cookie_count += 1
						suspiciousList.append((etag,new_etag,set_cookie,new_set_cookie,rank,site,server))
					except:
						print "No set-cookie found."
			except:
				print "Expected ETag but none found."
			res.close()
		except:
			print "Connection error."
	
	def main(self):
		self.db.connect()
		# etag, set_cookie, rank, site, server
		self.list = self.db.getFilteredList()
		
		print len(self.list)
		# list contains filtered sites (734)
		for a,b,c,d,e in self.list:
			t = threading.Thread(target=self.requestPage,args=(a,b,c,d,e,))
			t.daemon = True
			t.start()
			t.join(timeout = 15)
		
		print "Set-Cookie count: " + str(self.cookie_count)
		print "ETag changed count: " + str(self.etag_changed)
		print "ETag unchanged count: " + str(self.etag_unchanged)
		
		f = open("suspicious_list.txt",'w')
		for item in self.suspiciousList:
			f.write(str(item)+'\n')
		f.close()
		
		self.db.disconnect()
		
Rechecker().main()