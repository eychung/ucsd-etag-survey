import sqlite3
import sys

db_name = "resource.db"
write_file = "db_info.txt"
table_top50k_file = "table_top50k.txt"
table_filtered_file = "table_filtered.txt"

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
	
	def getTop50kList(self):
		list = []
		self.cur.execute('select etag, set_cookie, rank, site, server from top50k')
		for row in self.cur:
			list.append((row[0],row[1],int(row[2]),row[3],row[4]))
		f = open(table_top50k_file,'w')
		for item in list:
			f.write(str(item)+'\n')
		f.close()
		return list
	
	def getFilteredList(self):
		list = []
		self.cur.execute('select etag, set_cookie, rank, site, server from filtered')
		for row in self.cur:
			list.append((row[0],row[1],int(row[2]),row[3],row[4]))
		f = open(table_filtered_file,'w')
		for item in list:
			f.write(str(item)+'\n')
		f.close()
		return list
	
	def getUnfilteredList(self):
		list = []
		self.cur.execute("select etag, set_cookie, rank, site, server from top50k where set_cookie <> ''")
		for row in self.cur:
			list.append((row[0],row[1],int(row[2]),row[3],row[4]))
		f = open(table_filtered_file,'w')
		for item in list:
			f.write(str(item)+'\n')
		f.close()
		return list
	
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
			
	def getNumEtag(self,etag):
		self.cur.execute('select count(etag) from top50k where etag=?',(etag,))
		for row in self.cur:
			print row
	
	def getAllEtagCount(self):
		print "Showing all etags sorted ascending"
		self.cur.execute('select etag, count(etag) as c from top50k group by etag having count(etag)>1 order by c asc;')
		for row in self.cur:
			print row
			
	def getNumEtagInRange(self,low,high):
		print "Showing number of etags in range %d and %d" % (low,high)
		self.cur.execute('select count(site) from top50k where rank>? and rank<?;',(low,high,))
		for row in self.cur:
			print row
	
	def connect(self):
		self.conn = sqlite3.connect(db_name)
		self.cur = self.conn.cursor();
	
	def disconnect(self):
		self.conn.close()
	
	def main(self):
		self.conn = sqlite3.connect(db_name)
		self.cur = self.conn.cursor();

		sys.stdout = open(write_file,'w')

		#self.getEtagsByServer("Apache",1)
		#self.getServerInfo("Apache")
		self.getAllServerInfo()
		#self.getNumEtag('""')
		
		#self.getAllEtagCount()
		
		"""self.getNumEtagInRange(0,10000)
		self.getNumEtagInRange(10000,20000)
		self.getNumEtagInRange(20000,30000)
		self.getNumEtagInRange(30000,40000)
		self.getNumEtagInRange(40000,50000)"""
		
		self.conn.commit()
		self.conn.close()

		
#SQLite().main()