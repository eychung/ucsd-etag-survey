import re
import sqlite

read_file = "list_etags.txt"
write_file = "list_filtered.txt"

class Filter():
	def __init__(self):
		self.dict = {}
		self.removeList = []
		self.db = sqlite.SQLite()

	def fillList(self):
		f = open(read_file,'r')
		for line in f.readlines():
			a = line[2:line.find(',')-1]
			b = line[line.find(',')+3:len(line.strip())-2]
			self.dict[a] = b
		"""self.db.cur.execute('select site,etag from top50k')
		for row in self.db.cur:
			self.dict[row[0]] = row[1]"""
		print "Initial list size: " + str(len(self.dict))

	def filterFrequency(self,freq):
		self.db.cur.execute('select t1.rank, t1.site, t1.etag from top50k t1 join (select t2.rank, t2.site, t2.etag from top50k t2 group by t2.etag having count(*)>?) u on u.etag = t1.etag',(freq,))
		for row in self.db.cur:
			s = str(int(row[0]))+' '+str(row[1])+'.txt'
			self.removeList.append(s)
		for item in self.removeList:
			try:
				del self.dict[item]
			except:
				pass
		del self.removeList[:]
		print "Filtered by frequency val " + str(freq) + "; list size: " + str(len(self.dict))
		
	def filterApache(self):
		self.db.cur.execute('select rank,site,etag from top50k where server like ?',('Apache%',))
		prog = re.compile(r'^(")?[a-fA-F_0-9]{32}(:)[a-fA-F_0-9]{10}(")?$')
		for row in self.db.cur:
			s = str(int(row[0]))+' '+str(row[1])+'.txt'
			if row[2].count('-') == 2:
				self.removeList.append(s)
			elif prog.search(row[2]):
			#if prog.search(row[2]):
				self.removeList.append(s)
		for item in self.removeList:
			try:
				del self.dict[item]
			except:
				pass
		del self.removeList[:]
		print "Filtered Apache; list size: " + str(len(self.dict))
	
	def filterMicrosoftIIS(self):
		# Format: Filetimestamp:ChangeNumber (0222d5bffcbc41:3246)
		# http://support.microsoft.com/kb/922703
		self.db.cur.execute('select rank,site,etag from top50k where server like ?',('Microsoft-IIS%',))
		for row in self.db.cur:
			self.checkFileTimeStamp(row[0],row[1],row[2])
			if not row[2].find('ASP.NET'):
				s = str(int(row[0]))+' '+str(row[1])+'.txt'
				self.removeList.append(s)
		for item in self.removeList:
			try:
				del self.dict[item]
			except:
				pass
		del self.removeList[:]
		print "Filtered Microsoft-IIS; list size: " + str(len(self.dict))
	
	def filterGSE(self):
		# Format:
		#for a,b in self.dict.items():
			#if b.count('-') == 4:
			#	self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered GSE; list size: " + str(len(self.dict))
	
	def filterNginx(self):
		# http://nginx.org/en/docs/http/ngx_http_core_module.html#etag
		print "Filtered nginx; list size: " + str(len(self.dict))
	
	def filterLiteSpeed(self):
		# Format: xxxx-xxxxxxxx-x
		prog = re.compile(r"^[a-fA-F_0-9]{4}(-)[a-fA-F_0-9]{8}(-)[0-9]+")
		for a,b in self.dict.items():
			if prog.search(b):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered LiteSpeed; list size: " + str(len(self.dict))
	
	def filterEmpty(self):
		for a,b in self.dict.items():
			if b == '""' or b == '0':
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered empty etags; list size: " + str(len(self.dict))
	
	def filterPatterns(self):
		for a,b in self.dict.items():
			if b.lower().startswith('w/'):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered weak etags; list size: " + str(len(self.dict))
		for a,b in self.dict.items():
			rank = a[:a.find(' ')]
			site = a[a.find(' ')+1:a.find('.txt')]
			self.checkFileTimeStamp(rank,site,b)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered starting with time stamp: " + str(len(self.dict))
		for a,b in self.dict.items():
			if not b.find('Accept-Encoding') or not b.find('TESTBED'):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered misc. phrases; list size: " + str(len(self.dict))
		pat1 = re.compile(r'^(")?(-)?[0-9]+(")?')
		pat2 = re.compile(r'^[a-fA-F_0-9]{4}[a-fA-F_0-9]{13}$')
		for a,b in self.dict.items():
			if b.endswith(':0"') or b.endswith('|') or len(b) < 5 or pat1.match(b) or pat2.match(b):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered misc. patterns; list size: " + str(len(self.dict))
		prog = re.compile(r'^(")?[a-fA-F_0-9]{32}(")?$')
		for a,b in self.dict.items():
			if prog.match(b):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered size 32 etags; list size: " + str(len(self.dict))
		prog = re.compile(r'^(")?(pv)[a-fA-F_0-9]{32}(")?$')
		for a,b in self.dict.items():
			if prog.match(b):
				self.removeList.append(a)
		for item in self.removeList:
			del self.dict[item]
		del self.removeList[:]
		print "Filtered size 34 etags; list size: " + str(len(self.dict))
	
	def checkFileTimeStamp(self,rank,site,etag):
		index = etag.find(':')
		if index in range(13,17) and not etag.lower().startswith('w/'):
			s = str(int(rank))+' '+site+'.txt'
			self.removeList.append(s)
		elif index in range(16,20) and etag.lower().startswith('w/'):
			s = str(int(rank))+' '+site+'.txt'
			self.removeList.append(s)
	
	def filterAll(self):
		self.filterFrequency(2)
	
		self.filterApache()
		self.filterMicrosoftIIS()
		self.filterGSE()
		self.filterNginx()
		self.filterLiteSpeed()
		
		self.filterEmpty()
		self.filterPatterns()
	
	def writeDatabase(self):
		temp = []
		self.db.cur.execute('select etag,set_cookie,rank,site,server from top50k')
		for row in self.db.cur:
			s = str(int(row[2]))+' '+str(row[3])+'.txt'
			if s in self.dict.keys():
				temp.append((row[0],row[1],row[2],row[3],row[4]))
		self.db.cur.execute('drop table if exists filtered;')
		self.db.cur.execute('''create table filtered (etag text, set_cookie text,
			rank real, site text, server text)''')
		for a,b,c,d,e in temp:
			self.db.cur.execute('insert into filtered values (?,?,?,?,?)',(a,b,c,d,e,))
		self.db.conn.commit()
		
	def test(self):
		self.db.cur.execute('select rank,site,etag from top50k where server like ?',('Microsoft-IIS%',))
		count = 0
		for row in self.db.cur:
			index = row[2].find(':')
			if index in range(13,17) and not row[2].lower().startswith('w/'):
				count += 1
			elif index in range(16,20) and row[2].lower().startswith('w/'):
				count += 1
			elif not row[2].find('ASP.NET'):
				count += 1
		print count
		
	
	def main(self):
		self.db.connect()
	
		self.fillList()
		self.filterAll()
		
		#print self.dict
		f = open(write_file,'w')
		for k,v in self.dict.items():
			f.write(str(k)+' '+str(v)+'\n')
		f.close()
		
		print "Separate test:"
		self.test()
		
		self.writeDatabase()
		self.db.disconnect()
		

Filter().main()