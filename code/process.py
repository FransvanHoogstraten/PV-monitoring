# Copyrighted 2013 F.M. van Hoogstraten - Ademia BV.


import os
import time
import zipfile
import MySQLdb as mdb 
import logging
import logger	#this is the logger.py file
from lxml import etree

#variables
LOG_FILENAME="/var/log/foobar.log"
host='localhost'
usrnm='root'
psswrd='foo'
db_name='bar'
home="/root/Code/"

#Logging
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

#Initialization
serienummer=0
timestamp='timestamp'




def unzip(source_filename, dest_dir):
	zf = zipfile.ZipFile(source_filename)
	for member in zf.infolist():
		# Path traversal defense copied from
		# http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
		words = member.filename.split('/')
		path = dest_dir
		for word in words[:-1]:
			drive, word = os.path.splitdrive(word)
			head, word = os.path.split(word)
			if word in (os.curdir, os.pardir, ''): continue
			path = os.path.join(path, word)
		zf.extract(member, path)



while True:
	time.sleep(60)
	try:
		#Connect to database
		connRAW = mdb.connect(host, usrnm, psswrd, db_name);
		cursor = connRAW.cursor(mdb.cursors.DictCursor)

		#Check for newly uploaded ZIP files 
		list = os.listdir("/home/") # returns list
		list.sort()
		

		for file in list:
			try:
				unzip("/home/"+file, home+"/unzipped/")
				os.rename("/home/"+file, home+"/processed/"+file)
				logger.messageInfo(file+" successfully unzipped and moved to PROCESSED folder") 
			except:
				logger.messageException("Exception during Unzip - File is probably still being transferred")
			
			#remove test message from WebBox
			try:
				os.remove(home+"/unzipped/Info.xml")		
			except:
				pass
			
			
			
		#Removing LOG files (device events), keeping MEAN files (actual data)
		list = os.listdir(home+"/unzipped") # returns list
		list.sort()
		 
		for file in list:
			
			if "Mean." in file and ".zip" in file:
				unzip(home+"/unzipped/"+file, home+"/XML/")
				os.remove(home+"/unzipped/"+file)
				logger.messageInfo(file+" successfully unzipped. XML created and moved to XML folder") 
				
			elif "Mean." in file and ".xml" in file:
				os.rename(home+"/unzipped/"+file, home+"/XML/"+file)	#This occasionally happens, that directly the XML is zipped
				logger.messageInfo(file+" was already an XML file ==> moved to XML folder") 
				
			else:
				os.remove(home+"/unzipped/"+file)
				logger.messageInfo(file+" successfully removed") 


		#Read XML and process
		list = os.listdir(home+"/XML") # returns list
		list.sort()

		for file in list:
			with open (home+"/XML/"+file, "r") as myfile:
				source=myfile.read()
				os.remove(home+"/XML/"+file)

			document = etree.fromstring(source)
			inserts = []

		#Extract Time
			for parent in document.findall('Info'):
				for element in parent.iterdescendants():
					if element.getchildren() == []:				#to eleminate 'parent' tags
						if element.tag in ['Created']:
							timestamp=element.text

		#Extract Variables					
			for parent in document.findall('MeanPublic'):
				
				for element in parent.iterdescendants():
					if element.getchildren() == []:				#to eleminate 'parent' tags
						
						if element.tag in ['Key']:
							variable = element.text.split(':')[2]
							
							#New Converter:
							if element.text.split(':')[1] != serienummer:
								serienummer = element.text.split(':')[1]
		
								logger.messageInfo(timestamp+" NEW CONVERTER DATA ==> Serienummer: "+serienummer) 
								dI=0
								E_Total=0
								Event_Cnt=0
								Fac=0
								Fehler=0
								h_On=0
								h_Total=0
								Iac_Ist=0
								Ipv=0
								Netz_Ein=0
								Pac=0
								RErd_Start=0
								Status=0
								Uac=0
								Upv_Ist=0
								Upv_Soll=0
							
							
						#Load variables:	
						if element.tag in ['Mean']:
					
							if variable=="dI":
								dI=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(dI)) 
							if variable=="E-Total":
								E_Total=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(E_Total)) 
							if variable=="Event-Cnt":
								Event_Cnt=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Event_Cnt)) 
							if variable=="Fac":
								Fac=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Fac)) 
							if variable=="Fehler":
								Fehler=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Fehler)) 
							if variable=="h-On":
								h_On=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(h_On)) 
							if variable=="h-Total":
								h_Total=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(h_Total)) 
							if variable=="Iac-Ist":
								Iac_Ist=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Iac_Ist)) 
							if variable=="Ipv":
								Ipv=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Ipv)) 
							if variable=="Netz-Ein":
								Netz_Ein=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Netz_Ein)) 
							if variable=="Pac":
								Pac=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Pac)) 
							if variable=="RErd-Start":
								RErd_Start=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(RErd_Start)) 
							if variable=="Status":
								Status=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Status)) 
							if variable=="Uac":
								Uac=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Uac)) 
							if variable=="Upv-Ist":
								Upv_Ist=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Upv_Ist)) 
							if variable=="Upv-Soll":
								Upv_Soll=element.text
#								logger.messageInfo(timestamp+' '+variable+' '+str(Upv_Soll)) 
								
							
							
						if variable in ['Upv-Soll'] and element.tag in ['TimeStamp']:
							try:
#								sql="INSERT INTO `data_RAW` (`timestamp`,`dI`, `E-Total`, `Event-Cnt`, `Fac`, `Fehler`, `h-On`, `h-Total`, `Iac-Ist`, `Ipv`, `Netz-Ein`, `Pac`, `RErd-Start`, `Serienummer`, `Status`, `Uac`, `Upv-Ist`, `Upv-Soll`) "+\
#								"VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (timestamp,dI, E_Total, Event_Cnt, Fac, Fehler, h_On, h_Total, Iac_Ist, Ipv, Netz_Ein, Pac, RErd_Start, serienummer, Status, Uac, Upv_Ist, Upv_Soll)
								sql="INSERT INTO `data_RAW` (`timestamp`, `E-Total`, `Pac`, `Serienummer`) "+\
								"VALUES ('%s','%s', '%s', '%s')" % (timestamp, E_Total, Pac, serienummer)


								cursor.execute((sql))
								insert_id=connRAW.insert_id()

								connRAW.commit()
								logger.messageInfo("Event "+str(insert_id)+" created in database")
							except:
								connRAW.rollback()
								logger.messageException("Exception during writing to database")
	except:
		logger.messageException("Exception during main loop")
		

		



	

