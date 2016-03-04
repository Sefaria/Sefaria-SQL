#!/usr/bin/python2
import csv;
import os
import json
from pprint import pprint
import re



def links():
	path = '../Sefaria-Export/'
	fileName = path + 'links/links.csv';

	ifile = open(fileName, 'rb');
	reader = csv.reader(ifile)

	firstRow = True;
	numberOfLines = 0
	numberOfLinesPerFile = 100000000 #7000
	for row in reader:
		if numberOfLines % numberOfLinesPerFile == 0:
			try:
				ofile.close()
			except:
				pass
			filew = 'scripts/links/links' + str(numberOfLines/numberOfLinesPerFile) + '.csv'
			ofile = open(filew, 'wb');
			writer = csv.writer(ofile);
			
		if  firstRow:
			firstRow = False;
			continue;
		thisLine = []
		thisLine += [row[3]]
		thisLine += convert2Levels(row[0].replace(row[3] + " " ,"").split(':'), row[5]);
		thisLine += [row[4]]
		thisLine += convert2Levels(row[1].replace(row[4] + " " ,"").split(':'), row[6]);
		thisLine += [conncetionType(row[2])]
		writer.writerow(thisLine);
		numberOfLines += 1;


	ifile.close()


def convert2Levels(item, category):
	zeroList = [];
	for i in range(len(item), 6):
		zeroList += ['0'];
	if category == "Talmud":
		item[0] = daf2Num(item[0])
	return zeroList + item

def daf2Num(daf):
	if (daf[-1] != 'a' and daf[-1] != 'b'):
		print(daf[-1]);
		print(daf + " ERROR.... it's not a daf")
		return daf
	if "-" in daf[0:-1]:
		return daf
	value = int(daf[0:-1])*2 - 1
	if(daf[-1] == 'b'):
		value += 1;
	return value;

	
def conncetionType(connString):
	return connString[0:3].lower()
	#map = {'quotation': 1, 'commentary':2, 'reference':3,'related':4,'midrash':5,  'allusion':6, 'mesorat hashas': 7, 'summary':8, 'Law':9,'ein mishpat':10,'Liturgy':11,'explication':12, 'targum':13,'Ellucidation':14};
	#if connString not in map.keys():
	#	return 0;
	#return map[connString]
	


###########createFileList



mergedFiles = []
path = '../Sefaria-Export/'
compPath = path + 'json' #cant have extra / signs b/c this is used in a replace()
printFilesLength = 0

def createFileList():
	
	indexPath = path + "/table_of_contents.json"

	json.dumps(path_to_dict(path)) #get the mergedFiles list


	with open(indexPath) as data_file:    
	    data = json.load(data_file)
		
	parseIndex(data, "") #get the orderTitles list


	fileBuffer = []
	mergedFilesLength = len(mergedFiles)
	
	
	for title in orderTitles:
		heTitle = "/" + title + "/Hebrew/merged.json"
		enTitle = "/" + title + "/English/merged.json"
		foundHe = findMatch(heTitle, fileBuffer)
		foundEn = findMatch(enTitle, fileBuffer)
		
		if((not foundHe) and (not foundEn)):
			print("In index but not in dataDump: " + title)

	fileBuffer = reorderFiles(fileBuffer)

	f1 = open('scripts/fileList/fileList.txt','w')
	for merged in fileBuffer:
		f1.write(merged + '\n')

	f1.close()

	print("\nIn dataDump but not in index:\n")
	pprint(mergedFiles)
	print("printFilesLength: " + str(printFilesLength))
	print("mergedFilesLength: " + str(mergedFilesLength))


"""
def makeFolderJSON(path):
	# - create a json of the folder structure (not used for anything)
	f = open('folder.json', 'w')
	fp = open('folderPreaty.json', 'w')
	fp.write(json.dumps(path_to_dict(path), sort_keys=True, indent=4, separators=(',', ': '))); 
	f.write(json.dumps(path_to_dict(path))); 
"""

def reorderFiles(unordered):


	#note: this only works for the list of Commentaries that serperated from the rest of the list.
	# so it doesn't include Other/Commentary2
	
	specials = [
	'^Tanach/Torah/','^Tanach/Prophets/', '^Tanach/Writings/',
	'^Mishnah/Seder ', '^Talmud/Bavli/Seder ', '^Talmud/Yerushalmi/Seder ',

	'^Commentary/Tanach/Rashi', '^Tanach/Targum/Onkelos', '^Commentary/Tanach/Ibn Ezra', '^Commentary/Tanach/Ramban', '^Commentary/Tanach/Sforno', '^Commentary/Tanach/Rashbam',
	'^Commentary/Tanach', '^Tanach/Commentary/', '^Other/Commentary2/Tanach/', '^Tanach/Targum/', # make sure that the rest of the tanach commentaries come b/f any other category commenary
	

	'^Commentary/Mishnah/Bartenura', '^Commentary/Mishnah/Ikar Tosafot Yom Tov', '/Mishnah/Tosafot Yom Tov', '^Commentary/Mishnah/', '^Other/Commentary2/Mishnah/', # make sure that the rest of the mishna commentaries come b/f any other category commenary
	


	'^Commentary/Talmud/Rashi', '^Commentary/Talmud/Tosafot', '/Talmud/Rashba', '^Talmud/Rif/',
	'^Commentary/Talmud/', '^Other/Commentary2/Talmud/',

	'^Tosefta/Seder '

	]


	ordered1 = []
	ordered2 = unordered

	ordered2New = []


	for special in specials:
		for item in ordered2:
			if(re.search(special,item) != None):
				ordered1.append(item)
			else:
				ordered2New.append(item)

		ordered2 = ordered2New
		ordered2New = []


	return ordered1 + ordered2


def path_to_dict(path):
	#print(path, os.path.basename(path))
	d = {'name': os.path.basename(path)}
	if os.path.isdir(path):
			d['type'] = "directory"
			d['children'] = [path_to_dict(os.path.join(path,x)) for x in os.listdir(path)]
	else:
		d['type'] = "file"
		if os.path.basename(path) == "merged.json":
			#global listString
			#listString += path.replace(compPath + "/", "") + "\n"
			#global listD
			global it
			global mergedFiles
			#listD[str(it)] = path.replace(compPath + "/", "")
			mergedFiles.append(path.replace(compPath + "/", ""))
			#it +=1;
	return d
	




totalNum = 0
orderTitles = []
def parseIndex(data, catStr):
	global totalNum
	try:
		
		for j in range(len(data)):
			category = data[j]
			#print('cat: ' + category['category']  + str(j))
			try:
				item = category['contents']
				thisCat = catStr 
				cat = category['category']
				if(len(cat) > 0):
					thisCat = thisCat + "/" + cat
				for i in range(len(item)):
					#print(i)
					#pprint(item[i])
					subitem  = item[i]
					try:
						title = subitem['title']
						totalNum = totalNum +1
						#print(str(totalNum) + ". " + title + "\t" + thisCat)
						global orderTitles
						orderTitles.append(title)
						#print(thisCat +"/"+ title + "/Hebrew/merged.json")
						#print(thisCat +"/"+ title + "/English/merged.json")
					except:
						pass
				parseIndex(item, thisCat)
			except:
				pass
		
	except: #if this didn't work it means you've opened all the categories within this data
		pass
	

def findMatch(newTitle, fileBuffer):
	global mergedFiles
	for merged in mergedFiles:
		if(merged.find(newTitle) > 0):
			fileBuffer.append(merged)
			mergedFiles.remove(merged)
			global printFilesLength
			printFilesLength += 1
			return True
	return False














if __name__ == "__main__":
	print("Creating Links")
	links()
	print("Creating File List")
	createFileList()
