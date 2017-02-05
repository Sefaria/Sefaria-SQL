#!/usr/bin/python2
import csv
import os
import json
from pprint import pprint
import re
from shutil import copyfile
	

path = '../Sefaria-Export/'
compPath = path + 'json' #cant have extra / signs b/c this is used in a replace()
compPath.replace("//","/")
indexPath = path + "/table_of_contents.json"
path = compPath


def main():
	print("Creating Links")
	links()
	print("Creating File List")
	createFileList()


def links():
	path = '../Sefaria-Export/'
	in_file_number = 0

	filew = 'scripts/links/links{}.csv'.format(0)
	with open(filew, 'wb') as ofile:
		writer = csv.writer(ofile)

		while True:
			filename = '{}links/links{}.csv'.format(path, in_file_number)
			if not os.path.isfile(filename):
				break
			else:
				print(filename)

			numberOfLines = 0
			with open(filename, 'rb') as ifile:
				reader = csv.reader(ifile)
				firstRow = True

				for row in reader:
					if  firstRow:
						firstRow = False
					else:
						thisLine = []
						thisLine += [row[3]]
						thisLine += convert2Levels(row[0].replace(row[3] + " " ,"").split(':'), row[5], row[0])
						thisLine += [row[4]]
						thisLine += convert2Levels(row[1].replace(row[4] + " " ,"").split(':'), row[6], row[1])
						thisLine += [conncetionType(row[2])]
						writer.writerow(thisLine)
						numberOfLines += 1
			in_file_number += 1


def convert2Levels(item, category, title):
	zeroList = [];
	for i in range(len(item), 6):
		zeroList += ['0'];
	if category == "Talmud":
		try:
			item[0] = daf2Num(item[0])
		except RuntimeError as e:
			if not title.startswith('Introduction'):
				# if Intro it's therefore expected to not be a daf 
				print(str(e), title)
	return zeroList + item

def daf2Num(daf):
	if (daf[-1] != 'a' and daf[-1] != 'b'):
		raise RuntimeError(daf + " ERROR.... it's not a daf")
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
	

def createFileList():
	# -- deal wih index -- 
	with open(indexPath) as data_file:    
	    indexData = json.load(data_file)
	indexStr = json.dumps(indexData)
	names = ["testDBs/index.json", 'testDBs/sefaria_mobile_updating_index.json.jar']
	for name in names:
		smallJSON = open(name,'w')
		smallJSON.write(indexStr)
		smallJSON.close()
	parseIndex(indexData, "") #get the orderTitles list

	# -- deal with mergedFiles
	mergedFiles = []
	path_to_dict(path, mergedFiles) #get the mergedFiles list
	fileBuffer = []
	mergedFilesLength = len(mergedFiles)
	
	printFilesLength = 0
	for title in orderTitles:
		heTitle = "/" + title + "/Hebrew/merged.json"
		enTitle = "/" + title + "/English/merged.json"
		foundHe = findMatch(heTitle, fileBuffer, mergedFiles)
		foundEn = findMatch(enTitle, fileBuffer, mergedFiles)
		printFilesLength += foundHe + foundEn
		
		if((not foundHe) and (not foundEn)):
			print("In index but not in dataDump: " + title)

	fileBuffer = reorderFiles(fileBuffer)

	f1 = open('scripts/fileList/fileList.txt','w')
	for merged in fileBuffer:
		merged = replaceJPS(merged)
		f1.write(merged + '\n')

	f1.close()

	print("\nIn dataDump but not in index:\n")
	pprint(mergedFiles)
	print("printFilesLength: " + str(printFilesLength))
	print("mergedFilesLength: " + str(mergedFilesLength))


def reorderFiles(unordered):
	#note: this only works for the list of Commentaries that serperated from the rest of the list.
	# so it doesn't include Other/Commentary2
	specials = [
	'^Tanakh/Torah/','^Tanakh/Prophets/', '^Tanakh/Writings/',
	'^Mishnah/Seder ', '^Talmud/Bavli/Seder ', '^Talmud/Yerushalmi/Seder ',

	'^Commentary/Tanakh/Rashi', '^Tanakh/Targum/Onkelos', '^Commentary/Tanakh/Ibn Ezra', '^Commentary/Tanakh/Ramban', '^Commentary/Tanakh/Sforno', '^Commentary/Tanakh/Rashbam',
	'^Commentary/Tanakh', '^Tanakh/Commentary/', '^Other/Commentary2/Tanakh/', '^Tanakh/Targum/', # make sure that the rest of the Tanakh commentaries come b/f any other category commenary
	
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


def path_to_dict(path, mergedFiles):
	d = {'name': os.path.basename(path)}
	if os.path.isdir(path):
			d['type'] = "directory"
			d['children'] = [path_to_dict(os.path.join(path,x), mergedFiles) for x in os.listdir(path)]
	else:
		d['type'] = "file"
		if os.path.basename(path) == "merged.json":
			mergedFiles.append(path.replace(compPath + "/", ""))
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
	

def findMatch(newTitle, fileBuffer, mergedFiles):
	for merged in mergedFiles:
		if(merged.find(newTitle) > 0):
			fileBuffer.append(merged)
			mergedFiles.remove(merged)
			return True
	return False


def replaceJPS(fileName):
	temp =  re.sub('^Tanakh/',"../../JPS/Tanakh/", fileName) #be careful with Tanach vs. Tanakh (on both sides of the replace)
	#temp = temp.replace("merged.json","JPS 1985 English Translation.json")
	if('/English/merged.json' in temp and os.path.exists(compPath + '/' + temp)):
		return temp
	else:
		return fileName
	

if __name__ == "__main__":
	main()