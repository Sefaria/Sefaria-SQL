#!/usr/bin/python2
import os
import json
from pprint import pprint
import re


mergedFiles = []
path = '../../../Sefaria-Export/'
compPath = path + 'json' #cant have extra / signs b/c this is used in a replace()
printFilesLength = 0

def main():
	
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

	f1 = open('fileList.txt','w')
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
	main()

