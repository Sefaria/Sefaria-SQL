#!/usr/bin/python2
import os
import json
from pprint import pprint



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


	commentBuffer = []
	mergedFilesLength = len(mergedFiles)
	
	f1 = open('fileList.txt','w')
	useBuffer = True
	for title in orderTitles:
		heTitle = "/" + title + "/Hebrew/merged.json"
		enTitle = "/" + title + "/English/merged.json"
		foundHe = findMatch(heTitle,f1, commentBuffer, useBuffer)
		foundEn = findMatch(enTitle,f1, commentBuffer, useBuffer)
		if(foundHe and title == 'Jerusalem Talmud Niddah'):
			useBuffer = False

			commentBuffer = reorderCommentaries(commentBuffer)

			for merged in commentBuffer:
				f1.write(merged + '\n')
		if((not foundHe) and (not foundEn)):
			print("In index but not in dataDump: " + title)
			
	f1.close()

	if(useBuffer):
		print("DIDN'T FIND NIDAH YERUSHALMI!!!!!!!!!!!")
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

def reorderCommentaries(unordered):


	#note: this only works for the list of Commentaries that serperated from the rest of the list.
	# so it doesn't include Other/Commentary2
	
	specials = [
	'Commentary/Tanach/Rashi', 'Tanach/Targum/Onkelos', 'Commentary/Tanach/Ibn Ezra', 'Commentary/Tanach/Ramban', 'Commentary/Tanach/Sforno', 'Commentary/Tanach/Rashbam',
	'Commentary/Tanach', # make sure that the rest of the tanach commentaries come b/f any other category commenary
	'Mishnah/Bartenura', 'Ikar Tosafot Yom Tov', '/Mishnah/Tosafot Yom Tov', 'Commentary/Mishnah/', # make sure that the rest of the mishna commentaries come b/f any other category commenary
	'Commentary/Talmud/Rashi', 'Commentary/Talmud/Tosafot', '/Talmud/Rashba', 'Talmud/Rif/',
	'Commentary/Talmud/']


	ordered1 = []
	ordered2 = unordered

	ordered2New = []


	for special in specials:
		for item in ordered2:
			if(special in item):
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
	

def findMatch(newTitle, f1, commentBuffer, useBuffer):
	global mergedFiles
	for merged in mergedFiles:
		if(merged.find(newTitle) > 0):
			if(useBuffer and merged.find('Commentary') == 0):
				commentBuffer.append(merged)
			else:
				f1.write(merged + '\n')
			#print(merged)
			mergedFiles.remove(merged)
			global printFilesLength
			printFilesLength += 1
			return True
	return False




if __name__ == "__main__":
	main()

