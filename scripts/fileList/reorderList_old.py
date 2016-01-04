import json
from pprint import pprint

totalNum  = 0
def main():
	path = '../../../Sefaria-Export'
	indexPath = path + "/table_of_contents.json"

	with open(indexPath) as data_file:    
	    data = json.load(data_file)

	parseIndex(data, "")


def parseIndex(data, catStr):
	global totalNum;
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
						print(str(totalNum) + ". " + title + "\t" + thisCat)
						#print(thisCat +"/"+ title + "/Hebrew/merged.json")
						#print(thisCat +"/"+ title + "/English/merged.json")
					except:
						pass
				parseIndex(item, thisCat)
			except:
				pass
		
	except: #if this didn't work it means you've opened all the categories within this data
		pass
	
if __name__ == "__main__":
	main()

