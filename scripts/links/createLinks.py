import csv;

def main():
	path = '../../../Sefaria-Export/'
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
			filew = links + str(numberOfLines/numberOfLinesPerFile) + '.csv'
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
	

if __name__ == "__main__":
	main()
