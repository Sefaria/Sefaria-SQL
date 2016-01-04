import csv

def Num2Daf(dafNum):
	daf = str((dafNum +1)/2)
	if(dafNum %2 == 1):
		daf += 'a'
	else:
		daf += 'b'
	return daf


linesCount = -1 # must start at -1 global
ofile = "";
writer = "";
def addRowToFile(row):
	linesPerFile = 100000000
	num = 2
	headerFileNum = 0
	global linesCount
	global writer;
	global ofile;
	linesCount +=1
	if linesCount % linesPerFile == 0:
		try:
			ofile.close()
		except:
			dummmy = 'a';
		filew = 'headers/' + str(num) + 'header' + str(linesCount / linesPerFile) +  '.csv'
		ofile = open(filew, 'wb');
		writer = csv.writer(ofile);
	writer.writerow(row);

def makeHeadersForMesachta(mesachta, dafCount, startDafNum):
	# startDafNum= 3; #for bavli and 1 for yerushalmi.
	for dafNum in range(startDafNum, dafCount +1):
		row = [mesachta]
		row += [0] # level1
		row += [dafNum]
		row += [0,0,0,0] #levels 3-6
		row += [Num2Daf(dafNum)] #the daf ex: 34a
		row += [0,1]  #displayNum = false, displayLevelType (daf) = true
		addRowToFile(row)

mesachtas = ["Berakhot", "Shabbat", "Eruvin", "Pesachim", "Rosh Hashanah", "Yoma", "Sukkah", "Beitzah",  \
			"Taanit", "Megillah", \
			"Moed Katan", "Chagigah", "Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", \
			"Kiddushin", "Bava Kamma", "Bava Metzia", "Bava Batra", "Sanhedrin", "Makkot", "Shevuot", \
			"Avodah Zarah", "Horayot", "Zevachim", "Menachot", "Chullin", "Bekhorot", "Arakhin", "Temurah", \
			"Keritot", "Meilah", "Tamid", "Niddah"]
fakeDafCounts = [127, 314, 209, 242, 69, 175, 112, 80, \
				61, 63, \
				57, 53, 244, 224, 182, 132, 98, 180, \
				164, 238,237, 352, 226, 48, 98, \
				152, 27, 240, 219, 283, 121, 67, 67, \
				56, 43, 66, 145]
				
mesachtasYer = ["Berakhot", "Peah", "Demai", "Kilayim", "Shevi'it", "Terumot", "Ma'asrot",  \
			"Ma'aser Sheni", "Hallah", "Orlah", "Bikkurim", \
			"Shabbat", "Eiruvin", "Pesachim", "Rosh Hashanah", "Yoma", "Shekalim", "Sukkah", "Beitzah",  \
			"Ta'anit", "Megillah", 	"Moed Katan", "Chagigah", \
			"Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", "Kiddushin", \
			"Bava Kamma", "Bava Metsia", "Bava Batra", "Sanhedrin", "Makkot", "Shevuot", "Avodah Zarah", "Horayot", \
			"Niddah"]
fakeDafCountsYer = [135, 74, 67, 88, 61, 117, 52,\
				66, 56, 40, 25, \
				184, 130, 142, 43, 84, 66, 52, 44, \
				52, 67,  38, 44, \
				169, 143, 79, 94, 93, 108, 96, \
				88, 73, 67, 114, 18,88, 74, 38, \
				25]
				
for i in range(len(mesachtas)):
	makeHeadersForMesachta(mesachtas[i],fakeDafCounts[i],3);
	makeHeadersForMesachta("Rashi on " + mesachtas[i],fakeDafCounts[i],3);
	makeHeadersForMesachta("Tosafot on " +  mesachtas[i],fakeDafCounts[i],3);
	
for i in range(len(mesachtasYer)):
	makeHeadersForMesachta("Jerusalem Talmud " + mesachtasYer[i],fakeDafCountsYer[i],1);
	
	
	
	