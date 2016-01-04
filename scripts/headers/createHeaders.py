# coding=utf-8
import csv
import re

heChars=['א','ב','ג','ד','ה','ו','ז','ח','ט',"י",'כ','ל','מ','נ','ס','ע','פ','צ','ק','ר','ש','ת'];

def Num2Daf(dafNum):
	daf = str((dafNum +1)/2)
	if(dafNum %2 == 1):
		daf += 'a'
	else:
		daf += 'b'
	return daf
#eng to heb daf num: eg 34a -> lamed daled .
def DafNumTranslator(engDafNum):
	num = int(re.search('\d+',engDafNum).group())
	amud = re.search('[a-b]$',engDafNum).group()
	if amud == 'a':
		hebAmud = '.'
	elif amud == 'b':
		hebAmud = ':'
	return int2heb(num) + hebAmud

def int2heb(num):
	origNum = num;
	heb = "";
	place = 0;
	while num >= 1:
		digit = num%10; 
		num /= 10;
		baseHebChar = 0; #this is the position of a char in hebChars
		hebChar = "";
		if digit == 0:
			hebChar = "\0"; #no char when exactly multiple of ten
		else: 
			if place == 0:
				baseHebChar = 0; #alef
				hebChar = heChars[(baseHebChar + digit-1)];
				heb = hebChar + heb;
			elif place == 1:
				baseHebChar = 9; #yud
				hebChar = heChars[(baseHebChar + digit-1)];
				heb = hebChar + heb;
			elif place >= 2:
				baseHebChar = 18; #kuf
				if digit == 9: #can't be greater than tuf
					hChar1 = heChars[(baseHebChar + digit-9)];
					hChar2 = heChars[(baseHebChar + 3)]; #tuf, need two of these
					heb = "" + hChar2 + hChar2 + hChar1 + heb;
				elif digit > 4:
					hChar1 = heChars[(baseHebChar + digit-5)];
					hChar2 = heChars[(baseHebChar + 3)]; #tuf
					heb = "" + hChar2 + hChar1 + heb;
				else:
					hChar1 = heChars[(baseHebChar + digit-1)];
					heb = "" + hChar1 + heb;
		place+=1;	
	#now search for 15 & 16 to replace
	ka = "י" + "ה"; #carefull...don't join these strings
	ku = "י" + "ו";
	heb = heb.replace(ka,"טו");
	heb = heb.replace(ku,"טז");

	#Log.d("gem",origNum + " = " + heb);
	return heb;


linesCount = -1 # must start at -1 global
ofile = "";
writer = "";
def addRowToFile(row):
	linesPerFile = 100000000
	headerFileNum = 0
	global linesCount
	global writer;
	global ofile;
	linesCount +=1
	if linesCount % linesPerFile == 0:
		try:
			ofile.close()
		except:
			pass
		filew = 'headers/headers' + str(linesCount / linesPerFile) +  '.csv'
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
		row += [DafNumTranslator(Num2Daf(dafNum))] # heb daf
		row += [0,1]  #displayNum = false, displayLevelType (daf) = true
		row += [Num2Daf(dafNum)] #the daf ex: 34a
		addRowToFile(row)

		
def addHeaders(bookName, levelNum, listOfEnHeaders, listOfHeHeaders, displayNum, displayLevelType):
#levelNum is the "level" you want to add the headers. 1 indexed 
	for i in range(len(listOfHeHeaders)):
		row = [bookName]
		for j in range(6):
			if j + 1 == levelNum:
				row += [i+1]
			else: 
				row += [0]
		#row += [0] # level1
		#row += [i +1]
		#row += [0,0,0,0] #levels 3-6
		row += [listOfHeHeaders[i]] # heb header
		row += [displayNum, displayLevelType]  #displayNum = false, displayLevelType (daf) = true
		row += [listOfEnHeaders[i]] #the en header
		addRowToFile(row)

mesachtas = ["Berakhot", "Shabbat", "Eruvin", "Pesachim", "Rosh Hashanah", "Yoma", "Sukkah", "Beitzah",  \
			"Taanit", "Megillah", \
			"Moed Katan", "Chagigah", "Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", \
			"Kiddushin", "Bava Kamma", "Bava Metzia", "Bava Batra", "Sanhedrin", "Makkot", "Shevuot", \
			"Avodah Zarah", "Horayot", "Zevachim", "Menachot", "Chullin", "Bekhorot", "Arakhin", "Temurah", \
			"Keritot", "Meilah", "Tamid", "Niddah"]
mesachtasHe = ["ברכות", "שבת", "עירובין", "פסחים", "ראש השנה", "יומא", "סוכה", "ביצה",  \
			"תענית", "מגילה", \
			"מעד קטן", "חגיגה", "יבמות", "כתובות", "נדרים", "נזיר", "סוטה", "גיטין", \
			"קידושין", "בבא קמה", "בבא מציעה", "בבא בתרא", "סנהדרין", "מכות", "שבועות", \
			"עבודה זרה", "הוריות", "זבחים", "מנחות", "חולין", "בכורות", "ערכין", "תמורה", \
			"כריתות", "מעילה", "תמיד", "נדה"]
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

mesachtasEinEn = ["Berakhot","Shabbat","Eruvin","Pesachim","Yoma","Sukkah","Beitzah","Rosh Hashanah","Megillah","Taanit", \
                "Moed Katan","Chagigah","Yevamot","Ketubot","Nedarim","Nazir","Gittin","Sotah","Kiddushin","Bava Kamma", \
                "Bava Metzia","Bava Batra","Sanhedrin","Makkot","Shevuot","Eduyot","Avodah Zarah","Horayot","Zevachim", \
                "Menachot","Chullin","Bekhorot","Arakhin","Temurah","Keritot","Meilah","Tamid","Kinnim","Kelim","Negaim", \
                "Niddah","Yadayim","Oktzin","Middot"]

mesachtasEinHe = ["ברכות","שבת","עירובין","פסחים","יומא","סוכה","ביצה","ראש השנה","מגילה","תענית", \
                "מועד קטן","חגיגה","יבמות","כתובות","נדרים","נזיר","גיטין","סוטה","קידושין","בבא קמה", \
                "בבא מציעה","בבא בתרא","סנהדרין","מכות","שבועות","עדיות","עבודה זרה","הוריות","זבחים", \
                "מנחות","חולין","בכורות","ערכין","תמורה","כריתות","מעילה","תמיד","קינים","כלים","נגעים", \
                "נדה","ידים","עוקצים","מידות"]
				
mesachtasEinGlickEn = ["Berakhot","Shabbat","Eruvin","Pesachim","Yoma","Sukkah","Beitzah","Rosh Hashanah","Taanit","Megillah", \
                "Moed Katan","Chagigah","Yevamot","Ketubot","Nedarim","Nazir","Gittin","Sotah","Kiddushin","Bava Kamma", \
                "Bava Metzia","Bava Batra","Sanhedrin","Makkot","Shevuot","Eduyot","Avodah Zarah","Horayot","Zevachim", \
                "Menachot","Chullin"]

mesachtasEinGlickHe = ["ברכות","שבת","עירובין","פסחים","יומא","סוכה","ביצה","ראש השנה","תענית","מגילה", \
                "מועד קטן","חגיגה","יבמות","כתובות","נדרים","נזיר","גיטין","סוטה","קידושין","בבא קמה", \
                "בבא מציעה","בבא בתרא","סנהדרין","מכות","שבועות","עדיות","עבודה זרה","הוריות","זבחים", \
                "מנחות","חולין"]

parashasEn = ["Bereshit","Noach","Lech Lecha","Vayera","Chayei Sara","Toldot","Vayetzei","Vayishlach","Vayeshev","Miketz","Vayigash","Vayechi", \
		"Shemot","Vaera","Bo","Beshalach","Yitro","Mishpatim","Terumah","Tetzaveh","Ki Tisa","Vayakhel","Pekudei", \
		"Vayikra","Tzav","Shmini","Tazria","Metzora","Achrei Mot","Kedoshim","Emor","Behar","Bechukotai", \
		"Bamidbar","Nasso","Beha'alotcha","Sh'lach","Korach","Chukat","Balak","Pinchas","Matot","Masei", \
		"Devarim","Vaetchanan","Eikev","Re'eh","Shoftim","Ki Teitzei","Ki Tavo","Nitzavim","Vayeilech","Ha'azinu","V'zot Habracha"]

parashasHe = ["בראשית","נח","לך לך","וירא","חיי שרה","תולדות","ויצא","וישלח","וישב","מקץ","ויגש","ויחי", \
		"שמות","וארא","בא","בשלח","יתרו","משפטים","תרומה","תצוה","כי תשא","ויקהל","פקודי", \
		"ויקרא","צו","שמיני","תזריע","מצורע","אחרי מות","קדושים","אמר","בהר","בחוקתי", \
		"במדבר","נשא","בהעלותך","שלח","קרח","חקת","בלק","פנחס","מטות","מעסי", \
		"דברים","ואתחנן","עקב","ראה","Shoftim","Ki Teitzei","Ki Tavo","Nitzavim","Vayeilech","Ha'azinu","V'zot Habracha"]

fakeDafCountsYer = [135, 74, 67, 88, 61, 117, 52,\
				66, 56, 40, 25, \
				184, 130, 142, 43, 84, 66, 52, 44, \
				52, 67,  38, 44, \
				169, 143, 79, 94, 93, 108, 96, \
				88, 73, 67, 114, 18,88, 74, 38, \
				25]
				
hagadahSectionsEn = ["Kadesh", "U'Rechatz", "Karpas", "Yachatz", "Maggid", "Rachzah", "Motzi Matzah", "Marror", "Korech", "Shachan Orech", "Tzafun", "Barech", "Hallel", "Nerztah"]
hagadahSectionsHe = ["קדש", "ורחץ", "כרפס", "יחץ", "מגיד", "רחצה", "מוציא מצה", "מרור", "כורך", "שלחן עורך", "צפון", "ברך", "הלל", "נרצה"]

"""	
	
for i in range(len(mesachtas)):
	makeHeadersForMesachta(mesachtas[i],fakeDafCounts[i],3);
	makeHeadersForMesachta("Rashi on " + mesachtas[i],fakeDafCounts[i],3);
	makeHeadersForMesachta("Tosafot on " +  mesachtas[i],fakeDafCounts[i],3);
	
for i in range(len(mesachtasYer)):
	makeHeadersForMesachta("Jerusalem Talmud " + mesachtasYer[i],fakeDafCountsYer[i],1);
	
"""
	
def main():
	addHeaders("Ein Yaakov",3, mesachtasEinEn, mesachtasEinHe, 0, 1)
	addHeaders("Ein Yaakov (Glick Edition)",3, mesachtasEinGlickEn, mesachtasEinGlickHe, 0, 1)
	addHeaders("Pesach Haggadah",2, hagadahSectionsEn, hagadahSectionsHe, 0, 0)

	addHeaders("Ephod Bad" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Kimcha Davshuna" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Kos Shel Eliyahu" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Maarechet Heidenheim" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Maaseh Nissim" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Marbeh Lisaper" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Naftali Seva Ratzon" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
	addHeaders("Yismach Yisrael" + " on Pesach Haggadah",3, hagadahSectionsEn, hagadahSectionsHe, 0, 0)
		
if __name__ == "__main__":
	main()