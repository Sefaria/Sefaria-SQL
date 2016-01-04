

mesachtas = ["Berakhot", "Shabbat", "Eruvin", "Pesachim", "Rosh Hashanah", "Yoma", "Sukkah", "Beitzah",  \
			"Taanit", "Megillah", \
			"Moed Katan", "Chagigah", "Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", \
			"Kiddushin", "Bava Kamma", "Bava Metzia", "Bava Batra", "Sanhedrin", "Makkot", "Shevuot", \
			"Avodah Zarah", "Horayot", "Zevachim", "Menachot", "Chullin", "Bekhorot", "Arakhin", "Temurah", \
			"Keritot", "Meilah", "Tamid", "Niddah"]
			
mesachtasYer = ["Berakhot", "Peah", "Demai", "Kilayim", "Shevi'it", "Terumot", "Ma'asrot",  \
			"Ma'aser Sheni", "Hallah", "Orlah", "Bikkurim", \
			"Shabbat", "Eiruvin", "Pesachim", "Rosh Hashanah", "Yoma", "Shekalim", "Sukkah", "Beitzah",  \
			"Ta'anit", "Megillah", 	"Moed Katan", "Chagigah", \
			"Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", "Kiddushin", \
			"Bava Kamma", "Bava Metsia", "Bava Batra", "Sanhedrin", "Makkot", "Shevuot", "Avodah Zarah", "Horayot", \
			"Niddah"]
			
			
randomList = ["Sefer HaChinukh", "Nusach HaSefardim, Edot HaMizrach", "Nusach Sefard (Minhag Ashkenazim)"]


file = open("dontDisplayNum.txt", 'w')

for title in mesachtas:
	file.write(title + "\n")

for title in mesachtasYer:
	file.write("Jerusalem Talmud " + title + "\n")

for title in randomList:
	file.write(title + "\n")