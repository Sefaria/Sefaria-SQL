import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Book extends SQLite{

	public static final String CREATE_BOOKS_TABLE = "CREATE TABLE " + TABLE_BOOKS + "(\r\n" +
			" _id INTEGER PRIMARY KEY,\r\n" + 
			KcommentsOn 	+ " INTEGER, --often this will be null, meaning that it doesn't comment on a specific book.\r\n" + 
			KsectionNames 	+ " TEXT, \r\n" + 
			Kcategories 	+ " TEXT,\r\n" + 
			KtextDepth 	+ " INTEGER,\r\n" + 
			KwherePage 	+ " INTEGER DEFAULT " + DEFAULT_WHERE_PAGE + ", -- default is show on level2 (so that if verse is 1, a chapter will be that page displayed).\r\n" + 
			Klengths 		+ " TEXT,\r\n" + 
			Ktitle			+ " TEXT,\r\n" + 
			KheTitle		+ " TEXT,\r\n" + 
			KversionTitle	+ " TEXT,\r\n" +  
			KdataVersion	+ " REAL,\r\n" + 
			Kversions 		+ " TEXT,\r\n" + 
			Klanguages 	+ " INTEGER,\r\n" + 
			"heSectionNames" 	+ " TEXT, \r\n" +
			"minTid INTEGER DEFAULT -1, " +
			"maxTid INTEGER DEFAULT -1, " + 
			"rootNode INTEGER, " + 
			"path TEXT, " +
			"enCollectiveTitle TEXT, " +
			"heCollectiveTitle TEXT, " +
			"commentsOnMultiple TEXT, " + // This value should be "[<bid, bid2...> ]"
			"	CONSTRAINT uniqueTitle UNIQUE " + "(" + Ktitle + "),\r\n" + 
			"	FOREIGN KEY (" + KcommentsOn + ") REFERENCES " + TABLE_BOOKS + "(_id)\r\n" + 
			"	FOREIGN KEY (rootNode) REFERENCES " + " Nodes " + "(_id)\r\n" + 
			")";


	static String[] str2strArray(String str) {
		if(str == null)
			return new String [] {};
		Pattern r = Pattern.compile("(\\[|\\]|\")+"); //matches all [ , ] & "
		str = r.matcher(str).replaceAll("");
		String[] strArray = str.split(",");
		return strArray;
	}

	static private final String [] enSectionNamesList = {
		"Chapter",
		"Perek",
		"Line",
		"Daf",
		"Paragraph",
		"Parsha",
		"Parasha",
		"Parashah",
		"Parshah",
		"Seif",
		"Se'if",
		"Siman",
		"Section",
		"Verse",
		"Sentence",
		"Sha'ar",
		"Gate",
		"Comment",
		"Phrase",
		"Mishna",
		"Chelek",
		"Helek",
		"Year",
		"Masechet",
		"Massechet",
		"Letter",
		"Halacha",
		"Seif Katan",
		"Volume",
		"Book",
		"Shar",
		"Seder",
		"Part",
		"Pasuk",
		"Sefer",
		"Teshuva",
		"Tosefta",
		"Halakhah",
		"Kovetz",
		"Path",
		"Midrash",
		"Mitzvah",
		"Tefillah",
		"Torah"

	};






	static private final String [] heSectionNamesList = {
		"\u05E4\u05E8\u05E7",
		"\u05E4\u05E8\u05E7",
		"\u05E9\u05D5\u05E8\u05D4",
		"\u05D3\u05E3",
		"\u05E4\u05E1\u05E7\u05D4",
		"\u05E4\u05E8\u05E9\u05D4",
		"\u05E4\u05E8\u05E9\u05D4",
		"\u05E4\u05E8\u05E9\u05D4",
		"\u05E4\u05E8\u05E9\u05D4", //פרשה
		"\u05E1\u05E2\u05D9\u05E3",
		"\u05E1\u05E2\u05D9\u05E3",
		"\u05E1\u05D9\u05DE\u05DF",
		"\u05D7\u05DC\u05E7",
		"\u05E4\u05E1\u05D5\u05E7",
		"\u05DE\u05E9\u05E4\u05D8",
		"\u05E9\u05E2\u05E8",
		"\u05E9\u05E2\u05E8",
		"\u05E4\u05D9\u05E8\u05D5\u05E9",
		"\u05D1\u05D9\u05D8\u05D5\u05D9",
		"\u05DE\u05E9\u05E0\u05D4",
		"\u05D7\u05DC\u05E7",
		"\u05D7\u05DC\u05E7",
		"\u05E9\u05E0\u05D4",
		"\u05DE\u05E1\u05DB\u05EA",
		"\u05DE\u05E1\u05DB\u05EA",
		"\u05D0\u05D5\u05EA",
		"\u05D4\u05DC\u05DB\u05D4",
		"\u05E1\u05E2\u05D9\u05E3 \u05E7\u05D8\u05DF",
		"\u05DB\u05E8\u05DA",
		"\u05E1\u05E4\u05E8",
		"\u05E9\u05E2\u05E8",
		"\u05E1\u05D3\u05E8",
		"\u05D7\u05DC\u05E7", //חלק
		"\u05E4\u05E1\u05D5\u05E7", //פסוק
		"\u05E1\u05E4\u05E8", //ספר
		"\u05EA\u05E9\u05D5\u05D1\u05D4", //תשובה
		"\u05EA\u05D5\u05E1\u05E4\u05EA\u05D0",//תוספתא
		"\u05D4\u05DC\u05DB\u05D4", //הלכה
		"\u05E7\u05D5\u05D1\u05E5",  //קובץ
		"\u05E0\u05EA\u05D9\u05D1\u05D4", //נתיבה
		"\u05DE\u05D3\u05E8\u05E9", //מדרש
		"\u05DE\u05E6\u05D5\u05D4", //מצוה
		"\u05EA\u05E4\u05D9\u05DC\u05D4", //תפילה
		"\u05EA\u05D5\u05E8\u05D4" //תורה
	};


	protected static int idCount = 0;
	
	public static void addBook(Connection c, JSONObject enJSON, JSONObject heJSON, boolean complexText, JSONObject schema) throws JSONException, SQLException {		
		int langSum = 0;
		JSONObject json;
		if(enJSON != null && heJSON != null){
			langSum = returnLangNums(enJSON.getString("language")) + returnLangNums(heJSON.getString("language"));
			json = heJSON;
		}else if(enJSON == null && heJSON != null){
			json = heJSON;
			langSum = returnLangNums(json.getString("language"));
		}else if(enJSON != null && heJSON == null){
			json = enJSON;
			langSum = returnLangNums(json.getString("language"));
		}else{
			System.err.print("BOTH JSONs are null! :(");
			return; 
		}
		/**
		 * this is to test if it's complex or not. If it is, it will throw an error
		 */
		if(!complexText)
			json.get("sectionNames").toString().replace("\"\"", "\"Section\"");
		
		String title = json.getString(Ktitle );
					
		
		int id = ++idCount;

		PreparedStatement stmt = c.prepareStatement("INSERT INTO Books ("
				+ "_id" + ", "
				+ KcommentsOn + ", " + KsectionNames + ","  + Kcategories + ", " + KtextDepth + ", " 
				+ KwherePage + "," + Klengths + "," + Ktitle + ", " + KheTitle + ", "
				+ KversionTitle + ", " + KdataVersion +", " + Kversions + ", " + Klanguages  + ", heSectionNames "+ "" +
						", path, enCollectiveTitle, heCollectiveTitle, commentsOnMultiple) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

		stmt.setInt(1, id);//_id

		try{
			ArrayList<Integer> commentsOnMultiple = new ArrayList<Integer>();
			String dependence = schema.getString("dependence");
			if(dependence.length() > 0){
				// dependence is currently Commentary or Targum. 
				// But in the future it could be more, so as long as it's something
				// we're going to include it as a CommentsOn book
				JSONArray baseTextTitles = schema.getJSONArray("base_text_titles");
				for(int i = 0; i < baseTextTitles.length(); i++){
					String commentedOnBook = baseTextTitles.getJSONObject(i).getString("en");
					if(booksInDB.containsKey(commentedOnBook)){
						int commentsOn = booksInDBbid.get(commentedOnBook);
						commentsOnMultiple.add(commentsOn);
						stmt.setInt(2, commentsOn); // KcommentsOn
					}
				}
			}
			stmt.setString(18, commentsOnMultiple.toString());
		}catch(JSONException e){}
		int textDepth = 0;

		if(!complexText){//only simple texts have section names
			String sectionNames = json.get("sectionNames").toString().replace("\"\"", "\"Section\"");
			String heSectionNames = sectionNames;
			if(enSectionNamesList.length != heSectionNamesList.length){
				System.err.println("section names list are different sizes");
				System.exit(-1);
			}
			for(int i = 0;i < enSectionNamesList.length; i++){//replace the en names with he ones
				heSectionNames = heSectionNames.replace("\"" + enSectionNamesList[i] + "\"", "\"" + heSectionNamesList[i] + "\"");
			}
			textDepth = str2strArray(sectionNames).length;
			if(str2strArray(heSectionNames).length != textDepth){
				System.err.println("section names convertion problem:" + heSectionNames);
				System.exit(-1);
			}

			stmt.setString(3,sectionNames); // KsectionNames
			stmt.setString(14, heSectionNames);
		}
		JSONArray categories = (JSONArray) json.get("categories"); 
		stmt.setString(4, categories.toString().replace("\"Liturgy\"", "\"Tefillah\"")); // Kcategories

		if(textDepth >= 4)
			System.out.println("4!!!");
		//System.out.println("\t" + textDepth);
		stmt.setInt(5,textDepth); // KtextDepth

		int wherePageNum = 0;
		if(textDepth == 1) 	
			wherePageNum = 1;
		else
			wherePageNum = DEFAULT_WHERE_PAGE;
		stmt.setInt(6, wherePageNum); // KwherePage
		//stmt.setString(7, json.getString(Klengths )); // Klengths

		stmt.setString(8,title); // Ktitle
		try{
			stmt.setString(9, json.getString(KheTitle )); // KheTitle
		}catch(JSONException e){
			stmt.setString(9, title); // KheTitle
		}
		//stmt.setString(10, json.getString(KdataVersion )); // KdataVersion
		//stmt.setString(11, json.getString(KversionTitle )); // KversionTitle
		//stmt.setString(12, json.getString(Kversions )); // Kversions
		stmt.setInt(13,langSum); // Klanguages
		//stmt.setString(14, heSectionNames) is in if statement above
		StringBuilder path = new StringBuilder();
		
		for(int i = 0; i < categories.length(); i++){
			path.append(categories.getString(i) + "/");
		}
		path.append(title);
		stmt.setString(15,path.toString());
		
		try{
			JSONObject collectiveTitle = schema.getJSONObject("collective_title");
			stmt.setString(16, collectiveTitle.getString("en"));//collectiveTitle
			stmt.setString(17, collectiveTitle.getString("he"));//collectiveTitle
		}catch(JSONException e){}
		

		stmt.executeUpdate();
		stmt.close();
		c.commit();
		booksInDBbid2Title.put(id,title);
		booksInDBbid.put(title,id);
		booksInDBtextDepth.put(title,textDepth);
		booksInDB.put(title, langSum); //make sure we have a good list of which books are in the db (for when we try to add a second language).
		booksIsComplex.put(id, complexText);
		return;

	}
	
	static public void convertCommWherePage(Connection c){
		String sql = "UPDATE Books set wherePage = ? WHERE categories LIKE ?  AND textDepth = ?" ;
		PreparedStatement stmt = null;
		try {
			stmt = c.prepareStatement(sql);
			stmt.setInt(1, 3);
			stmt.setString(2, "[\"Commentary\",%");
			stmt.setInt(3, 3);
			stmt.executeUpdate();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	static public void setTidMinMax(Connection c){
		String sql = "UPDATE Books SET minTid=(select MIN(_id) from Texts where bid = ?), maxTid=(select MAX(_id) from Texts where bid = ?) where _id = ?" ;
		PreparedStatement stmt = null;
		try {
			stmt = c.prepareStatement(sql);
			for(int i= 0; i<booksInDB.size() +1;i++){
				stmt.setInt(1, i);
				stmt.setInt(2, i);
				stmt.setInt(3, i);
				stmt.executeUpdate();
			}
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
