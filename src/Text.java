import java.awt.print.PrinterAbortException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.Stack;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Text extends SQLite{


	static String CREATE_COMPRESS_TEXTS_TABLE = "CREATE TABLE " + TABLE_TEXTS + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER NOT NULL,\r\n" + 
			"	enTextCompress BLOB,\r\n" + 
			"	heTextCompress BLOB,\r\n" + 
			"	level1 INTEGER DEFAULT 0,\r\n" + 
			"	level2 INTEGER DEFAULT 0,\r\n" + 
			"	level3 INTEGER DEFAULT 0,\r\n" + 
			"	level4 INTEGER DEFAULT 0,\r\n" + 

			"	flags  INTEGER DEFAULT 0, \r\n" +
			"	hasLink BOOLEAN DEFAULT 0,  \r\n" + 
			"	parentNode INTEGER DEFAULT 0,  \r\n" +  

			"	FOREIGN KEY (bid) \r\n" + 
			"		REFERENCES Books (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	FOREIGN KEY (parentNode) \r\n" + 
			"		REFERENCES Nodes (_id)\r\n" + 
			"		ON DELETE CASCADE\r\n" + 
			"	, CONSTRAINT TextsUnique UNIQUE (bid, level1, level2, level3, level4,parentNode)\r\n" +
			//"	, CONSTRAINT TextsUnique UNIQUE (bid, parentNode, level4, level3, level2, level1)\r\n" +

				");"
				//+ "CREATE INDEX levels ON " + TABLE_TEXTS + " (bid);"
				;

	static String CREATE_TEXTS_TABLE = 
			//"CREATE VIRTUAL TABLE " + TABLE_TEXTS + " USING fts3 " + "(\r\n" + 
			"CREATE TABLE " + TABLE_TEXTS + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER NOT NULL,\r\n" + 
			"	enText TEXT,\r\n" + 
			"	heText TEXT,\r\n" + 
			"	level1 INTEGER DEFAULT 0,\r\n" + 
			"	level2 INTEGER DEFAULT 0,\r\n" + 
			"	level3 INTEGER DEFAULT 0,\r\n" + 
			"	level4 INTEGER DEFAULT 0,\r\n" + 
			"	level5 INTEGER DEFAULT 0,\r\n" + 
			"	level6 INTEGER DEFAULT 0, \r\n" + 
			"	displayNumber BOOLEAN DEFAULT 1, \r\n" +
			"	hasLink BOOLEAN DEFAULT 0,  \r\n" + 
			"	parentNode INTEGER DEFAULT 0,  \r\n" + 

			//"	hid INTEGER,\r\n" + 
			"	FOREIGN KEY (bid) \r\n" + 
			"		REFERENCES Books (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	FOREIGN KEY (parentNode) \r\n" + 
			"		REFERENCES Nodes (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			//"	FOREIGN KEY (" + Khid + ") \r\n" + 
			//"		REFERENCES " + Header.TABLE_HEADERS +"(" + Khid + ")\r\n" + 
			//"		ON DELETE CASCADE,\r\n" + 
			"	CONSTRAINT TextsUnique UNIQUE (bid, level1, level2, level3, level4, level5, level6,parentNode)\r\n" + 
			//	"	PRIMARY KEY (bid, level1, level2, level3, level4, level5, level6)\r\n" + 

				")";

	static void displayNum(Connection c){
		displayNumCat(c,0, "[\"Talmud\",%");
		displayNumCat(c,0, "[\"Liturgy\"%");
		displayNumCat(c,0, "[\"Tefillah\"%");
		displayNumCat(c,1, "[\"Commentary\",%");
		displayNumCat(c,0, "[\"Commentary\",%",true);
		displayNumCat(c,1, "[\"Mishnah\",%");
		displayNumCat(c,1, "[\"Tanach\",%");
		displayNumCat(c,1, "[\"Tosefta\",%");
		displayNumCat(c,1, "[\"Halakhah\",\"Mishneh Torah\",%");
		displayNumCat(c,1, "[\"Talmud\",\"Yerushalmi\",%");




	}

	private static void textDontDisplayNum(Connection c, String title){
		//Assuming that the whole book has the same rules (at least for this input method)
		String sql = "UPDATE Texts set displayNumber = 0 WHERE bid in (SELECT B._id FROM Books B WHERE B.title = ?);" ;
		PreparedStatement stmt = null;
		try {
			stmt = c.prepareStatement(sql);
			stmt.setString(1, title);
			stmt.executeUpdate();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void displayNumCat(Connection c, int displayNumber, String likeCategories){
		displayNumCat(c, displayNumber, likeCategories, false);
	}
	static void displayNumCat(Connection c, int displayNumber, String likeCategories, boolean commentSection){
		System.out.println("displayNumCat:" + likeCategories);
		String sql;
		if(!commentSection)
			sql = "UPDATE Texts set displayNumber = ? WHERE bid in (SELECT B._id FROM Books B WHERE B.categories LIKE ? )" ;
		else
			sql = "UPDATE Texts set displayNumber = ? WHERE bid in (SELECT B._id FROM Books B WHERE B.categories LIKE ? AND B.sectionNames LIKE ? )" ;
		PreparedStatement stmt = null;
		try {
			stmt = c.prepareStatement(sql);
			stmt.setInt(1, displayNumber);
			stmt.setString(2, likeCategories);
			if(commentSection)
				stmt.setString(3,"%\"Comment\"]");
			stmt.executeUpdate();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void setHasLink(Connection c){
		String sql = "Select tid, count(*) as linkCount FROM (select tid1 as tid from Links_small UNION ALL select tid2 as tid from Links_small) GROUP BY tid";

		Statement stmt = null;
		try {
			stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery(sql);


			//stmt.executeUpdate();
			PreparedStatement preparedStatement = null;
			int groupCount = 0;
			while (rs.next()) {
				if((groupCount % 10000) == 0)
					System.out.print("\nsetHasLink:" + groupCount);
				if((groupCount++ % 1000) == 0)
					System.out.print(".");
				int tid = rs.getInt("tid");
				int linkCount = rs.getInt("linkCount");
				//System.out.println("counts: " + tid + " " + linkCount);
				String updateSql = "UPDATE Texts set hasLink=" + linkCount + " where _id =" + tid;
				preparedStatement = c.prepareStatement(updateSql);
				preparedStatement.execute();
			}
			stmt.close();
			if(preparedStatement != null)
				preparedStatement.close();
			else
				System.err.println("There were no links for hasLink\n");
			c.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//This is really bad code that should be done recursively. 
	/**
	 * 
	 * @param c
	 * @param json
	 * @return
	 * @throws JSONException
	 */
	protected static int addText(Connection c, JSONObject json) throws JSONException{

		int lang = returnLangNums(json.getString("language"));
		String title = json.getString("title");
		if(!booksInDB.containsKey(title)){
			System.err.println("Don't have book in DB and trying to add text");
			return -1;
		}
		int id = booksInDBbid.get(title);
		int textDepth = booksInDBtextDepth.get(title);
		int [] it = new int[MAX_LEVELS + 1];
		int forLoopNum = 6;
		if(forLoopNum != MAX_LEVELS)
			System.err.println("ERROR: forLoopNum is not teh same as MAX_LEVELS");
		boolean [] skipThisLoop = new boolean[forLoopNum + 1];
		JSONArray [] jsonArray = new JSONArray[forLoopNum + 1];

		jsonArray[forLoopNum] = (JSONArray) json.get("text");


		for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
			if(textDepth >= forLoopNum)
				try{
					jsonArray[forLoopNum -1] = jsonArray[forLoopNum].getJSONArray(it[forLoopNum]);
				}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
					continue;
				}
			else{
				jsonArray[forLoopNum - 1] = jsonArray[forLoopNum];
				skipThisLoop[forLoopNum] = true;
			}
			forLoopNum = 5;
			for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
				if(textDepth >= forLoopNum)
					try{
						jsonArray[forLoopNum -1] = jsonArray[forLoopNum].getJSONArray(it[forLoopNum]);
					}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
						continue;
					}
				else{
					jsonArray[forLoopNum - 1] = jsonArray[forLoopNum];
					skipThisLoop[forLoopNum] = true;
				}
				forLoopNum = 4;
				for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
					if(textDepth >= forLoopNum)
						try{
							jsonArray[forLoopNum -1] = jsonArray[forLoopNum].getJSONArray(it[forLoopNum]);
						}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
							continue;
						}
					else{
						jsonArray[forLoopNum - 1] = jsonArray[forLoopNum];
						skipThisLoop[forLoopNum] = true;
					}
					forLoopNum = 3;
					for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
						if(textDepth >= forLoopNum){
							try{
								jsonArray[forLoopNum -1] = jsonArray[forLoopNum].getJSONArray(it[forLoopNum]);
							}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
								continue;
							}

						}
						else{
							jsonArray[forLoopNum - 1] = jsonArray[forLoopNum];
							skipThisLoop[forLoopNum] = true;
						}
						forLoopNum = 2;
						for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
							if(textDepth >= forLoopNum){
								try{
									jsonArray[forLoopNum -1] = jsonArray[forLoopNum].getJSONArray(it[forLoopNum]);
								}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
									continue;
								}
							}
							else{
								jsonArray[forLoopNum - 1] = jsonArray[forLoopNum];
								skipThisLoop[forLoopNum] = true;
							}
							forLoopNum = 1;
							for(it[forLoopNum] = 0; !skipThisLoop[forLoopNum] && it[forLoopNum]<jsonArray[forLoopNum].length();it[forLoopNum]++){
								try{
									insertValues(c, title, textDepth, id, jsonArray[forLoopNum], lang, it,0);
								}catch(Exception e){//MOST LIKELY THIS IS B/C THERE IS A 0 and not another level of JSON there.
									continue;
								}
							}
							forLoopNum = 2;
						}
						forLoopNum = 3;
					}
					forLoopNum = 4;
				}
				forLoopNum = 5;
			}
			forLoopNum = 6;
		}
		return 1; //it worked
	}



	protected static int  insertValues(Connection c, String title,int textDepth, int bid, JSONArray jsonLevel1, int lang, int [] it,int parentNodeID) throws JSONException{
		String theText = "";
		try{

			Object textObj = jsonLevel1.get(it[1]);
			if(!(jsonLevel1.get(it[1]) instanceof String)) //it's not text
				return -1;	
			theText = (String) textObj;
			if(theText.length() < 1){ //this means that it's useless to try to add to the database.
				return -1;
			}

			//theText = convertToJH8(theText);////CONVERT FROM UTF8!!!!!!!
			//convertFromJH8(theText);
		}catch(Exception e){ //if there was a problem getting the text, then it probably wasn't text anyway so just leave the function.
			System.err.println("Error: " + e);
			System.err.println("sql_adding_text: Problem adding text " + title + " it[1] = " + it[1]);
			textsFailedToUpload++;
			return -1;
		}

		//Huffman.addTextCount(theText);//commented out in order to make the copying a seperated task
		PreparedStatement stmt = null;
		try{
			stmt = c.prepareStatement("INSERT INTO Texts ("
					+ Kbid + ", " + KenText + ", " + KheText + ", " 
					+ Klevel1 + ", " + Klevel2 + ", "+ Klevel3 + ", "+ Klevel4 + ", "+ Klevel5 + ", "+ Klevel6 + ", parentNode" + ")"
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

			stmt.setInt(1,bid); // Kbid

			int useableLang = lang + 1;//english =1 -> 2 & hebrew = 2 ->3
			stmt.setString(useableLang,theText); // KenText or  KheText

			final int LEVEL_IN_STATEMENT_START = 4;
			for(int i = 1; i<= MAX_LEVELS; i++){
				int num = 0;
				if(i<=textDepth)
					num = it[i]+1;
				stmt.setInt(LEVEL_IN_STATEMENT_START + i - 1,num);
			}
			stmt.setInt(LEVEL_IN_STATEMENT_START +MAX_LEVELS, parentNodeID);
			stmt.executeUpdate();
			stmt.close();
			Searching.countWords(lang,theText, ++textsUploaded, Searching.SEARCH_METHOD.FRESH_COMPRESS_INDEX);		

		}catch(SQLException e){
			if(e.getMessage().equals(HAS_TEXT_SQL_ERROR)){ //this text has already been placed into the db, so now you just want to add the text in the new lang.
				try{	
					int [] levels = new int [textDepth];
					for(int i = 1; i<= textDepth; i++){
						levels[i-1] = it[i] + 1;
					}
					String updateStatement = "UPDATE Texts set " + convertLangToLangText(lang) + " = ? WHERE " + whereClause(bid, levels,parentNodeID);
					stmt = c.prepareStatement(updateStatement);

					stmt.setString(1, theText);
					stmt.setInt(2, bid); //bid
					final int LEVEL_IN_UPDATE_START = 3;
					for(int i =1; i<=textDepth; i++){
						stmt.setInt(LEVEL_IN_UPDATE_START + i - 1,it[i] + 1);
					}
					stmt.setInt(LEVEL_IN_UPDATE_START + textDepth,parentNodeID);
					stmt.executeUpdate();
					stmt.close();
					///TODO currTID should be correct!!!

					//count words for searching table
					if(lang == LANG_HE){
						int tid = getTid(c, bid, levels, parentNodeID,textDepth,false,it, true);
						Searching.countWords(lang,theText, tid, Searching.SEARCH_METHOD.FRESH_COMPRESS_INDEX);
					}

				}catch(Exception e1){
					System.err.println("ERROR: " + e1);
					textsFailedToUpload++;
				}
			}
			else{
				System.err.println("ERROR: " + e);
				textsFailedToUpload++;
			}
		}
		return 0;
	}

	/**
	 * for a normal tid lookup, set useRealLevels = true and it = null
	 * 
	 * @param c
	 * @param bid
	 * @param levels
	 * @param parentNodeID
	 * @param textDepth
	 * @param useRealLevels
	 * @param it
	 * @return
	 * @throws SQLException
	 */
	public static int getTid(Connection c, int bid, int [] levels, int parentNodeID, int textDepth, boolean useRealLevels, int[] it, boolean getStart) throws SQLException{
		String findTID = "SELECT _id FROM Texts WHERE " + whereClause(bid, levels, parentNodeID);
		if(!getStart){
			findTID += " ORDER BY _id desc";
		}
		PreparedStatement stmt = c.prepareStatement(findTID);
		ResultSet rs;
		stmt.setInt(1, bid); //bid
		final int LEVEL_IN_UPDATE_START_2 = 2;
		int numberOfZeros = 0;
		if(!useRealLevels){
			for(int i =1; i<=textDepth; i++){
				stmt.setInt(LEVEL_IN_UPDATE_START_2 + i - 1, it[i] + 1);
			}
		}else{
			for(int i =0; i<textDepth; i++){
				if(levels[i] != 0){
					stmt.setInt(LEVEL_IN_UPDATE_START_2 + i - numberOfZeros, levels[i]);
				}else{
					//this means that we don't want to include this level.
					// probably b/c we want to get last item instead of first
					numberOfZeros++;
				}
			}
		}
		stmt.setInt(LEVEL_IN_UPDATE_START_2 + textDepth - numberOfZeros, parentNodeID);
		//System.out.println(findTID + "\n" + (LEVEL_IN_UPDATE_START_2 + textDepth) + ":"+ parentNodeID + ":"+ bid);
		rs = stmt.executeQuery();
		int tid = -1;
		if ( rs.next() ) {
			tid = rs.getInt(1);
		}
		else{
			System.err.print("tidError...");// + bid + " level1:" + levels[0] + " parNode:" + parentNodeID + "\n");
		}
		return tid;
	}

	public static int getTidFromNode(Connection c, int bid, int parentNodeID, boolean getFirst) throws SQLException{
		String findTID = "SELECT _id FROM Texts WHERE bid=" + bid + " AND (parentNode=" + parentNodeID
		+ " OR parentNode in (SELECT _id FROM Nodes WHERE parentNode = " + parentNodeID + "))";
		if(getFirst){
			findTID += " ORDER BY _id";
		}else{
			findTID += " ORDER BY _id desc ";
		}
		
		findTID += " limit 1";
		PreparedStatement stmt = c.prepareStatement(findTID);
		ResultSet rs;
		rs = stmt.executeQuery();
		int tid = -1;
		if ( rs.next() ) {
			tid = rs.getInt(1);
		}
		else{
			System.err.print("tidError...");
		}
		return tid;
	}

	
	
	private static String convertLangToLangText(String lang){
		if(lang.equals("en"))
			return KenText;
		else if(lang.equals("he"))
			return  KheText;
		System.err.println( "sql_text_convertLang: Unknown lang:" + lang);
		return "";
	}



	protected static String convertLangToLangText(int lang){
		if(lang == LANG_EN)
			return KenText;
		else if(lang == LANG_HE)
			return  KheText;
		System.err.println( "sql_text_convertLang: Unknown lang:" + lang);
		return "";
	}


	protected static String whereClause(int bid, int[] levels, int parentNodeID){
		String whereStatement = "bid = ? ";
		for(int i = 0; i < levels.length; i++){
			if(!(levels[i] == 0)){
				whereStatement += " AND level" + String.valueOf(i + 1) + "=? ";
			}
		}

		whereStatement += " AND parentNode = ? ";
		return whereStatement;
	}

	private static String bytesToHex(byte[] a) {
		StringBuilder sb = new StringBuilder(a.length * 3);

		for(int i =0 ; i< a.length; i++){
			int num = Byte.toUnsignedInt(a[i]);
			sb.append("0x" + Integer.toHexString(num) + " " );
		}
		return sb.toString();
	}


	static private boolean isValidUTF8(final byte[] bytes) {
		try {
			Charset.availableCharsets().get("UTF-8").newDecoder().decode(ByteBuffer.wrap(bytes));
		} catch (CharacterCodingException e) {
			return false;
		}
		return true;
	}

	static final int  NEXT_CHAR_IS_UNICODE_INT =  0xFA;
	static final byte NEXT_CHAR_IS_UNICODE = (byte) NEXT_CHAR_IS_UNICODE_INT;

	static private String convertToJH8(String original) throws UnsupportedEncodingException{
		ArrayList<Byte> newBytesList = new ArrayList <Byte> () ;  
		byte [] orgbytes = original.getBytes(Charset.forName("UTF-8"));
		//System.out.println(bytesToHex(orgbytes));
		if(!isValidUTF8(orgbytes)){
			System.err.println("ERROR: NOT VALID UTF-8");
			System.exit(-1);
		}
		for(int i = 0 ; i < orgbytes.length; i++){
			int num = Byte.toUnsignedInt(orgbytes[i]);
			int unicodeAmount = 0;
			if(num <= 0x7F){ //it's english
				newBytesList.add((byte) num);
			}
			else if(num == 0xD6 || num == 0xD7){//it's hebrew (unless it's armenian)
				int diff = 0;
				int nextNum = Byte.toUnsignedInt(orgbytes[++i]);
				if(num == 0xD7){
					diff = nextNum - 0x80 + 48; //48 is  (0xD6BF - 0xD690 + 1)
				}else{//num == 0xD6
					if(!(nextNum<= 0x90)){ 
						diff = nextNum - 0x90;
					}else{ //It's the rare few ARMENIAN chars from U+057F to U+058F
						diff = NEXT_CHAR_IS_UNICODE_INT;
						i--; //bring it back i b/c we assumed that this byte was for diff.
						unicodeAmount = 2;
					}
				}
				newBytesList.add((byte) diff);
			}
			else if(num <= 0xDF){//It's 2 Unicode Chars
				newBytesList.add(NEXT_CHAR_IS_UNICODE);
				unicodeAmount = 2;
			}
			else if(num <= 0xEF){//It's 3 Unicode Chars
				newBytesList.add(NEXT_CHAR_IS_UNICODE);
				unicodeAmount = 3;
			}
			else { //There's 4 Unicode Chars
				newBytesList.add(NEXT_CHAR_IS_UNICODE);
				unicodeAmount = 4;
			}

			//Add the unicode letters that belong
			for(int j = 0 ;j < unicodeAmount; j++){
				newBytesList.add((byte) Byte.toUnsignedInt(orgbytes[++i]));
			}
		}
		byte [] newbytes = new byte[newBytesList.size()];
		int bNum = 0;
		for (Byte b : newBytesList){
			newbytes[bNum++] = b;
		}

		String newString2 = new String(newbytes, "ISO-8859-1");
		/*
		byte [] testBytes = newString2.getBytes(Charset.forName("ISO-8859-1"));
		System.out.println(bytesToHex(testBytes));
		bNum = 0;
		for (Byte b : newBytesList){
			if(testBytes[bNum] != b){

				System.err.print("YOU SUCK: 0x" + Integer.toHexString( Byte.toUnsignedInt(b)) + " 0x" + Integer.toHexString(Byte.toUnsignedInt(testBytes[bNum])));
				System.exit(-1);
			}
			else
				System.out.print(".");
		}
		 */

		return newString2;
	}


	static private String convertFromJH8(String jhString) throws UnsupportedEncodingException{
		ArrayList<Byte> newBytesList = new ArrayList <Byte> () ;  
		byte [] jhBytes = jhString.getBytes(Charset.forName("ISO-8859-1"));
		for(int i = 0 ; i < jhBytes.length; i++){
			int num = Byte.toUnsignedInt(jhBytes[i]);
			//int unicodeAmount = 0;
			if(num == NEXT_CHAR_IS_UNICODE_INT){
				int nextNum = Byte.toUnsignedInt(jhBytes[i+1]);
				int unicodeAmount = 0;
				if(nextNum <= 0xDF){//It's 2 Unicode Chars
					unicodeAmount = 2;
				}
				else if(nextNum <= 0xEF){//It's 3 Unicode Chars
					unicodeAmount = 3;
				}
				else { //There's 4 Unicode Chars
					unicodeAmount = 4;
				}
				for(int j = 0; j< unicodeAmount;j++){
					newBytesList.add(jhBytes[++i]);
				}
			}
			else if(num <= 0x7F){ //it's english
				newBytesList.add((byte) num);
			}
			else //if(num >= 0x80){//it's hebrew
				if(num < 176){//original unicode was 0xD6 //176 = 48 + 0x80
					newBytesList.add((byte) 0xD6);
					newBytesList.add((byte)(0x10 + num));//0x10 = 0x90 - 0x80
				}
				else{ //original unicode was 0xD7)
					newBytesList.add((byte) 0xD7);					
					newBytesList.add((byte)(num - 48)); 
				}
		}
		//else //there's an error

		byte [] newbytes = new byte[newBytesList.size()];
		int bNum = 0;
		for (Byte b : newBytesList){
			newbytes[bNum++] = b;
		}

		String newString2 = new String(newbytes, "UTF-8");
		//System.out.println("FINISH: " + newString2);

		byte [] testBytes = newString2.getBytes(Charset.forName("ISO-8859-1"));
		System.out.print("P: " + newString2);
		System.out.print("P: "); 
		System.out.println(bytesToHex(testBytes));

		return newString2;
	}








}

