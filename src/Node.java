import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
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


public class Node extends SQLite{

	private static final int NODE_BRANCH = 1;
	private static final int NODE_TEXTS = 2;
	private static final int NODE_REFS = 3;

	static String CREATE_NODE_TABLE = 
			"CREATE TABLE " +  "Nodes " + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER NOT NULL,\r\n" + 
			"	parentNode INTEGER,\r\n" + 
			"	nodeType INTEGER not null,\r\n" + 
			"	siblingNum INTEGER not null,\r\n" + //0 means the first sibling 
			"	enTitle TEXT,\r\n" + 
			"	heTitle TEXT,\r\n" + 

			
			//for support of altStructs
			"	structNum INTEGER NOT NULL default 1,\r\n" + 
			"	textDepth INTEGER,\r\n" + 

			"	startTid INTEGER,\r\n" +  //maybe only used with refferences on alt structure
			"	endTid INTEGER,\r\n" +  //maybe only used with refferences on alt structure
			"	extraTids TEXT,\r\n" +  //maybe only used with refferences on alt structure ex. "[34-70,98-200]"
//maybe some stuff like to display chap name and or number (ei. maybe add some displaying info)
			
	//		"	FOREIGN KEY (bid) \r\n" + 
	//		"		REFERENCES Books (_id)\r\n" + 
	//		"		ON DELETE CASCADE,\r\n" + 
			"	FOREIGN KEY (parentNode) \r\n" + 
			"		REFERENCES Nodes (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	CONSTRAINT uniqSiblingNum UNIQUE (parentNode,siblingNum)\r\n" + 

				")";
	



	protected static int addText(Connection c, JSONObject json) throws JSONException{

		System.out.println("in add node.addText");

		
		int lang = returnLangNums(json.getString("language"));
		String title = json.getString("title");
		Boolean isFirstLang = true;
		if(!booksInDB.containsKey(title)){
			System.err.println("Don't have book in DB and trying to add text");
			//return -1;
		}
		int bid = 12345;//booksInDBbid.get(title);
		int textDepth = 0;//booksInDBtextDepth.get(title);
		JSONObject node = (JSONObject) json.get("schema");
		JSONObject text = (JSONObject) json.get("text");

		insertNode(c, node,text, 0,0,bid,0);
		return 1; //it worked
	}

	private static int nodeCount = 0;
	
	private final static String INSERT_NODE = "INSERT INTO Nodes (" +
			"_id,bid,parentNode,nodeType,siblingNum,enTitle,heTitle,structNum,textDepth,startTid,endTid,extraTids)"
			+ "VALUES (?,?, ?, ?, ?, ?, ?, ?,?, ?, ?,?);";
	private static int insertNode(Connection c, JSONObject node,JSONObject text,int depth, int siblingNum,int bid,int parentNode){
		String heTitle = node.getString("heTitle");
		String enTitle = node.getString("enTitle");
		int nodeID = ++nodeCount;
		int nodeType;
		JSONArray nodes = null;
		try{
			nodes  =  (JSONArray) node.get("nodes");
			if(depth >0)
				text = (JSONObject) text.get(enTitle);
			nodeType = NODE_BRANCH;
		}catch(Exception e){
			nodeType =NODE_TEXTS;//leaf
		}
		System.out.println(heTitle  + " " + enTitle);
		

		
		PreparedStatement stmt = null;
		try{
			stmt = c.prepareStatement(INSERT_NODE);
			stmt.setInt(1, nodeID);
			stmt.setInt(2,bid); // Kbid
			stmt.setInt(3,parentNode);
			stmt.setInt(4,nodeType); //TODO will need to change
			stmt.setInt(5,siblingNum);
			stmt.setString(6,enTitle);
			stmt.setString(7,heTitle);
			stmt.setInt(8,1); //TODO will need changing 
			//stmt.setInt(6,);
			//stmt.setInt(6,);
			stmt.executeUpdate();
			stmt.close();
		}catch(Exception e){
			System.err.println(e);
		}
		if(nodeType == NODE_BRANCH){
			for(int i =0;i<nodes.length();i++){
				insertNode(c, (JSONObject) nodes.get(i),text,depth+1,i,bid,nodeID);
			}
		} else if(nodeType == NODE_TEXTS){
			JSONArray textArray = (JSONArray) text.get(enTitle);
			ArrayList<Integer> levels = new ArrayList<Integer>();
			int textDepth = insertTextArray(textArray, levels);
			System.out.println("textDepth: " + textDepth);
		}
		
		return 0;
	}

	
	private static int insertTextArray(JSONArray textArray,ArrayList<Integer> levels){
		try{
			JSONArray textArray2 = textArray.getJSONArray(0);
			//if this worked that means thats there's more levels
			int returnDepth = 0;
			for(int i=0;i<textArray.length();i++){
				ArrayList<Integer> levels2 = (ArrayList<Integer>) levels.clone();
				levels2.add(i+1);
				returnDepth = insertTextArray(textArray.getJSONArray(i),levels2);
			}
			return returnDepth;
		}catch(Exception JSONException){
			levels.add(0);
			for(int i=0;i<textArray.length();i++){
				levels.set(levels.size()-1, i+1);
				String text = textArray.getString(i);
				//Text.insertValues(c, title, textDepth, id, jsonLevel1, lang, it, isFirstLang);
				System.out.println(" " + levels);
				//TODO Text.insertValue
			}
			return levels.size();
		}
		
	}
	
	private static int  insertNode1(Connection c, String title,int textDepth, int id, JSONArray jsonLevel1, int lang, int [] it, boolean isFirstLang) throws JSONException{
		String theText;
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
		}catch(Exception e){ //if there was a problem getting the text, then it probably wasn't text anyways so just leave the function.
			System.err.println("Error: " + e);
			System.err.println("sql_adding_text: Problem adding text " + title + " it[1] = " + it[1]);
			textsFailedToUpload++;
			return -1;
		}


		PreparedStatement stmt = null;
		try{


			stmt = c.prepareStatement("INSERT INTO Texts ("
					+ Kbid + ", " + KenText + ", " + KheText + ", " 
					+ Klevel1 + ", " + Klevel2 + ", "+ Klevel3 + ", "+ Klevel4 + ", "+ Klevel5 + ", "+ Klevel6 + ")"
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

			stmt.setInt(1,id); // Kbid
			int useableLang = lang + 1;//english =1 -> 2 & hebrew = 2 ->3
			//////////////////////

			stmt.setString(useableLang,theText); // KenText or  KheText




			final int LEVEL_IN_STATEMENT_START = 4;
			for(int i = 1; i<= MAX_LEVELS; i++){
				int num = 0;
				if(i<=textDepth)
					num = it[i]+1;
				stmt.setInt(LEVEL_IN_STATEMENT_START + i - 1,num);
			}
			stmt.executeUpdate();
			stmt.close();
			Searching.countWords(lang,theText, ++textsUploaded);

		}catch(SQLException e){
			if(e.getMessage().equals(HAS_TEXT_SQL_ERROR)){ //this text has already been placed into the db, so now you just want to add the text in the new lang.
				try{	
					int [] levels = new int [textDepth];
					for(int i = 1; i<= textDepth; i++){
						levels[i-1] = it[i] + 1;
					}
					String updateStatement = "UPDATE Texts set " + convertLangToLangText(lang) + " = ? WHERE " + whereClause(id, levels);
					stmt = c.prepareStatement(updateStatement);

					stmt.setString(1, theText);
					stmt.setInt(2, id); //bid
					final int LEVEL_IN_UPDATE_START = 3;
					for(int i =1; i<=textDepth; i++){
						stmt.setInt(LEVEL_IN_UPDATE_START + i - 1,it[i] + 1);
					}
					stmt.executeUpdate();
					stmt.close();
					///TODO currTID should be correct!!!

					if(lang == 2){
						String findTID = "SELECT _id FROM Texts WHERE " + whereClause(id, levels);
						stmt = c.prepareStatement(findTID);
						ResultSet rs;
						stmt.setInt(1, id); //bid
						final int LEVEL_IN_UPDATE_START_2 = 2;
						for(int i =1; i<=textDepth; i++){
							stmt.setInt(LEVEL_IN_UPDATE_START_2 + i - 1,it[i] + 1);
						}
						rs = stmt.executeQuery();
						int tid = -1;
						if ( rs.next() ) {
							tid = rs.getInt(1);
						}
						else
							System.err.println("couldn't find tid");
						Searching.countWords(lang,theText, tid);
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


	private static String convertLangToLangText(String lang){
		if(lang.equals("en"))
			return KenText;
		else if(lang.equals("he"))
			return  KheText;
		System.err.println( "sql_text_convertLang: Unknown lang");
		return "";
	}

	protected static String convertLangToLangText(int lang){
		if(lang == 1)
			return KenText;
		else if(lang == 2)
			return  KheText;
		System.err.println( "sql_text_convertLang: Unknown lang");
		return "";
	}


	protected static String whereClause(int bid, int[] levels){
		String whereStatement = "bid = ? ";
		for(int i = 0; i < levels.length; i++){
			if(!(levels[i] == 0)){
				whereStatement += " AND level" + String.valueOf(i + 1) + "=? ";
			}
		}
		return whereStatement;
	}

	public static String bytesToHex(byte[] a) {
		StringBuilder sb = new StringBuilder(a.length * 3);

		for(int i =0 ; i< a.length; i++){
			int num = Byte.toUnsignedInt(a[i]);
			sb.append("0x" + Integer.toHexString(num) + " " );
		}
		return sb.toString();
	}


	static boolean isValidUTF8(final byte[] bytes) {
		try {
			Charset.availableCharsets().get("UTF-8").newDecoder().decode(ByteBuffer.wrap(bytes));
		} catch (CharacterCodingException e) {
			return false;
		}
		return true;
	}

	static final int  NEXT_CHAR_IS_UNICODE_INT =  0xFA;
	static final byte NEXT_CHAR_IS_UNICODE = (byte) NEXT_CHAR_IS_UNICODE_INT;

	static String convertToJH8(String original) throws UnsupportedEncodingException{
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


	static String convertFromJH8(String jhString) throws UnsupportedEncodingException{
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

