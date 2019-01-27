import java.awt.PageAttributes.OriginType;
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.html.parser.Entity;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Node extends SQLite{
	private static int nodeCount = 0;


	static String CREATE_NODE_TABLE = 
			"CREATE TABLE " +  "Nodes " + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER NOT NULL,\r\n" + 
			"	parentNode INTEGER,\r\n" + 
			"	nodeType INTEGER not null,\r\n" + 
			"	siblingNum INTEGER not null,\r\n" + //0 means the first sibling 
			"	enTitle TEXT,\r\n" + 
			"	heTitle TEXT,\r\n" + 
			
			"	sectionNames TEXT,\r\n" + 
			"	heSectionNames TEXT,\r\n" + 

			//for support of altStructs
			"	structNum INTEGER NOT NULL default 1,\r\n" + 
			"	textDepth INTEGER,\r\n" + 

			"	startTid INTEGER,\r\n" +  //maybe only used with references on alt structure
			"	endTid INTEGER,\r\n" +  //maybe only used with references on alt structure
			"	extraTids TEXT,\r\n" +  //maybe only used with references on alt structure ex. "[34-70,98-200]"
			"	startLevels TEXT,\r\n" +  //
			"	key TEXT,\r\n" +  //
			//maybe some stuff like to display chap name and or number (ei. maybe add some displaying info)

	//		"	FOREIGN KEY (bid) \r\n" + 
	//		"		REFERENCES Books (_id)\r\n" + 
	//		"		ON DELETE CASCADE,\r\n" + 
	"	FOREIGN KEY (parentNode) \r\n" + 
	"		REFERENCES Nodes (_id)\r\n" + 
	"		ON DELETE CASCADE,\r\n" + 
	"	CONSTRAINT uniqSiblingNum UNIQUE (bid,parentNode,siblingNum,structNum)\r\n" + //needs bid b/c otherwise parent is 0 for each root 

				")";

	
	public static class NodeInfo{
		public int parentID;
		public int bid;
		public String enTitle;
		public NodeInfo(int bid,int parentID,String enTitle) {
			this.bid = bid;
			this.parentID = parentID;
			this.enTitle = enTitle;
		}
		@Override
		public String toString() {
			return "parentID: " + parentID + " bid: " + bid + " enTitle: " + enTitle; 
		}
		
		@Override
		public int hashCode() {
			return bid + parentID*2048 + enTitle.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof NodeInfo) {
	            NodeInfo pp = (NodeInfo) obj;
	            return (pp.enTitle.equals(this.enTitle) && pp.bid == this.bid && pp.parentID == this.parentID);
	        } else {
	            return false;
	        }
		}
	};
	public static class NodePair{
		public int nid;
		public int textDepth = 0;
		public NodePair(int nid,Integer textDepth) {
			this.nid = nid;
			if(textDepth != null)
				this.textDepth = textDepth;
		}
		@Override
		public String toString() {
			return "nid: " + nid + " textDepth: " + textDepth;
		}
	};


	protected static int addText(Connection c, JSONObject enJSON, JSONObject heJSON, JSONObject schemaFile) throws JSONException{
		if(enJSON == null && heJSON == null){
			System.err.print("Both JSONs are null in Node.addText()");
			return -1;
		}
		int enLang =0,heLang=0;
		String title ="";
		JSONObject node = null;
		if(enJSON != null){
			enLang = returnLangNums(enJSON.getString("language"));
			title = enJSON.getString("title");
			node = (JSONObject) enJSON.get("schema");
		}		
		if(heJSON != null){
			heLang = returnLangNums(heJSON.getString("language"));
			title = heJSON.getString("title");
			node = (JSONObject) heJSON.get("schema");
		}
		int lang = enLang + heLang;
		/**
		 * check for errors
		 */
		if((enLang != SQLite.LANG_EN  && enLang != 0)|| (heLang != SQLite.LANG_HE  && heLang != 0)){
			System.err.println("Error in Node.addText(): not right lang numbers. enLang:" + enLang  + " heLang:" + heLang);
			return -1;
		}
		if(title.equals("")){
			System.err.println("no Title");
			return -1;
		}
		if(enJSON != null && heJSON != null){
			if(!heJSON.get("schema").toString().equals(enJSON.get("schema").toString())){
				System.err.println("en and he JSONs schemas don't match\n");	
				System.out.println(heJSON.get("schema"));
				System.out.println(enJSON.get("schema"));
				return -1;
			}
			if(!enJSON.getString("title").equals(heJSON.getString("title"))){
				System.err.println("en and he JSONs title don't match" + enJSON.getString("title") + " - " + heJSON.getString("title"));
				return -1;
			}

		}
		if(!booksInDB.containsKey(title)){
			System.err.println("Don't have book in DB and trying to add text");
			return -1;
		}
		int bid = booksInDBbid.get(title); 
		JSONObject enText = null, heText = null;
		if(enJSON != null){
			enText = (JSONObject) enJSON.get("text");
		}
		if(heJSON != null){
			heText = (JSONObject) heJSON.get("text");
		}
		JSONObject schema = null;
		try{
			schema = schemaFile.getJSONObject("schema");
		}catch(JSONException e){
			e.printStackTrace();
		}
		insertNode(c, schema, enText,heText, 0,0,bid,0,lang);
		return 1; //it worked
	}

	private static final int IS_COMPLEX = 2;
	private static final int IS_TEXT_SECTION = 4;
	private static final int IS_GRID_ITEM = 8;
	private static final int IS_REF = 16;
	/***
	 * returns an int which represents the nodeType using boolean flags
	 * @param isComplex
	 * @param isTextSection
	 * @param isGridItem
	 * @param isRef
	 * @return nodeType
	 */
	private static int getNodeType(boolean isComplex,boolean isTextSection,boolean isGridItem, boolean isRef ){
		int nodeType = 1; //start off with something that isn't 0, so that 0 is reserved for when it's got no info
		if(isComplex)
			nodeType += IS_COMPLEX;
		if(isTextSection)
			nodeType += IS_TEXT_SECTION;
		if(isGridItem)
			nodeType += IS_GRID_ITEM;
		if(isRef)
			nodeType += IS_REF;

		return nodeType;
	}

	private static void insertSingleNodeToDB(Connection c, Integer nid, Integer bid, Integer parentNode,Integer nodeType, Integer siblingNum,
			String enTitle, String heTitle, Integer structNum, Integer textDepth, Integer startTid, Integer endTid,
			String extraTids, String sectionNames, String heSectionNames, String startLevels, String key
			){
		final String INSERT_NODE = "INSERT INTO Nodes (" +
				"_id,bid,parentNode,nodeType,siblingNum,enTitle,heTitle,structNum,textDepth,startTid,endTid,extraTids,sectionNames,heSectionNames, startLevels,key )"
				+ "VALUES (?,?, ?, ?, ?, ?, ?, ?,?, ?, ?,?,?,?,?,?);";

		PreparedStatement stmt;
		try {
			stmt = c.prepareStatement(INSERT_NODE);
			if(nid != null) stmt.setInt(1, nid);
			if(bid != null) stmt.setInt(2,bid);
			if(parentNode != null) stmt.setInt(3,parentNode);
			if(nodeType != null) stmt.setInt(4,nodeType);
			if(siblingNum != null) stmt.setInt(5,siblingNum);
			if(enTitle != null) stmt.setString(6,enTitle);
			if(heTitle != null) stmt.setString(7,heTitle);
			if(structNum != null) stmt.setInt(8,structNum); 
			if(startTid != null) stmt.setInt(10,startTid);
			if(endTid != null) stmt.setInt(11,endTid);
			if(extraTids != null) stmt.setString(12,extraTids);
			if(sectionNames != null) stmt.setString(13,sectionNames);
			if(heSectionNames != null) stmt.setString(14,heSectionNames);
			if(startLevels != null) stmt.setString(15,startLevels);
			if(key != null) stmt.setString(16, key);
			stmt.executeUpdate();
			stmt.close();
			
			NodeInfo nodeInfo = new NodeInfo(bid, parentNode, enTitle);
			NodePair nodePair = new NodePair(nid, textDepth);
			allNodesInDB.put(nodeInfo, nodePair);
			if("default".equals(key)){
				allDefaultNodesByBID.put(bid, nodePair);
				allDefaultNodesByNID.put(nid, nodePair);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private static int insertNode(Connection c, JSONObject node,JSONObject enText,JSONObject heText,int depth, int siblingNum,int bid,int parentNode,int lang){
		if(enText == null && heText == null){
			System.err.println("Both JSONs are null in insertNode");
			return -1;
		}
		String enTitle = null, heTitle = null;
		String key = null;
		try{
			key = node.getString("key");
		}catch(Exception e){
			System.err.print(".key fail. " + " ..." + e.toString() + "\n");
		}
		try{
			JSONArray titles = node.getJSONArray("titles");
			for(int i = 0; i < titles.length(); i++){
				JSONObject titleObj = titles.getJSONObject(i);
				try{
					if(titleObj.getBoolean("primary")){
						String titleLang = titleObj.getString("lang");
						if("he".equals(titleLang)){
							heTitle = titleObj.getString("text");
						}else if("en".equals(titleLang)){
							enTitle = titleObj.getString("text");
						}
					}
				}catch(JSONException e1){//couldn't get primary (or lang)
					;
				}
			}
		}catch(JSONException e){ //couldn't find titles
			try{
				enTitle = node.getString("title"); //enTitle
			}catch(JSONException e1){
				enTitle = key;
			}
			try{
				heTitle = node.getString("heTitle");		
			}catch(JSONException e1){
				heTitle = enTitle;
			}
		}
		
		
		if(enTitle == null){
			System.err.println("Problem getting enTitle");
			enTitle = key;
		}
		if(heTitle == null){
			System.err.println("Problem getting heTitle");
			heTitle = enTitle;
		}
		
		int nodeID = ++nodeCount;
		int nodeType;
		JSONArray nodes = null;
		try{
			nodes  =  node.getJSONArray("nodes");
			if(depth >0){
				if(enText != null)
					enText = enText.getJSONObject(enTitle); //this is the test to determine the Node type
				if(heText != null)
					heText = heText.getJSONObject(enTitle); //this is the test to determine the Node type
			}
			nodeType = getNodeType(true, false, false, false);
		}catch(Exception e){
			//System.out.println("Error in Nodes.insertNode(): " + e.getMessage() + "..." + enTitle + "-" + heTitle);
			//This whole block is to test if it's a IS_TEXT_SECTION

			boolean hasEnglish = false, hasHebrew = false;
			if(enText != null){
				JSONArray textArray = enText.getJSONArray(enTitle);
				try{
					textArray.getString(0);
					hasEnglish = true;
				}catch(Exception e1){}
			}
			
			if(heText != null){
				JSONArray textArray = (JSONArray) heText.getJSONArray(enTitle);
				try{
					textArray.getString(0);
					hasHebrew = true;
				}catch(Exception e1){}
			}
			nodeType = getNodeType(true, (hasEnglish || hasHebrew), false, false);
		}
		insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType, siblingNum, enTitle, heTitle, 1,
				null, null, null, null, null, null,null, key);
		if((nodeType & IS_TEXT_SECTION) == 0 && nodes != null){ //it's a branch and not a TEXT_SECTION
			for(int i =0;i<nodes.length();i++){
				insertNode(c, (JSONObject) nodes.get(i),enText,heText,depth+1,i,bid,nodeID,lang);
			}
		}else{
			
			int enTextDepth = 0, heTextDepth = 0;
			if(enText != null){
				JSONArray textArray = (JSONArray) enText.get(enTitle);
				ArrayList<Integer> levels = new ArrayList<Integer>();
				enTextDepth = insertTextArray(c, textArray, levels,bid,LANG_EN,nodeID);
			}
			if(heText != null){
				JSONArray textArray = (JSONArray) heText.get(enTitle);
				ArrayList<Integer> levels = new ArrayList<Integer>();
				heTextDepth = insertTextArray(c, textArray, levels,bid,LANG_HE,nodeID);
			}
			int textDepth = Math.max(enTextDepth, heTextDepth);
			String updateStatement = "UPDATE Nodes set textDepth = ? WHERE _id = ?";
			try {
				PreparedStatement stmt = c.prepareStatement(updateStatement);
				stmt.setInt(1, textDepth);
				stmt.setInt(2, nodeID);
				stmt.executeUpdate();
				stmt.close();
				NodePair nodePair = allDefaultNodesByNID.get(nodeID);
				if(nodePair != null){ //updating the nodePair textDepth value
					nodePair.textDepth = textDepth;
				}
				NodeInfo nodeInfo = new NodeInfo(bid, parentNode, enTitle);
				nodePair = allNodesInDB.get(nodeInfo);
				if(nodePair != null){ //updating the nodePair textDepth value
					nodePair.textDepth = textDepth;
				}
			} catch (SQLException e) {
				System.err.println("Error 1 in InsertNode: " + e);

			}

		}

		return 0;
	}


	/**
	 * //TODO have to deal with only hebrew getting into db.  
	 * @param connection
	 * @param textArray
	 * @param levels
	 * @param bid
	 * @param lang
	 * @param parentNodeID
	 * @return textDepth
	 */
	private static int insertTextArray(Connection c, JSONArray textArray,ArrayList<Integer> levels,int bid,int lang,int parentNodeID){
		try{
			int returnDepth = 0;
			for(int i=0;i<textArray.length();i++){
				ArrayList<Integer> levels2 = (ArrayList<Integer>) levels.clone();
				levels2.add(i);
				returnDepth = insertTextArray(c, textArray.getJSONArray(i),levels2,bid,lang,parentNodeID);
			}
			return returnDepth;
		}catch(Exception JSONException){
			levels.add(0);
			String title = "complex text:" + bid;
			int [] it = new int[MAX_LEVELS + 1];

			for(int i=0;i<textArray.length();i++){
				levels.set(levels.size()-1, i);
				for(int j=0;j<levels.size();j++)
					it[j+1] = levels.get(levels.size()-j-1);
				//String text = textArray.getString(i);
				//System.out.println(" " + levels + "---" + it);
				Text.insertValues(c, title, levels.size(), bid, textArray, lang, it,parentNodeID);

			}
			return levels.size();
		}

	}

	/**
	 * add Nodes for alt structures.
	 * @param c
	 * @param nodes
	 * @param bid
	 * @param parentNode
	 * @param structNum
	 * @return
	 * @throws JSONException
	 */
	private static int addSchemaRefNodes(Connection c, JSONArray nodes, int bid, int parentNode, int structNum) throws JSONException{
		for(int j =0;j<nodes.length();j++){
			JSONObject node = nodes.getJSONObject(j);
			String enTitle = node.getString("title");
			String heTitle = node.getString("heTitle");
			String nodeTypeString;
			try{
				 nodeTypeString = node.getString("nodeType");
			}catch(JSONException e){ // this seems to mean that there's another layer of depth first... going to test theory by getting "nodes"
				try{
					JSONArray nodes2 = node.getJSONArray("nodes");
					int nodeType = getNodeType(true, false, false, false);
					int nodeID = ++nodeCount;
					insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType, j,enTitle, heTitle, structNum,null,null,null,null,null,null,null,null);
					addSchemaRefNodes(c, nodes2, bid, nodeID, structNum);
					continue;
				}catch(JSONException e1){
					System.err.println(e1);
					throw e;	
				}
			}
			String key = null;
			try{
				key = node.getString("key");
			}catch(Exception e){
				;//I think this happens everytime but I'm going to keep this in here just in case there is a key
			}
			int nodeType;
			if(!nodeTypeString.equals("ArrayMapNode")){
				System.err.println("Error not a ref");
				return -1;	
			}
			
			String sectionNames = "[",heSectionNames = "[";
			try{
				JSONArray sectionNamesArray = node.getJSONArray("sectionNames");
				if(sectionNamesArray.length()>0){
					sectionNames += "\"";
					heSectionNames += "\"";
				}
				for(int i=0;i<sectionNamesArray.length();i++){
					String name = sectionNamesArray.getString(i);
					if(name.length() == 0)
						name = "Section";
					String heName = tryToGetHebrewName(name);
					if(i<sectionNamesArray.length()-1){
						name += "\",\"";
						heName += "\",\"";
					}
					sectionNames += name;
					heSectionNames += heName;
				}
				if(sectionNamesArray.length()>0){
					sectionNames += "\"";
					heSectionNames += "\"";
				}
			}catch(JSONException e){
				;
			}
			sectionNames += "]";
			heSectionNames += "]";
			//String sectionNames = node.getJSONArray("sectionNames").toString().replace("\"\"", "\"Section\"");
			//String heSectionNames = sectionNames;//node.getJSONArray("heSectionNames").toString().replace("\"\"", "\"Section\"");
			int depth = node.getInt("depth");
			JSONArray refs;
			if (depth == 1){
				int nodeID = ++nodeCount;
				nodeType = getNodeType(true, false, false, false);//NODE_REFS;
				refs = node.getJSONArray("refs");
				int offset;
				try{
					offset = node.getInt("offset");
				}catch(JSONException e){
					//default offset to 0
					offset = 0;
				}
				insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType, j, enTitle, heTitle, structNum,
						null, null, null, null, sectionNames, heSectionNames,null, key);

				//System.out.println(enTitle + " " +  heTitle + " " + nodeType + " " + sectionNames + refs);
				int subParentNode = nodeID;
				for(int i = 0; i < refs.length(); i++){
					String ref = refs.getString(i);
					if(ref.equals("")) continue;
					nodeID = ++nodeCount;
					nodeType = getNodeType(true, true, true, true);
					RefValues refValues = ref2Tids(c, ref, bid);
					int startTID = refValues.startTid;
					int endTID = refValues.endTid;
					insertSingleNodeToDB(c, nodeID, bid, subParentNode, nodeType,
							i + offset, "", "", structNum, null, startTID, endTID, ref,
							sectionNames, heSectionNames, refValues.startLevels, key);
				}
			}else if(depth == 0){ //so the refs aren't in a grid
				int nodeID = ++nodeCount;
				nodeType = getNodeType(true, true, false, true);
				//TODO get real heSection names
				String ref = node.getString("wholeRef");
				if(ref.equals("")) continue;
				RefValues refValues = ref2Tids(c,ref, bid);
				int startTID = refValues.startTid;
				int endTID = refValues.endTid;
				insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType,
						j, enTitle, heTitle, structNum, null, startTID, endTID, ref,
						sectionNames, heSectionNames,refValues.startLevels, key);
			}else{
				System.err.println("Node.addSchemaNodes(): I don't know how to deal with this depth:" + depth);
			}

		}
		return 1;//worked
	}

	private static int [] halfRef2Levels(String ref){
		String [] levelsStr  = ref.replace(" ", "").split(":");
		//println("levelsStr:" +levelsStr[0]+","+levelsStr[1] + "... " + ref);
		int [] levels = new int [levelsStr.length];
		for(int i=0;i<levelsStr.length;i++){
			levels[i] = Link.catchDafs(levelsStr[levelsStr.length-i-1]);
		}

		return levels;
	}

	private static class RefValues{
		
		public RefValues(int startTid,int endTid,int [] start, String ref){
			this.startTid = startTid;
			this.endTid = endTid;
			this.startLevels = Arrays.toString(start);
			this.ref = ref;
		}
		public int startTid;
		public int endTid;
		public String startLevels;
		public String ref;
	}
	
	private static RefValues ref2Tids(Connection c, String ref, int bid){
		// (space or ".") then has (numbers dash or a/b (for dafs)) and goes to end
		final String regex_numbersDashestoEnd = "[\\s\\.][0-9a-b" + "\u2013"  + "-]*$";
		String fullRef = ref;
		String title = booksInDBbid2Title.get(bid);
		int textDepth = booksInDBtextDepth.get(title);
		int parentNode = 0; // this will be 0 unless there's a complex text that it's a ref to
		if(textDepth == 0){ //its compex
			Node.NodePair nodePair = null;
			try{
				String pathNoNum = ref.replaceAll(regex_numbersDashestoEnd, "");
				nodePair = Link.getParentID(title, pathNoNum, bid);
				ref = ref.replace(pathNoNum, "");//Converting ref to just the number part
			}catch(Exception e){//idk
				nodePair = allDefaultNodesByBID.get(bid);
			}
			if(nodePair != null){
				textDepth = nodePair.textDepth;
				parentNode = nodePair.nid;
			}
		}
		
		int startTid = 0, endTid =0;
		int [] start = null, stop = null;
		try{
		try{
			String [] startStop = ref.replace(title, "").split("-");
			if(startStop.length == 1){
				 //This is "–" which slightly different char then "-" (it's a different UTF8 value)
				startStop = ref.replace(title, "").split("\u2013");
				//EN DASH vs. HYPHEN-MINUS
			}
			
			stop = start;
			if(startStop.length == 2){
				start = halfRef2Levels(startStop[0]);
				stop = halfRef2Levels(startStop[1]);
				
			}else if(startStop.length == 1){
				try{
					start = halfRef2Levels(startStop[0]);
				}catch(NumberFormatException e){
					//If length is 1, it's probably just listing out the whole node. (could be text or number)
					final Pattern p = Pattern.compile("\\d+[ab]?"); // number (and maybe an a or b for daf amud)
					Matcher m = p.matcher(startStop[0]);
					if(!m.find()){
						//Ex. Ben Ish Hai: "wholeRef": "Ben Ish Hai, Halachot 1st Year, Bereshit"
						startTid = Text.getTidFromNode(c, bid, parentNode, true);
						endTid = Text.getTidFromNode(c, bid, parentNode, false);
						return new RefValues(startTid, endTid, start, fullRef);
					}else{
						throw e;
					}
				}
				stop = start;
			}else{
				System.err.println("Error in Node.Schema.ref2Tids: wrong number of startStop. ref:" + ref);
			}
		}catch(NumberFormatException e){
			String newRef = fullRef.replace(".", " ");
			if(!newRef.equals(fullRef)){
				return ref2Tids(c, newRef, bid);
			}
			throw e;
		}
		if(textDepth != start.length || textDepth != stop.length){
			if(stop.length < start.length){
				int [] tempStop = new int [start.length];
				for(int i=0;i<stop.length;i++){
					tempStop[i] = stop[i];
				}
				for(int i=stop.length;i<start.length;i++){
					tempStop[i] = start[i];
				}
				stop = tempStop;
			}			
			start = add1sForMissingLevels(start, textDepth, true); 
			stop = add1sForMissingLevels(stop, textDepth, false);
		}
		try {
			startTid = Text.getTid(c, bid, start, parentNode, textDepth, true, null, true);
			endTid = Text.getTid(c, bid, stop, parentNode, textDepth, true, null, false);
		} catch (SQLException e) {
			throw new Exception();
		}
		if(startTid < 0 || endTid < 0){
			throw new Exception();
		}
		}catch(Exception e){
			System.err.println("Error in Node.Schema.ref2Tids: " + e.toString());
		}
		//System.out.println(fullRef + "...s,e: " + startTid + ", "  + endTid);
		return new RefValues(startTid, endTid, start, fullRef);
	}
		
	static private int [] add1sForMissingLevels(int [] levels, int textDepth, boolean startVsend){
		int additionalNum;
		if(startVsend){
			 //I just have to assume that it's referring to the 1st object
			additionalNum = 1;
		}else{
			// will use 0 to get to end of item
			additionalNum = 0;
		}
		if(levels.length < textDepth){
			int [] tempLevels = new int [textDepth];
			int diff = textDepth - levels.length;
			for(int i=0;i<diff;i++){
				tempLevels[i] = additionalNum;
			}
			for(int i= diff;i<tempLevels.length;i++){
				tempLevels[i] = levels[i-diff];
			}
			return tempLevels;
		}else{
			return levels;
		}
		
	}
	String as = "";
	private static final Map<String, String> sectionNameMap;
    static
    {
    	sectionNameMap = new HashMap<String, String>();
		sectionNameMap.put("Chapter", "\u05E4\u05E8\u05E7");
		sectionNameMap.put("Chapters", "\u05E4\u05E8\u05E7\u05D9\u05DD");
		sectionNameMap.put("Perek", "\u05E4\u05E8\u05E7");
		sectionNameMap.put("Line", "\u05E9\u05D5\u05E8\u05D4");
		sectionNameMap.put("Daf", "\u05D3\u05E3");
		sectionNameMap.put("Paragraph", "\u05E4\u05E1\u05E7\u05D4");
		sectionNameMap.put("Parsha", "\u05E4\u05E8\u05E9\u05D4");
		sectionNameMap.put("Parasha", "\u05E4\u05E8\u05E9\u05D4");
		sectionNameMap.put("Parashah", "\u05E4\u05E8\u05E9\u05D4");
		sectionNameMap.put("Seif", "\u05E1\u05E2\u05D9\u05E3");
		sectionNameMap.put("Se'if", "\u05E1\u05E2\u05D9\u05E3");
		sectionNameMap.put("Siman", "\u05E1\u05D9\u05DE\u05DF");
		sectionNameMap.put("Section", "\u05D7\u05DC\u05E7");
		sectionNameMap.put("Verse", "\u05E4\u05E1\u05D5\u05E7");
		sectionNameMap.put("Sentence","\u05DE\u05E9\u05E4\u05D8");
		sectionNameMap.put("Sha'ar", "\u05E9\u05E2\u05E8");
		sectionNameMap.put("Gate", "\u05E9\u05E2\u05E8");
		sectionNameMap.put("Comment","\u05E4\u05D9\u05E8\u05D5\u05E9");
		sectionNameMap.put("Phrase", "\u05D1\u05D9\u05D8\u05D5\u05D9");
		sectionNameMap.put("Mishna", "\u05DE\u05E9\u05E0\u05D4");
		sectionNameMap.put("Chelek", "\u05D7\u05DC\u05E7");
		sectionNameMap.put("Helek", "\u05D7\u05DC\u05E7");
		sectionNameMap.put("Year", "\u05E9\u05E0\u05D4");
		sectionNameMap.put("Masechet", "\u05DE\u05E1\u05DB\u05EA");
		sectionNameMap.put("Massechet", "\u05DE\u05E1\u05DB\u05EA");
		sectionNameMap.put("Letter", "\u05D0\u05D5\u05EA");
		sectionNameMap.put("Halacha", "\u05D4\u05DC\u05DB\u05D4");
		sectionNameMap.put("Piska", "\u05E4\u05E1\u05E7\u05D4");
		sectionNameMap.put("Seif Katan", "\u05E1\u05E2\u05D9\u05E320\u05E7\u05D8\u05DF");
		sectionNameMap.put("Se'if Katan", "\u05E1\u05E2\u05D9\u05E320\u05E7\u05D8\u05DF");
		sectionNameMap.put("Volume", "\u05DB\u05E8\u05DA");
		sectionNameMap.put("Book", "\u05E1\u05E4\u05E8");
		sectionNameMap.put("Shar", "\u05E9\u05E2\u05E8");
		sectionNameMap.put("Seder", "\u05E1\u05D3\u05E8");
		sectionNameMap.put("Part", "\u05D7\u05DC\u05E7");
		sectionNameMap.put("Pasuk", "\u05E4\u05E1\u05D5\u05E7");
		sectionNameMap.put("Sefer", "\u05E1\u05E4\u05E8");
		sectionNameMap.put("Teshuva", "\u05EA\u05E9\u05D5\u05D1\u05D4");
		sectionNameMap.put("Teshuvot", "\u05EA\u05E9\u05D5\u05D1\u05D5\u05EA");
		sectionNameMap.put("Tosefta", "\u05EA\u05D5\u05E1\u05E4\u05EA\u05D0");
		sectionNameMap.put("Halakhah", "\u05D4\u05DC\u05DB\u05D4");
		sectionNameMap.put("Kovetz", "\u05E7\u05D5\u05D1\u05E5");
		sectionNameMap.put("Path", "\u05E0\u05EA\u05D9\u05D1");
		sectionNameMap.put("Parshah", "\u05E4\u05E8\u05E9\u05D4");
		sectionNameMap.put("Midrash", "\u05DE\u05D3\u05E8\u05E9");
		sectionNameMap.put("Mitzvah", "\u05DE\u05E6\u05D5\u05D4");
		sectionNameMap.put("Tefillah", "\u05EA\u05E4\u05D9\u05DC\u05D4");
		sectionNameMap.put("Torah", "\u05EA\u05D5\u05E8\u05D4");
		sectionNameMap.put("Perush", "\u05E4\u05D9\u05E8\u05D5\u05E9");
		sectionNameMap.put("Peirush", "\u05E4\u05D9\u05E8\u05D5\u05E9");
		sectionNameMap.put("Aliyah", "\u05E2\u05DC\u05D9\u05D9\u05D4");
		sectionNameMap.put("Tikkun", "\u05EA\u05D9\u05E7\u05D5\u05DF");
		sectionNameMap.put("Tikkunim", "\u05EA\u05D9\u05E7\u05D5\u05E0\u05D9\u05DD");
		sectionNameMap.put("Hilchot", "\u05D4\u05D9\u05DC\u05DB\u05D5\u05EA");
		sectionNameMap.put("Topic", "\u05E0\u05D5\u05E9\u05D0");
		sectionNameMap.put("Contents", "\u05EA\u05D5\u05DB\u05DF");
		sectionNameMap.put("Article", "\u05E1\u05E2\u05D9\u05E3");
		sectionNameMap.put("Shoresh", "\u05E9\u05D5\u05E8\u05E9");
		sectionNameMap.put("Remez", "\u05E8\u05DE\u05D6");
		}
	
	private static String tryToGetHebrewName(String enName){
		 return sectionNameMap.getOrDefault(enName, enName);
	}

	
	protected static int addWholeSchemas(Connection c, JSONObject schemas) throws JSONException{
		try{
			JSONObject alts = schemas.getJSONObject("alts");
			String bookTitle = schemas.getString("title");
			String default_struct = "__UNUSED__"; //didn't leave it blank in case 2 sturcts both have no name and I push them together.
			try{
				default_struct = schemas.getString("default_struct");
			}catch(Exception e){
				;
			}
			int bid = booksInDBbid.get(bookTitle);
			String [] altNames = JSONObject.getNames(alts);
			for(int i=0;i<altNames.length;i++){
				//System.out.println(altNames[i]);
				String altName = altNames[i];
				JSONObject alt = alts.getJSONObject(altName);
				Integer nodeID = ++nodeCount;
				int structNum;
				if(altName.equals(default_struct))
					structNum = 0; //this is even b/f the rigid one which is at number 1
				else
					structNum = i+2;//1 is the rigid one, so add 2 to the alt structs.
				String enTitle =  altName;
				String heTitle = tryToGetHebrewName(enTitle);//TODO make real heTitle
				int nodeType = getNodeType(true, false, false, false);
				String sectionNames = "[\"" + enTitle + "\"]";
				String heSectionNames = "[\"" + heTitle + "\"]";
				insertSingleNodeToDB(c, nodeID, bid, 0, nodeType, 0,enTitle , heTitle, structNum,
						null, null, null, null, sectionNames, heSectionNames,null, null);
				JSONArray nodes;
				try{
					nodes = alt.getJSONArray("nodes");
				}catch(JSONException e){
					JSONObject [] altArray = new JSONObject [] {alt};
					nodes = new JSONArray(altArray);
				}
				
				addSchemaRefNodes(c, nodes, bid, nodeID, structNum);
			}
		}catch(JSONException e1){
			if(!e1.toString().equals("org.json.JSONException: JSONObject[\"alts\"] not found.")){
				throw e1;//I don't know what it is so throw it back out there
			}
			//else it only has a single structure 
		}catch(Exception e){
			System.err.println("Error (addSchemas): " + e.toString());
			e.printStackTrace();
		}

		return 1; //it worked
	}

}	

