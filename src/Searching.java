import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class Searching {



	private static Hashtable<String, Integer> countText = new Hashtable <String, Integer> ();
	private static Hashtable<String, BitSet> chunkBLOB = new Hashtable <String, BitSet> ();
	private static Hashtable<String, ArrayList<Integer>> allTermLocations = new Hashtable<String, ArrayList<Integer>>();
	
	
	final static int CHUNK_SIZE = 312;

	
	final static private String SEARCHING_FULL_INDEX_NAME = "SearchingFull";

	private static void resetCounts(){
		countText = new Hashtable <String, Integer> (); 
		chunkBLOB = new Hashtable <String, BitSet> ();
		allTermLocations = new Hashtable<String, ArrayList<Integer>>();
	}
	
	public enum SEARCH_METHOD{
		COPY,
		NO_KEY,
		FRESH_FULL_INDEX,
		FRESH_COMPRESS_INDEX
	};
	
	public static void makeSearching(SEARCH_METHOD searchMethod, Connection newConnection, String oldDBName, String newDBName) throws SQLException{
		if(searchMethod == SEARCH_METHOD.COPY){
			Huffman.copyTable(newConnection, "Searching", Searching.CREATE_SEARCH, newDBName);
		}else if(searchMethod == SEARCH_METHOD.NO_KEY){
			Statement stmt = newConnection.createStatement();
			stmt.executeUpdate("CREATE TABLE SearchingNoIn(_id TEXT,chunks BLOB\r)");
			String columns = "_id, chunks";
			newConnection.prepareStatement("INSERT INTO SearchingNoIn (" + columns + ") SELECT " + columns + " FROM oldDB.Searching").execute();
		}else if(searchMethod == SEARCH_METHOD.FRESH_FULL_INDEX){
			makeFreshIndex(newConnection, oldDBName, searchMethod);
		}else if(searchMethod == SEARCH_METHOD.FRESH_COMPRESS_INDEX){
			makeFreshIndex(newConnection, oldDBName, searchMethod);
		}
	}
	
	public static void makeFreshIndex(Connection newDBConnection, String oldDB, SEARCH_METHOD searchMethod) throws SQLException{
		Statement stmt = newDBConnection.createStatement();
		String tableName = "Searching";
		if(searchMethod == SEARCH_METHOD.FRESH_FULL_INDEX){
			tableName = SEARCHING_FULL_INDEX_NAME;
		}
		String sql1 = "CREATE TABLE " + tableName + " (_id TEXT PRIMARY KEY, chunks BLOB\r)";
		//sql1 = "CREATE TABLE " + SEARCHING_FULL_INDEX_NAME + " (_id TEXT PRIMARY KEY, counts INT, chunks BLOB\r)";
		stmt.executeUpdate(sql1);
		stmt.close();
		resetCounts();
		Connection oldDBConnection = SQLite.getDBConnection(oldDB);
		String sql = "Select _id, heText from Texts where heText not null;";
		try {
			stmt = oldDBConnection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int tid = rs.getInt("_id");
				if(tid % 20000 == 0)
					System.out.println("fresh search index tid:" + tid + ". totalCount:" + allTermLocations.size());
				String heText = rs.getString("heText");
				countWords(SQLite.LANG_HE, heText, tid, searchMethod);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		putInCountWords(newDBConnection, searchMethod);
		resetCounts();
		System.out.println("Done fresh full index");
	}

	
	private static String [] getWords(String text){
		//String orgTetx = ""+ text;
		text = text.replaceAll("[\u05be]", " ");
		text = text.replaceAll("[\u0591-\u05C7\u05f3\u05f4\'\"]", "");
		text = text.replaceAll("([^\u05d0-\u05ea])", " ");
		String [] words = text.split(" ");
		for(int i=0; i<words.length;i++){
			words[i] = words[i].replaceAll("\\s", "").replaceAll("\\b\\u05d5",""); ///remove white space and starting vavs
		}
		return words;
	}
	
	enum INDEX_TYPE{
		count,
		fullIndex,
		chunk
	}

	public static void countWords(int lang, String text, int tid, SEARCH_METHOD searchMethod) throws SQLException{
		if(lang != SQLite.LANG_HE)
			return;
		if(searchMethod != SEARCH_METHOD.FRESH_COMPRESS_INDEX && searchMethod != SEARCH_METHOD.FRESH_FULL_INDEX){
			System.err.println("WRONG SEARCHING METHOD IN COUNT WORDS!!!");
			return;
		}
		String [] words = getWords(text);
		BitSet tempBits = null;
		for(int i = 0; i< words.length; i++){
			String word = words[i];
			if(word.length() == 0)
				continue;
			//if(word.equals("à"))
			//	i = i +1 -1	;]
			if(!countText.containsKey(word)){
				countText.put(word, 1);
				
				if(searchMethod == SEARCH_METHOD.FRESH_COMPRESS_INDEX){
					tempBits = new BitSet();		
					int index = tid/CHUNK_SIZE;
					tempBits.set(index);
					chunkBLOB.put(word,tempBits);
				}else{
					ArrayList<Integer> list = new ArrayList<Integer>();
					list.add(tid);
					allTermLocations.put(word, list);
				}					
			}
			else{
				int newCount = countText.get(word)+1;
				countText.put(word, newCount);
				
				if(searchMethod == SEARCH_METHOD.FRESH_COMPRESS_INDEX){
					int index = tid/CHUNK_SIZE;
					tempBits = chunkBLOB.get(word);
					tempBits.set(index);
				}else{
					ArrayList<Integer> list = allTermLocations.get(word);
					if(list.get(list.size() -1) != tid) //make sure we don't repeat adding the same tid
						list.add(tid);
				}

			}
		}

	}


	private static byte[] toByteArray(BitSet bits, int byteCount) {
		if(byteCount <= 0)
			byteCount = bits.length()/8+1;
		byte[] bytes = new byte[byteCount];
		for (int i=0; i<bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length-i/8-1] |= 1<<(i%8); //or 
			}
		}
		return bytes;
	}


	final static int BITS_PER_PACKET = 24;
	final static int PACKET_SIZE = 32; 

	private static ArrayList<Integer> JHpacketToNums (Blob blob) throws SQLException{
		byte[] bytes = blob.getBytes(1, (int)blob.length());
		ArrayList<Integer> chunkList = new ArrayList<Integer>();
		int bitNum = 0;
		for(int i = 0;i< bytes.length/4;i++){
			int a = Integer.valueOf(bytes[i*4])*BITS_PER_PACKET;
			for(int j = 0; j <3;j++){
				byte b = bytes[i*4 +j];
				byte mask = 0x01;
				for (int k = 0; k < 8; k++)
				{
					if((b & mask) != 0){
						chunkList.add(bitNum);
					}
					bitNum++;
					mask = (byte) (mask << 1);

				}
			}
		}
		return chunkList;
	}

	private static ArrayList<Integer> findDoubles(ArrayList<Integer> l1, ArrayList<Integer> l2){
		ArrayList<Integer> list = new ArrayList<Integer>(l1.size());
		int j =0;
		int i = 0;
		while(i < l1.size() && j < l2.size()){
			int num1 = l1.get(i);
			int num2 = l2.get(j);
			if(num1 == num2){
				i++;
				j++;
				list.add(num1);
			}
			else if(num1 < num2)
				i++;
			else //num1 > num2
				j++;
		}		
		return list;
	}


	private static byte[] toJHpackets(BitSet bits) {
		//packet =: byte { packNum, MSB,.., LSB}
		//ex. 47 = 24 + 23 -> {1 (=24/24), 0xA0,0x00,0x00}
		ArrayList<Byte> blob = new ArrayList<Byte>();
		int packetCount = bits.length()/BITS_PER_PACKET + 1;
		if(packetCount > 255){
			System.err.println("TO BIG PACKET COUNT... MUST CHANGE TO 8 BYTE packets...packetCouunt: " + packetCount);
			System.exit(-1);
		}
		
		for(int i = 0; i< packetCount; i++){
			BitSet packBits = new BitSet(BITS_PER_PACKET);
			boolean usingPacket = false;
			for(int j=0; j< BITS_PER_PACKET; j++){
				int bitNum = j+i*BITS_PER_PACKET;
				if(bitNum >= bits.length()){ // this will happen whenever bits.length isn't an even multiple of BITS_PER_PACKET
					break;
				}
				try{
				if(bits.get(bitNum)){
					usingPacket = true;
					packBits.set(j);
				}
				}catch(Exception e){
					System.err.println("bitnum: " + bitNum + ".. bits.length:" + bits.length() + " (i,j): " + i + "," + j + "...packetCouunt: " + packetCount); 
					break;
				}
			}
			if(usingPacket){//add packet
				blob.add((byte)i);
				byte [] packByte = toByteArray(packBits,BITS_PER_PACKET/8);
				for(int k = 0;k <BITS_PER_PACKET/8;k++){
					blob.add(packByte[k]);
				}
			}
		}
		byte [] bytes = new byte[blob.size()];
		for(int i = 0; i<blob.size(); i++)
			bytes[i] = blob.get(i);
		return bytes ;
	}

	private static String bitSetToString(BitSet bits){
		StringBuilder chunkString = new StringBuilder(bits.length()*2);
		for(int i = 0; i<bits.length(); i++){
			if(bits.get(i))
				chunkString.append("1.");
			else
				chunkString.append("0.");

		}
		return chunkString.toString();
	}

	//not needed for this part of the code:
	private static BitSet blobToBitSet(Blob blob) throws SQLException {
		byte[] bytes = blob.getBytes(1, (int)blob.length());
		BitSet bitSet = fromByteArray(bytes);

		return bitSet;
	}

	private static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i=0; i<bytes.length*8; i++) {
			if ((bytes[bytes.length-i/8-1]&(1<<(i%8)))!= 0){//  if ((bytes[bytes.length-i/8-1]&(1< 0) {
				bits.set(i);
			}
		}
		return bits;
	}

	public final static String CREATE_SEARCH = "CREATE table Searching (_id TEXT PRIMARY KEY, chunks BLOB)";//, packCount INTEGER, count INTEGER, , chunkstring TEXT)"); //, isNeg BOOLEAN

	public static void putInCountWords(Connection c, SEARCH_METHOD searchMethod){
		if(searchMethod != SEARCH_METHOD.FRESH_COMPRESS_INDEX && searchMethod != SEARCH_METHOD.FRESH_FULL_INDEX){
			System.err.println("WRONG SEARCHING METHOD IN PUT_COUNT WORDS!!!");
			return;
		}
		System.out.println("HashTable size: " + countText.size() +  "... NOW MAKING IT>>>> (IF YOU DON'T WANT IT IN THERE, REMOVE IT NOW!!");

		Set<String> keys = countText.keySet();


		System.out.println("creating wordCounts...");
		FileWriter writer;
		try {
			writer = new FileWriter("wordCounts/" + SQLite.DB_NAME_PART + "_counts.csv");
			for(String key: keys){
				writer.append(key +"," + countText.get(key)+ "\n");
			}
			writer.flush();
			writer.close();	
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		
		PreparedStatement stmt = null;
		try {
			boolean wasAutoCommit = c.getAutoCommit();
			c.setAutoCommit(false);
			System.out.println("creating searching...");
			String tableName = "Searching";
			if(searchMethod == SEARCH_METHOD.FRESH_FULL_INDEX)
				tableName = SEARCHING_FULL_INDEX_NAME;
			stmt = c.prepareStatement("INSERT into " + tableName + " VALUES (?,?)");
			System.out.println("Total keys: " + keys.size());
			int i = 0;
			
			int maxPacketCount = 0;
			String maxPacketString = "";
			for(String key: keys){
				if(i++ % 50000 == 0){
					System.out.println("Adding to searching key num:" + (i-1));
					c.commit();
				}

				
				stmt.setString(1,key);
				//stmt.setInt(2, countText.get(key));
				byte[] bytes;
				if(searchMethod == SEARCH_METHOD.FRESH_COMPRESS_INDEX){
					BitSet bits = chunkBLOB.get(key);
					int packetCount = bits.length()/BITS_PER_PACKET + 1;
					if(packetCount > maxPacketCount){
						maxPacketCount = packetCount;
						maxPacketString = key;
					}
					bytes = toJHpackets(bits);
				}else{ //SEARCH_METHOD.FRESH_FULL_INDEX
					ArrayList<Integer> list = allTermLocations.get(key);
					int [] intList = new int [list.size()];
					int j = 0;
					for(int num: list){
						intList[j++] = num;
					}
			        ByteBuffer byteBuffer = ByteBuffer.allocate(intList.length * 4);        
			        IntBuffer intBuffer = byteBuffer.asIntBuffer();
			        intBuffer.put(intList);
			        bytes = byteBuffer.array();
				}
				stmt.setBinaryStream(2,new ByteArrayInputStream(bytes),bytes.length);
				//stmt.setInt(4, bytes.length);
				//stmt.setInt(3, 1);
				stmt.execute();
			}
			System.out.println("packetCount: " + maxPacketCount + ".. " + maxPacketString);
			stmt.close();
			c.commit();
			c.setAutoCommit(wasAutoCommit);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		try {
			Huffman.setSettings("blockSize", ""+CHUNK_SIZE, c);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
