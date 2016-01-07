import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Set;


public class Searching {



	static Hashtable<String, Integer> countText = new Hashtable <String, Integer> (); 
	static Hashtable<String, BitSet> chunkBLOB = new Hashtable <String, BitSet> (); 
	final static int CHUNK_SIZE = 500;
	final static int VERSE_COUNT = 1000000;
	final static int CHUNK_COUNT = VERSE_COUNT/CHUNK_SIZE;
	final static int WORD_COUNT = 25000000;

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

	public static void countWords(int lang, String text, int tid) throws SQLException{
		if(lang != 2)
			return;
		String [] words = getWords(text);
		BitSet tempBits = null;
		for(int i = 0; i< words.length; i++){
			String word = words[i];
			if(word.equals(""))
				continue;
			//if(word.equals("א"))
			//	i = i +1 -1	;]
			if(!countText.containsKey(word)){
				countText.put(word, 1);
				tempBits = new BitSet(CHUNK_COUNT);
				int index = tid/CHUNK_SIZE;
				tempBits.set(index);
				chunkBLOB.put(word,tempBits);
			}
			else{

				int newCount = countText.get(word)+1;
				countText.put(word, newCount);
				int index = tid/CHUNK_SIZE;
				tempBits = chunkBLOB.get(word);
				tempBits.set(index);

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
	final static int MAX_PACKET_COUNT = (int) Math.ceil(CHUNK_COUNT/BITS_PER_PACKET);

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
		byte packNum = 0;
		for(byte i = 0; i< MAX_PACKET_COUNT; i++){
			BitSet packBits = new BitSet(BITS_PER_PACKET);
			boolean usingPacket = false;
			for(int j=0; j< BITS_PER_PACKET; j++){
				if(bits.get(j+i*BITS_PER_PACKET)){
					usingPacket = true;
					packBits.set(j);
				}
			}
			if(usingPacket){//add packet
				blob.add(i);
				byte [] packByte = toByteArray(packBits,BITS_PER_PACKET/8);
				for(int k = 0;k <BITS_PER_PACKET/8;k++){
					blob.add(packByte[k]);
				}
			}
		}
		int j=0;
		byte [] bytes = new byte[blob.size()];
		for(int i = 0; i<blob.size(); i++)
			bytes[i] = blob.get(i);
		return bytes ;
	}

	private static String bitSetToString(BitSet bits){
		StringBuilder chunkString = new StringBuilder(CHUNK_COUNT*2);
		for(int i = 0; i<CHUNK_COUNT; i++){
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

	public static final String CREATE_SEARCH = "CREATE table Searching (_id TEXT PRIMARY KEY, chunks BLOB)";//, packCount INTEGER, count INTEGER, , chunkstring TEXT)"); //, isNeg BOOLEAN

	public static void putInCountWords(Connection c){
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
			System.out.println("creating searching...");
			stmt = c.prepareStatement("INSERT into Searching VALUES (?,?)");
			for(String key: keys){
				//if(key.equals("ברא") )//key.equals("גדול") || 
				//	System.out.print("ok");
				stmt.setString(1,key);
				//stmt.setInt(2, countText.get(key));
				BitSet bits = chunkBLOB.get(key);
				byte[] bytes = toJHpackets(bits);
				stmt.setBinaryStream(2,new ByteArrayInputStream(bytes),bytes.length);
				//stmt.setInt(4, bytes.length);
				//stmt.setInt(3, 1);
				stmt.execute();
			}
			stmt.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}


	}

}
