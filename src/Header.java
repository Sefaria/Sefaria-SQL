import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Header extends SQLite {

	static int headerCount = 0;
	static int headersFailed = 0;
	
	
	public static final String CREATE_HEADES_TABLE = "CREATE TABLE " + TABLE_HEADERS + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER,\r\n" + 
			"	heHeader TEXT,\r\n" + 
			"	level1 INTEGER,\r\n" + 
			"	level2 INTEGER,\r\n" + 
			"	level3 INTEGER,\r\n" + 
			"	level4 INTEGER,\r\n" + 
			"	level5 INTEGER,\r\n" + 
			"	level6 INTEGER,\r\n" + 
			"	displayNum BOOLEAN DEFAULT 1,\r\n" +
			"	displayLevelType BOOLEAN DEFAULT 1,\r\n" +
			" 	enHeader TEXT," +
			"	FOREIGN KEY(bid) \r\n" + 
			"		REFERENCES Books(bid)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	CONSTRAINT HeadersUnique UNIQUE (bid,level1,level2,level3,level4,level5, level6)\r\n" + 
			")";

	static void addAllHeaders(Connection c, String folderName) throws FileNotFoundException{
		File folder = new File(folderName);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				System.out.println("\t" + listOfFiles[i].getName());
				String path = folderName + listOfFiles[i].getName();
				CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(path)));
				Header.addHeader(c, reader);
			}
			//else if (listOfFiles[i].isDirectory()) {
			// System.out.println("Directory " + listOfFiles[i].getName());
		}

		try {
			c.commit();
			System.out.println("Good Headers: " + (headerCount - headersFailed) + "\nFailed Headers: " + headersFailed);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}		


	static void addHeader(Connection c, CSVReader reader){

		String row[] = {};

		try {
			String lastTitle = new String();
			int bid = 0;
			while(true) {
				row = reader.readNext();
				if(row != null) {
					headerCount++;
					try{
						if(!booksInDB.containsKey(row[0])){//don't have book, so go to the next line
							headersFailed++;
							continue;
						}
						if(!row[0].equals(lastTitle)){
							lastTitle = row[0];							
							bid = booksInDBbid.get(row[0]);
						}
						PreparedStatement stmt = c.prepareStatement("INSERT INTO Headers (" +
								Kbid + ", " + 
								Klevel1 + ", " + 
								Klevel2 + ", " + 
								Klevel3 + ", " + 
								Klevel4 + ", " + 
								Klevel5 + ", " + 
								Klevel6 + ", " + 
								"heHeader" + ", " + 
								KdisplayNum + ", " + 
								KdisplayLevelType  +  ", " + 
								"enHeader" +
								") VALUES (?,?,?,?,?,?,?,?,?,?,?);");

						stmt.setInt(1, bid);
						stmt.setInt(2, Integer.valueOf(row[1]));
						stmt.setInt(3, Integer.valueOf(row[2]));
						stmt.setInt(4, Integer.valueOf(row[3]));
						stmt.setInt(5, Integer.valueOf(row[4]));
						stmt.setInt(6, Integer.valueOf(row[5]));
						stmt.setInt(7, Integer.valueOf(row[6]));
						stmt.setString(8, row[7]);
						stmt.setInt(9, Integer.valueOf(row[8]));
						stmt.setInt(10, Integer.valueOf(row[9]));
						String enHeader;
						try{
							enHeader = row[10];
						}catch(ArrayIndexOutOfBoundsException e1){
							enHeader = "";
						}
						stmt.setString(11,enHeader);
						stmt.executeUpdate();
						stmt.close();


					} catch (Exception e){
						headersFailed++;
						System.err.println("Header Error: " + e + " " + row[0]);
					}
				} else {
					break;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}



		return;
	}

}
