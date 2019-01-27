# Sefaria-SQL
Converts [Sefaria-Export](https://github.com/Sefaria/Sefaria-Export) to SQLite database.

## Where to get database

## How to use
1. git clone https://github.com/Sefaria/Sefaria-SQL.git 
2. git clone https://github.com/Sefaria/Sefaria-Export.git (into the same dir that Sefaria-SQL is in)
3. Go to scripts/links and run: python2 createLinks.py
4. Go to scripts/fileList and run: python2 createFileList.py
5. (Not really needed b/c headers are part of clone) go to Sefaria-SQL/scripts/headers and run: python2 createHeaders.py 
6. Open Sefaria-SQL in [Eclipse](http://www.eclipse.org/downloads/) for Java (File -> import -> Existing Projects into Workspace)
7. In src/SQLite.java, you can change variables
8. Run project
9. The exported database is in testDBs/ and word counts are saved in wordCounts/

## Exploring the Code

The java code is in src/

SQLite.java is the highest level code (it run at startup). Book.java contains methods for inputting the data about each book into the database. Simularly, Header, Link, Searching, and Text are responsible for putting their respective items into the database (in their own table). Node.java is responsible for putting in Nodes for complex texts and/or alternate structures.

There are some preprocessing python scripts in scripts/

scripts/fileList/createFileList.py creates a list of files to be upload based on the index and exported files.

scripts/links/createLinks.py converts [Sefaria-Export/links/links.csv](https://github.com/Sefaria/Sefaria-Export/blob/master/links/links.csv) to scripts/links/links0.csv which has divided level numbers and is easier to upload.



## License

[GPL](http://www.gnu.org/copyleft/gpl.html)
