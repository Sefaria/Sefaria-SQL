# Sefaria-SQL
Converts [Sefaria-Export](https://github.com/Sefaria/Sefaria-Export) to SQLite database.

## Where to get database

## How to use
1. git clone https://github.com/Sefaria/Sefaria-SQL.git 
2. git clone https://github.com/Sefaria/Sefaria-Export.git (into the same dir that Sefaria-SQL is in)
3. Go to scripts/links and run: pytyhon2 createLinks.py
4. Go to scripts/fileList and run: pytyhon2 createFileList.py
5. (Not really needed b/c headers are part of clone) go to Sefaria-SQL/scripts/headers and run: pytyhon2 createHeaders.py 
6. Open Sefaria-SQL in [Eclipse](http://www.eclipse.org/downloads/) for Java (File -> import -> Existing Projects into Workspace)
7. In src/SQLite.java, you can change variables
8. Run project
9. The exported database is in testDBs/ and word counts are saved in wordCounts/
