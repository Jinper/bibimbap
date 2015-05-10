package Gennerator;

import java.util.HashMap;

public class Processor {
	public static void main(String args[])
	{
		DBInfo dbinfo = new DBInfo();
		dbinfo.connect();
		dbinfo.readDBStructure();
		HashMap<String, String> dbStructure = dbinfo.returnDBStructure();
		
		FileLoader fileloader = new FileLoader();
		Query query  = fileloader.returnQuery();
		
		Writer writer = new Writer(dbStructure,query);
		writer.write();
		
	}
}
