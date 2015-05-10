package Gennerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DBInfo {
	private HashMap<String, String> dbStructure = new HashMap<String, String>();
    private Connection connect;
	
	private String url;
	private String user;
	private String password;
	
	public DBInfo(String url, String user, String password)
	{
		this.url = url;
		this.user = user;
		this.password = password;
	}
	
	public DBInfo()
	{
		url = "jdbc:postgresql://localhost:5432/testdb";
		user = "postgres";
		password = "zuoyouzuo";
	}
	
	public void readDBStructure()
	{		
		try
		{
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery("select column_name,data_type from information_schema.columns"
					+ " where table_name='sales'");
			
			while(rs.next())
			{
				String type;
				String name = rs.getString("column_name");
				if(rs.getString("data_type").equals("character varying") || rs.getString("data_type").equals("character"))
				{
					type = "String";
				}
				else
					type = "int";
				dbStructure.put(name, type);
			}
			
			closeConnect();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public HashMap<String, String> returnDBStructure()
	{
		return dbStructure;
	}
	
	public void connect()
	{
		try 
		{
			Class.forName("org.postgresql.Driver");     
			System.out.println("Success loading Driver!");
		} catch(Exception exception)
		{
			System.out.println("Failed loading Driver!");
			exception.printStackTrace();
		}
		
		try
		{
			connect=DriverManager.getConnection(url,user,password);
			System.out.println("Success Connect Dadabase!");
		}catch(Exception e)
		{
			System.out.println("Failed Connect Database!");
			e.printStackTrace();
		}
	}
		
	public Connection returnConnect()
	{
		return connect;
	}
	
	public void closeConnect()
	{
		try
		{
			connect.close();
		}catch(Exception e)
		{
			System.out.println("Failed Close Connection!");
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		DBInfo dbInfo = new DBInfo();
		dbInfo.connect();
		dbInfo.readDBStructure();
		for(Map.Entry<String, String> entry: dbInfo.returnDBStructure().entrySet())
		{
			System.out.println("column name: "+entry.getKey()+" data type: "+entry.getValue());
		}
	}
}
