package Gennerator;

import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;

public class FileLoader {
	private String fileDir;
	Query query;
	
	public FileLoader()
	{
		query = new Query();
		System.out.print("Enter the direction of your file please: ");
		Scanner scan = new Scanner(System.in);
		fileDir = scan.nextLine();
	}
	
	public String returnFileDir()
	{
		return fileDir;
	}
	
	public Query returnQuery()
	{
		try
		{
			File file = new File(fileDir);
			Scanner sc = new Scanner(file);
			
			String type  = sc.nextLine();
			String [] projection = sc.nextLine().split(",");
			String where = sc.nextLine();
			String [] groupAtt = sc.nextLine().split(",");
			int numOfGroupVar = Integer.parseInt(sc.nextLine());
			String [] aggregates = sc.nextLine().split(",");
			String [] groupVar = sc.nextLine().split(",");
			String having = sc.nextLine();
			
			if(!(type.equals("EMF")||type.equals("MF")))
			{
				System.out.println("Unvalid type of query!");
				System.exit(-1);
			}else
				query.type = type;
			
			for(String s:projection)
				query.selectAtt.add(s);
			for(String s:groupAtt)
				query.groupAtt.add(s);
			for(String s:groupVar)
				query.groupVar.add(s);
			for(String s:aggregates)
			{
				String[] temp = s.split("_");
				ArrayList<String> list = new ArrayList<String>();
				for(String ss:temp)
				{
					list.add(ss);
				}
				query.aggregate.add(list);
			}
			
			//´æ´¢where ºÍ number ºÍ having
			query.whereClause = where;
			query.numOfGroupVar = numOfGroupVar;
			query.havingClause = having;
		}catch(Exception e)
		{
			System.out.println("Failed Read File! Please Check the Direction.");
			e.printStackTrace();
		}
		
		return query;
	}
	
	public static void main(String args[])
	{
		FileLoader fl = new FileLoader();
		Query q = fl.returnQuery();
		
		System.out.println(q.havingClause);
	}
	

}
