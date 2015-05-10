package Gennerator;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Writer {
	private HashMap<String,String> dbStructure;
	private Query query;

	public Writer(HashMap<String,String> map,Query q)
	{
		dbStructure = map;
		query = q;
	}
	
	public void write()
	{
		writeMFStructure();
		writeTuple();
		writeMain();
	}
	
	public void writeMFStructure()
	{
		try
		{
			File mfFile = new File("src\\MFStructure.java");
			if(mfFile.exists())
				if(!mfFile.delete())
					System.out.println("Failed Delete File!");
			
			if(!mfFile.exists())
			{
				if(!mfFile.createNewFile())
					System.out.println("Failed Create File!");
				else
				{
					PrintWriter pw = new PrintWriter(mfFile);
					
					pw.println("import java.util.*;");
					pw.println("class MF_Structure {");
					
					for(String s:query.groupAtt)
					{
						if(dbStructure.get(s).equals("String"))
							pw.println("\tArrayList<String> " + s + "= new ArrayList<String>();");
						else
							pw.println("\tArrayList<Integer> " + s + "= new ArrayList<Integer>();");
					}
					
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(s.contains("avg"))
						{
							String sum = s.replace("avg", "sum");
							String count = s.replace("avg", "count");
							pw.println("\tArrayList<Integer> " + sum +"= new ArrayList<Integer>();");
							pw.println("\tArrayList<Integer> " + count +"= new ArrayList<Integer>();");
							pw.println("\tArrayList<Integer> " + s +"= new ArrayList<Integer>();");
						}
						else
							pw.println("\tArrayList<Integer> " + s +"= new ArrayList<Integer>();");
					}
					
					pw.println("\tMF_Structure(){}");
					pw.println("\tint getSize() {");
					pw.println("\t\treturn "+query.groupAtt.get(0)+".size();");
					pw.println("\t}");
						
					pw.println("}");
					pw.flush();
					
				}
			}
		}catch(Exception e)
		{
			System.out.println("Failed Write File!");
			e.printStackTrace();
		}
	}
	
	public void writeTuple()
	{
		try
		{
			File tupleFile = new File("src\\Tuple.java");
			if(tupleFile.exists())
				if(!tupleFile.delete())
					System.out.println("Failed Delete File!");
			
			if(!tupleFile.exists())
			{
				if(!tupleFile.createNewFile())
					System.out.println("Failed Create File!");
				else
				{
					PrintWriter pw = new PrintWriter(tupleFile);
					pw.println("class Tuple {");
					
					for(Map.Entry<String, String> entry:dbStructure.entrySet())
					{
						pw.println("\t" + entry.getValue() + " " + entry.getKey() + ";");
					}
					
					pw.println("}");	
					pw.flush();
				}
			}
		}catch(Exception e)
		{
			System.out.println("Failed Write File!");
		}
		
			
	}
	
	public void writeMain()
	{
		try
		{
			File outFile = new File("src\\Query_Output.java");
			if(outFile.exists())
				if(!outFile.delete())
					System.out.println("Failed Delete File!");
			
			if(!outFile.exists())
			{
				//创建文件不成功
				if(!outFile.createNewFile())
					System.out.println("Failed to create file!");
				//创建文件成功
				else
				{
					PrintWriter pw = new PrintWriter(outFile);
					
					pw.println("import java.sql.*;\n" + "import java.util.*;");   //写入import 语句
					
					pw.println();
					//---------------------------------------------写入主函数----------------------------------------------
					pw.println("public class Query_Output{");
					pw.println("\tpublic static void main(String args[]) {");
					//----------------------写入加载驱动----------------------
					pw.println("\t\ttry {");
					pw.println("\t\t\tClass.forName(\"org.postgresql.Driver\");");
					pw.println("\t\t\tSystem.out.println(\"Loading Driver Successfully!\");");
					pw.println("\t\t}catch(Exception e) {");
					pw.println("\t\t\tSystem.out.println(\"Failed Extracting Data!\");");
					pw.println("\t\t\te.printStackTrace();");
					pw.println("\t\t}");
					
					//----------------------写入连接数据库---------------------
					pw.println("\t\ttry {");
					pw.println("\t\t\tConnection connect=DriverManager.getConnection(\"jdbc:postgresql://localhost:5432/testdb\",\"postgres\",\"zuoyouzuo\");");
					pw.println("\t\t\tSystem.out.println(\"Success Connect Dadabase!\");");
					pw.println("\t\t\tStatement stmt=connect.createStatement();");
					pw.println("\t\t\tMF_Structure mf = new MF_Structure();");
					
					//----------------------写入执行语句----------------------
					//--------------第一次扫描-------------
					pw.println("\t\t\tResultSet rs=stmt.executeQuery(\"select * from sales\");");
					pw.println("\t\t\twhile(rs.next()) {");
					pw.println("\t\t\t\tboolean found = false;");
					pw.println("\t\t\t\tTuple tuple = new Tuple();");
					//接收数据
					for(Map.Entry<String, String> entry:dbStructure.entrySet())
					{
						if(entry.getValue().equals("String"))
							pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getString(\""+entry.getKey()+"\");");
						else if(entry.getValue().equals("int"))
							pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getInt(\""+entry.getKey()+"\");");
					}
					
					//检查是否为空
					
					pw.println("\t\t\t\tif(mf.getSize()==0&&"+query.whereClause+"){");
					
					for(String s:query.groupAtt)
					{
						pw.println("\t\t\t\t\tmf."+s+".add(tuple."+s+");");
					}
					
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(list.get(1).equals("0"))
						{
							if(list.get(0).equals("sum")||list.get(0).equals("max")||list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\tmf."+s+".add("+"tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\tmf."+s+".add(1);");
							else if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_0_"+list.get(2)+".add("+"tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\tmf.count_0_"+list.get(2)+".add(1);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
						}
						else
						{
							if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf.count_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
							else
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
						}
					}
					pw.println("\t\t\t\t}"); //if结束
					
					pw.println("\t\t\t\telse{"); //else，非空遍历
					pw.println("\t\t\t\t\tfor(int i=0;i<mf.getSize();i++){");
					
					//---------------------if(mf.product.get(i).equals(product) && mf.month.get(i)==month && year==1991)----
					pw.print("\t\t\t\t\t\tif(");
					for(String s:query.groupAtt)
					{
						pw.print("mf."+s+".get(i).equals(tuple."+s+")&&");
					}
					pw.print(query.whereClause+"){\n");
					pw.println("\t\t\t\t\t\t\tfound = true;");
					for(ArrayList<String> list:query.aggregate)
					{
						if(list.get(1).equals("0"))
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							if(list.get(0).equals("sum"))
								pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+tuple."+list.get(2)+");");
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+1);");
							else if(list.get(0).equals("avg"))
							{
								String sum = s.replace("avg", "sum");
								String count = s.replace("avg", "count");
								pw.println("\t\t\t\t\t\t\tmf."+sum+".set(i,mf."+sum+".get(i)+tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\t\t\tmf."+count+".set(i,mf."+count+".get(i)+1);");
							}
							else if(list.get(0).equals("max"))
							{
								pw.println("\t\t\t\t\t\t\tif(tuple."+list.get(2)+")>mf."+s+".get(i)");
								pw.println("\t\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\t\t\tif(tuple."+list.get(2)+")<mf."+s+".get(i)");
								pw.println("\t\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
							}
						}
					}
					pw.println("\t\t\t\t\t\t\tbreak;");
					pw.println("\t\t\t\t\t\t}");//if 结束
					pw.println("\t\t\t\t\t}");//for 结束
					
					//------------------------if((!found) && year==1991)----------------------
					pw.println("\t\t\t\t\tif(!found&&"+query.whereClause+"){");
					for(String s:query.groupAtt)
					{
						pw.println("\t\t\t\t\tmf."+s+".add(tuple."+s+");");
					}
					
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(list.get(1).equals("0"))
						{
							if(list.get(0).equals("sum")||list.get(0).equals("max")||list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\tmf."+s+".add("+"tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\tmf."+s+".add(1);");
							else if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_0_"+list.get(2)+".add("+"tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\tmf.count_0_"+list.get(2)+".add(1);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
						}
						else
						{
							if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf.count_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
							else
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
						}
					}
					pw.println("\t\t\t\t\t}");
					pw.println("\t\t\t\t}");//else结束
					pw.println("\t\t\t}");//while 结束
					for(ArrayList<String> list:query.aggregate)
					{
						
						if(list.get(0).equals("avg")&&list.get(1).equals("0"))
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							String sum = s.replace("avg", "sum");
							String count = s.replace("avg", "count");
							pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++){");
							pw.println("\t\t\t\tif(mf."+count+".get(i)==0)");
							pw.println("\t\t\t\t\tmf."+s+".set(i,0);");
							pw.println("\t\t\t\telse");
							pw.println("\t\t\t\t\tmf."+s+".set(i,mf."+sum+".get(i)/mf."+count+".get(i));");
							pw.println("\t\t\t}");
						}
					}
					
					pw.println();
					pw.println("//-----------------------scan 1 to n------------------------");
					//-----------------------后续扫描------------------------
					for(int n=1;n<=query.numOfGroupVar;n++)
					{
						pw.println("\t\t\trs = stmt.executeQuery(\"select * from sales\");");
						pw.println("\t\t\twhile(rs.next()){");
						pw.println("\t\t\t\tTuple tuple = new Tuple();");
						for(Map.Entry<String, String> entry:dbStructure.entrySet())
						{
							if(entry.getValue().equals("String"))
								pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getString(\""+entry.getKey()+"\");");
							else if(entry.getValue().equals("int"))
								pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getInt(\""+entry.getKey()+"\");");
						}
						pw.println("\t\t\t\tfor(int i=0;i<mf.getSize();i++){");
						
						//区分mf和emf
						if(query.type.equals("EMF"))
							pw.println("\t\t\t\t\tif("+query.groupVar.get(n-1)+"&&"+query.whereClause+"){");
						
						else if(query.type.equals("MF"))
						{
							pw.print("\t\t\t\t\tif(");
							for(String s:query.groupAtt)
							{
								pw.print("tuple."+s+".equals(mf."+s+".get(i))"+"&&");
							}
							pw.print(query.groupVar.get(n-1)+query.whereClause+"){");
						}
						for(ArrayList<String> list:query.aggregate)
						{
							if(list.get(1).equals(String.valueOf(n)))
							{
								String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
								if(list.get(0).equals("sum"))
									pw.println("\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+tuple."+list.get(2)+");");
								else if(list.get(0).equals("count"))
									pw.println("\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+1);");
								else if(list.get(0).equals("avg"))
								{
									String sum = s.replace("avg", "sum");
									String count = s.replace("avg", "count");
									pw.println("\t\t\t\t\t\tmf."+sum+".set(i,mf."+sum+".get(i)+tuple."+list.get(2)+");");
									pw.println("\t\t\t\t\t\tmf."+count+".set(i,mf."+count+".get(i)+1);");
								}
								else if(list.get(0).equals("max"))
								{
									pw.println("\t\t\t\t\t\tif(tuple."+list.get(2)+")>mf."+s+".get(i)");
									pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
								}
								else if(list.get(0).equals("min"))
								{
									pw.println("\t\t\t\t\t\tif(tuple."+list.get(2)+")<mf."+s+".get(i)");
									pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
								}
							}
						}
						
						
						
						pw.println("\t\t\t\t\t}");
						
						pw.println("\t\t\t\t}");//for 结束
						pw.println("\t\t\t}");//while 结束
						
						for(ArrayList<String> list:query.aggregate)
						{
							
							if(list.get(0).equals("avg")&&list.get(1).equals(String.valueOf(n)))
							{
								String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
								String sum = s.replace("avg", "sum");
								String count = s.replace("avg", "count");
								pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++){");
								pw.println("\t\t\t\tif(mf."+count+".get(i)==0)");
								pw.println("\t\t\t\t\tmf."+s+".set(i,0);");
								pw.println("\t\t\t\telse");
								pw.println("\t\t\t\t\tmf."+s+".set(i,mf."+sum+".get(i)/mf."+count+".get(i));");
								pw.println("\t\t\t}");
							}
						}
					}//n次扫描结束
					
					//------------------------------having部分-------------------------
					if(query.havingClause.equals("null"))
						pw.println("\t\t\tMF_Structure mfs = mf;");
					else
					{
						pw.println("\t\t\tMF_Structure mfs = new MF_Structure();");
						pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++) {");
						pw.println("\t\t\t\tif("+query.havingClause+"){");
						for(String s:query.groupAtt)
						{
							pw.println("\t\t\t\t\tmfs."+s+".add(mf."+s+".get(i));");
						}
						for(ArrayList<String> list:query.aggregate)
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							pw.println("\t\t\t\t\tmfs."+s+".add(mf."+s+".get(i));");
						}
						
						pw.println("\t\t\t\t}");
						pw.println("\t\t\t}");
					}
					
					
					//------------------------------写入显示结果部分------------------------
					pw.print("\t\t\tSystem.out.println(\"");
					for(String string:query.selectAtt)
					{
						String[] temp = string.split("\\.");
						String s = temp[1];
						if(s.equals("prod"))
							pw.print(s+"     ");
						else if(s.equals("cust"))
							pw.print(s+"     ");
						else if(string.contains("/"))
						{
							
							String[] t = string.split("/");
							String[] t1 = t[0].split("\\.");
							String[] t2 = t[1].split("\\.");
							String combo = t1[1]+"/"+t2[1];
							
							pw.print(combo+" ");
						}
						else
							pw.print(s+" ");
					}
					pw.print("\");\n");
					pw.println("\t\t\tfor(int i=0;i<mfs.getSize();i++){");
					pw.println("\t\t\t\tfloat x=0;");
					for(String s:query.selectAtt)
					{
						if(s.contains("/"))
						{
							String[] temp = s.split("/");
							pw.println("\t\t\t\tif("+temp[temp.length-1]+"!=0)");
							pw.println("\t\t\t\t\tx=(float)"+s+";");
							
						}
					}
					
					pw.print("\t\t\t\tSystem.out.printf(\"");
					for(String string:query.selectAtt)
					{
						String [] str = string.split("\\.");
						String s = str[1];
						int size = s.length();
						if(s.equals("prod")||s.equals("cust"))
							size = 8;
						
						String align;
						String c;
						
						if(dbStructure.containsKey(s))
						{
							if(dbStructure.get(s).equals("String"))
							{
								align = "-";
								c = "s";
							}
							else 
							{
								align = "";
								c = "d";
							}
						}
						else
						{
							if(string.contains("/"))
							{
								c = "f";
								size = 0;
								String[] temp = string.split("/");
								for(String t:temp)
								{
									String[] tempTemp = t.split("\\.");
									size = size + tempTemp[1].length();
								}
								size++;
							}
							else
								c = "d";
							align = "";
						}
						
						pw.print("%"+align+String.valueOf(size)+c+" ");
					}
					pw.print("\"");
					for(String s:query.selectAtt)
					{
						if(s.contains("/"))
							pw.print(",x");
						else
							pw.print(","+s);
					}
					pw.print(");\n");
					pw.println("\t\t\t\tSystem.out.println();");
					
					pw.println("\t\t\t}");
					
					pw.println("\t\t\ttry{");
					pw.println("\t\t\t\tconnect.close(); stmt.close(); rs.close();");
					pw.println("\t\t\t}catch(Exception e){");
					pw.println("\t\t\t\te.printStackTrace();");
					pw.println("\t\t\t}");
					
					//---------------------------------------------------------------------------------------
					pw.println("\t\t}catch(Exception e) {");
					pw.println("\t\t\tSystem.out.println(\"Oops!Error Occurs...\");");
					pw.println("\t\t\te.printStackTrace();");
					pw.println("\t\t}");
					
					
					pw.println("\t}");
					pw.println("}");
					
					pw.flush();
				}
			}
		}catch(Exception e)
		{
			System.out.println("Failed Create File!");
			e.printStackTrace();
		}
	}
	
}
		
