package Gennerator;

import java.util.ArrayList;

class Query{
	String type;
	String whereClause;
	int numOfGroupVar;
	String havingClause;
	
	ArrayList<String> selectAtt = new ArrayList<String>();
	ArrayList<String> groupAtt = new ArrayList<String>();
	ArrayList<String> groupVar = new ArrayList<String>();
	ArrayList<ArrayList<String>> aggregate = new ArrayList<ArrayList<String>>();

	Query(){}
}
