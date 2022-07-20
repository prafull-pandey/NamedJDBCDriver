package com.pradey.NamedJDBCDriver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NamedPreparedStatement {

	private PreparedStatement preparedStatement;
	private Connection connection;
	private Map<String,List<Integer>> indexListMap;
	private Map<String,int[]> indexMap;

	public NamedPreparedStatement(Connection connection, String query) throws SQLException {
		this.connection=connection;
		indexListMap=new HashMap<>();
		String parsedQuery=createSqlQuery(query);
		this.preparedStatement=this.connection.prepareStatement(parsedQuery);
	}

	private String createSqlQuery(String query) {
		int length=query.length();
		StringBuffer sqlQuery=new StringBuffer();
		boolean inSingleQuote=false;
		boolean inDoubleQuote=false;
		int index=1;
		int x=0;
		String y= "prafull";
		String z="This is my added code";

		for(int i=0;i<length;i++) {
			char ch =query.charAt(i);
			if(inSingleQuote) {
				if (ch=='\'') {
					inSingleQuote=false;
				}
			}else if(inDoubleQuote) {
				if (ch=='\"') {
					inDoubleQuote=false;
				}
			}else {
				if (ch=='\'') {
					inSingleQuote=true;
				}else if (ch=='\"') {
					inDoubleQuote=true;
				}else if(ch==':' && i+1<length && Character.isJavaIdentifierStart(query.charAt(i+1))){
					int j=i+2;
					while(j<length && Character.isJavaIdentifierPart(query.charAt(j))) {
						j++;
					}
					String name=query.substring(i+1,j);
					ch='?';
					i+=name.length();

					List<Integer> indexList=(List<Integer>)indexListMap.get(name);
					if(indexList==null) {
						indexList=new LinkedList<Integer>();
						indexListMap.put(name, indexList);
					}
					indexList.add(new Integer(index));
					index++;
				}
			}
			sqlQuery.append(ch);
		}
		
		for (Map.Entry<String,List<Integer>> entry : indexListMap.entrySet())  {
			int[] indexes=entry.getValue().stream().mapToInt(k->k).toArray();
			indexMap.put(entry.getKey(), indexes);
			}

		return sqlQuery.toString();
	}

	private int[] getIndexes(String name) {
		int[] indexes=(int[])indexMap.get(name);
		if(indexes==null) {
			throw new IllegalArgumentException("Parameter not found: "+name);
		}
		return indexes;
	}

	public void setObject(String name, Object value) throws SQLException {
		int[] indexes=getIndexes(name);
		for(int i=0; i < indexes.length; i++) {
			preparedStatement.setObject(indexes[i], value);
		}
	}
	public void setString(String name, String value) throws SQLException {
		int[] indexes=getIndexes(name);
		for(int i=0; i < indexes.length; i++) {
			preparedStatement.setString(indexes[i], value);
		}
	}
	public void setInt(String name, int value) throws SQLException {
		int[] indexes=getIndexes(name);
		for(int i=0; i < indexes.length; i++) {
			preparedStatement.setInt(indexes[i], value);
		}
	}
	public void setLong(String name, long value) throws SQLException {
		int[] indexes=getIndexes(name);
		for(int i=0; i < indexes.length; i++) {
			preparedStatement.setLong(indexes[i], value);
		}
	}
	public void setTimestamp(String name, Timestamp value) throws SQLException 
	{
		int[] indexes=getIndexes(name);
		for(int i=0; i < indexes.length; i++) {
			preparedStatement.setTimestamp(indexes[i], value);
		}
	}
	public PreparedStatement getPreparedStatement() {
		return preparedStatement;
	}
	public boolean execute() throws SQLException {
		return preparedStatement.execute();
	}
	public ResultSet executeQuery() throws SQLException {
		return preparedStatement.executeQuery();
	}
	public int executeUpdate() throws SQLException {
		return preparedStatement.executeUpdate();
	}
	public void close() throws SQLException {
		preparedStatement.close();
	}
	public void addBatch() throws SQLException {
		preparedStatement.addBatch();
	}
	public int[] executeBatch() throws SQLException {
		return preparedStatement.executeBatch();
	}
}
