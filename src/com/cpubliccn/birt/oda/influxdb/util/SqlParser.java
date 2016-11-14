package com.cpubliccn.birt.oda.influxdb.util;

import java.util.ArrayList;
import java.util.List;

public class SqlParser {
	private static final String KEYWORD_SELECT = "SELECT";
	private static final String KEYWORD_AS = "AS";
	private static final String KEYWORD_FROM = "FROM";
	private static final String KEYWORD_WHERE = "WHERE";
	private static final String KEYWORD_GROUPBY = "GROUP BY";
	private static final String KEYWORD_ORDERBY = "ORDER BY";
	
	private String sqlString;
	private String[] columns = null;
	
	public SqlParser(String sqlString) {
		this.sqlString = sqlString.trim();
		parseSql();
	}
	
	public String[] getFields() {
		return this.columns;
	}
	
	private void parseSql() {
		String[] selectParts = null, fromParts = null, whereParts = null;
		String selectPart = null, fromPart = null, wherePart = null, groupPart = null;
		List<String> selectColumns = null, groupColumns = new ArrayList<String>();;
		selectParts = sqlString.split("[Ff][Rr][Oo][Mm]");
		selectPart = selectParts[0];
		selectColumns = this.parseSelect(selectPart);
		fromParts = selectParts[1].split("[Ww][Hh][Ee][Rr][Ee]");
		fromPart = fromParts[0];
		whereParts = null;
		if (fromParts.length == 2) {
			whereParts = fromParts[1].split("[Gg][Rr][Oo][Uu][Pp]\\s+[Bb][Yy]");
			wherePart = whereParts[0];
			if (whereParts.length == 2) {
				groupPart = whereParts[1].split("[Oo][Rr][Dd][Ee][Rr]\\s+[Bb][Yy]")[0];
				groupColumns = parseGroupBy(groupPart);
			}
		}
		
		int groupColumnSize = groupColumns.size(), selectColumnSize = selectColumns.size();
		columns = new String[groupColumnSize + 1 + selectColumnSize];
		for (int groupColIdx = 0; groupColIdx<groupColumnSize; groupColIdx++){
			columns[groupColIdx] = groupColumns.get(groupColIdx);
		}
		columns[groupColumnSize] = "time";
		for (int selectColIdx = 0; selectColIdx < selectColumnSize; selectColIdx++){
			columns[selectColIdx+groupColumnSize+1] = selectColumns.get(selectColIdx);
		}
	}
	
	private List<String> parseSelect(String selectClause) {
		String[] children = selectClause.split(",");
		List<String> columnList = new ArrayList<String>();
		for (int i = 0; i < children.length; i++) {
			String[] parts = children[i].split("\\s");
//			int len = parts.length;
//			column = parts[len -1];
			columnList.add(parts[parts.length - 1].trim());;
		}
		//TODO need to consider *
		return columnList;
	}
	
	private List<String> parseGroupBy(String groupbyClause) {
		String[] children = groupbyClause.split(",");
		List<String> columnList = new ArrayList<String>();
		for (int i = 0; i < children.length; i++) {
			String child = children[i].trim();
			if (i == 0) {
				String[] firstGroup = child.split("\\s");
				columnList.add(firstGroup[firstGroup.length - 1].replaceAll("\\\"", ""));
			} else if (child.matches("time\\(\\d\\w\\)")){
				continue;
			} else {
				columnList.add(child.replaceAll("\\\"", ""));
			}
		}
		
		return columnList;
	}
	
	public static void main(String[] args) {
		//String queryString = "select max(value) as maxValue, min(value) as minValue, mean(value) as avgValue from LZ_CPU_USED where time < now() - 5m and time > now() - 10m group by \"name\", time(1h)";
		//String diskQuery = "select max(value) as maxValue, min(value) as minValue, mean(value) as avgValue from LZ_DISK_USED where time < now() - 5m and time > now() - 10m group by \"name\", \"subname\", time(1h)";
		String diskQuery = "select value  from NT_DISK_TOTAL where time < '2016-11-01 23:59:59' group by \"name\", \"subname\",\"units\" order by time desc limit 1";
		SqlParser sqlParser = new SqlParser(diskQuery);
		String[] fields = sqlParser.getFields();
		for (String field:fields) {
			System.out.println(field);
		}
	}
}
