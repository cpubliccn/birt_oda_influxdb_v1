package com.cpubliccn.birt.oda.influxdb.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InfluxDataParser {
	@SuppressWarnings({ "rawtypes" })
	private ArrayList dataList;
	
	@SuppressWarnings("rawtypes")
	public InfluxDataParser(Map rawData) {
		this.dataList = (ArrayList)rawData.get("results");
	}
	
	@SuppressWarnings({ "rawtypes" })
	private ArrayList getSeriesList(int resultIndex) {
		Map seriesListMap = (Map)dataList.get(resultIndex);
		ArrayList seriesList = (ArrayList)seriesListMap.get("series");
		return seriesList;
	}	
	
	@SuppressWarnings({ "rawtypes" })
	private Map getSeriesMap(int resultIndex, int seriesIndex) {
		ArrayList seriesList = getSeriesList(resultIndex);
		if (seriesList == null || seriesList.isEmpty()) {
			return new HashMap();
		}
		Map seriesMap = (Map)seriesList.get(seriesIndex);
		return seriesMap;
	}
	
	public int getSeriesCount(int resultIndex) {
		return getSeriesList(resultIndex).size();
	}
	
	public String getSeriesName(int resultIndex, int seriesIndex) {
		return (String)getSeriesMap(seriesIndex, resultIndex).get("name");
	}
	
	
	@SuppressWarnings({ "rawtypes" })
	public String[] getSeriesTags(int resultIndex, int seriesIndex) {
		return this.getTagsFromSeries((Map)getSeriesMap(seriesIndex, resultIndex));
	}	
	
	@SuppressWarnings("rawtypes")
	public String getSeriesTagValue(int resultIndex, int seriesIndex, String tagName) {
		Map seriesMap = this.getSeriesMap(resultIndex, seriesIndex);
		return this.getTagValueFromSeries(seriesMap, tagName);
	}
	
	@SuppressWarnings({ "rawtypes" })
	public String[] getSeriesColumns(int resultIndex, int seriesIndex) {
		return this.getColumnsFromSeries((Map)getSeriesMap(seriesIndex, resultIndex));
	}	
	
	public String[] getResultColumns(int resultIndex, int seriesIndex) {
		String[] tags = this.getSeriesTags(resultIndex, seriesIndex);
		String[] origColumns = this.getSeriesColumns(resultIndex, seriesIndex);
		
		String[] columns = new String[tags.length + origColumns.length];
		for (int i = 0; i < tags.length; i++) {
			columns[i] = tags[i];
		}
		for (int j = 0; j < origColumns.length; j++) {
			columns[tags.length + j] = origColumns[j];
		}
		return columns;
	}
	
	@SuppressWarnings("rawtypes")
	public Object[][] getRowSet(int resultIndex) {
		List<Object[][]> rsList = new ArrayList<Object[][]>();
		ArrayList seriesList = this.getSeriesList(resultIndex);
		if (seriesList == null || seriesList.size() == 0) {
			Object[][] result = {{}};
			return result;
		}
		int rowCount = 0;
		for (Object seriesMap : seriesList) {;
			Object[][] seriesValues = this.getRowSetFromSeries((Map)seriesMap);
			rowCount += seriesValues.length;
			rsList.add(seriesValues);
		}
		Object[][] rowSet = new Object[rowCount][rsList.get(0).length];
		int i = 0;
		for (Object[][] values : rsList) {
			for (int row = 0; row < values.length; row++) {
				rowSet[i] = values[row];
				i++;
			}
		}
		return rowSet;
	}
	
	@SuppressWarnings("rawtypes")
	private String[] getTagsFromSeries(Map seriesMap) {
		Map tagsMap = (Map)seriesMap.get("tags");
		if (tagsMap == null) {
			return new String[0];
		}
		String[] tags = new String[tagsMap.size()];
		int i = 0;
		Iterator it = tagsMap.keySet().iterator();
		while (it.hasNext()) {
			tags[i] = String.valueOf(it.next());
			i++;
		}	
		return tags;
	}
	
	@SuppressWarnings("rawtypes")
	private String getTagValueFromSeries(Map seriesMap, String tagName) {
		Map tagsMap = (Map)seriesMap.get("tags");
		return convertString(String.valueOf(tagsMap.get(tagName)));
	}
	
	@SuppressWarnings("rawtypes")
	private String[] getColumnsFromSeries(Map seriesMap) {
		ArrayList columnsArray = (ArrayList)seriesMap.get("columns");
		String[] columns = new String[columnsArray.size()];
		int i = 0;
		for (Object column : columnsArray) {
			columns[i] = (String)column;
			i++;
		}
		return columns;
	}	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object[][] getRowSetFromSeries(Map seriesMap) {
		Object[][] rowSet;
		String[] tags = this.getTagsFromSeries(seriesMap);
		String[] columns = this.getColumnsFromSeries(seriesMap);
		
		ArrayList<ArrayList> valueList = (ArrayList<ArrayList>)seriesMap.get("values");
		int rowCount = valueList.size();
		int columnCount = tags.length + columns.length;
		
		rowSet = new Object[rowCount][columnCount];
		for (int row = 0; row < rowCount; row++){
			for (int tag = 0; tag < tags.length; tag++) {
				rowSet[row][tag] = this.getTagValueFromSeries(seriesMap, tags[tag]);
			}
			for (int column = 0; column < columns.length; column++) {
				rowSet[row][tags.length + column] = valueList.get(row).get(column);
			}
		}		
		
		return rowSet;
	}
	
	private String convertString(String source) {
//		return source;
		return this.convertString(source, "iso8859-1", "utf-8");
	}
	
	private String convertString(String source, String origChartset, String destCharset) {
		String result = null;
		try {
			result = new String(source.getBytes(origChartset), destCharset);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
		
}
