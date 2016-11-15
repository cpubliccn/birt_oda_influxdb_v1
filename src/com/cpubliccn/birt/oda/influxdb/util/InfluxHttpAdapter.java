package com.cpubliccn.birt.oda.influxdb.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.eclipse.datatools.connectivity.oda.OdaException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by chenzq on 11/15/16.
 */
public class InfluxHttpAdapter implements InfluxAdapter {
    private String host;
    private String dbname;
    private String dbuser;
    private String dbpass;

    private CloseableHttpClient httpClient;

    public InfluxHttpAdapter(String host, String dbname) {
        this(host, dbname, "admin", "admin");
    }

    public InfluxHttpAdapter(String host, String dbname, String dbuser, String dbpass) {
        this.host = host;
        this.dbname = dbname;
        this.dbuser = dbuser;
        this.dbpass = dbpass;
        init();
    }

    private void init() {
        httpClient = HttpClients.createDefault();
    }

    public String[] getColumns(int resultIndex, int seriesIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object[][] executeQuery(String query) throws OdaException  {
        HttpGet httpGet = new HttpGet(host);
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("q", query));
        params.add(new BasicNameValuePair("db", this.dbname));

        String str = null;
        Object resultObject = null;
        try {
            str = EntityUtils.toString(new UrlEncodedFormEntity(params));
            httpGet.setURI(new URI(httpGet.getURI().toString() + "?" + str));
            HttpResponse httpresponse = httpClient.execute(httpGet);
            HttpEntity entity = httpresponse.getEntity();
            resultObject = JSON.parse(EntityUtils.toString(entity));
        } catch (Exception e) {
            throw new OdaException(e);
        } 

        Object[][] dataArray = {{}};
        if (resultObject != null ) {
            Object results = ((Map)resultObject).get("results");
            dataArray = copyDataToArray(results);
        }

        return dataArray;
    }

    private Object[][] copyDataToArray(Object results) {
        Object[][] dataArray = null;
        JSONArray resultList = (JSONArray)results;

        List<List<Object>> valueList = new ArrayList<List<Object>>();
        String[] columns = null;

        boolean tagsCountFlag = false, valueColCountFlag = false;
        int tagCount = 0, valueColCount = 0;
        for (int i=0; i<resultList.size(); i++) {
            JSONArray seriesList = getSeriesArray(i, results);
            int seriesSize = seriesList.size();
            String[] tagNames = null;
            for (int j=0; j<seriesSize; j++) {

                Map seriesMap = (Map)seriesList.get(j);
                if (j == 0) {
                    columns = getSeriesColumns(seriesMap);
                }
                Map<String, String> tags = getSeriesTags(seriesMap);
                if (!tagsCountFlag) {
                    tagCount = tags.size();
                    tagNames = new String[tagCount];
                    Iterator<String> it = tags.keySet().iterator();
                    int tagIndex = 0;
                    while (it.hasNext()) {
                    	String tagName = it.next();
                    	tagNames[tagIndex] = tagName;
                    	tagIndex++;
                    }
                    tagsCountFlag = true;
                }
                List<Object[]> origValueList = getSeriesValues(seriesMap);
                for (Object[] origValueArray: origValueList) {

                    List<Object> rowList = new ArrayList<Object>();
                    for (int tagIndex = 0; tagIndex < tagCount; tagIndex++) {
                        rowList.add(tags.get(tagNames[tagIndex]));
                    }
                    rowList.addAll(Arrays.asList(origValueArray));
                    valueList.add(rowList);

                    if (!valueColCountFlag) {
                        valueColCount = origValueArray.length;
                        valueColCountFlag = true;
                    }
                }
            }
        }

        int rowCount = valueList.size();
        int colCount = tagCount + valueColCount;
        dataArray = new Object[rowCount][colCount];
        for (int row = 0; row < rowCount; row++) {
            List<Object> rowList = valueList.get(row);
            for (int col = 0; col < colCount; col++) {
                Object val = rowList.get(col);
                dataArray[row][col] = val;
            }
        }
        return dataArray;
    }


    private JSONArray getSeriesArray(int resultIndex, Object results) {
        JSONArray resultList = (JSONArray)results;
        Map resultMap = (Map)resultList.get(resultIndex);
        return (JSONArray) resultMap.get("series");
    }

    private Map getSeriesMap(int resultIndex, int seriesIndex, Object results) {
        JSONArray seriesArray = getSeriesArray(resultIndex, results);
        return (Map)seriesArray.get(seriesIndex);
    }

    private String[] getSeriesColumns(Map seriesMap) {
        JSONArray columnArray = (JSONArray) seriesMap.get("columns");
        String[] columns = new String[columnArray.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columnArray.getString(i);
        }
        return columns;
    }

    private String[] getSeriesColumns(int resultIndex, int seriesIndex, Object results) {
        Map seriesMap = this.getSeriesMap(resultIndex, seriesIndex, results);
        return getSeriesColumns(seriesMap);
    }

    private Map<String, String> getSeriesTags(Map seriesMap) {
        Map tagsMap;
        Object tagsObject = seriesMap.get("tags");
        if (tagsObject == null) {
            tagsMap = new HashMap<String, String>();
        } else {
            tagsMap = (Map)tagsObject;
        }
        return tagsMap;
    }

    private Map<String, String> getSeriesTags(int resultIndex, int seriesIndex, Object results) {
        Map seriesMap = this.getSeriesMap(resultIndex, seriesIndex, results);
        return getSeriesTags(seriesMap);
    }

    private List<Object[]> getSeriesValues(Map seriesMap) {
        List<Object[]> valueList = new ArrayList<Object[]>();
        JSONArray seriesValueRows = (JSONArray) seriesMap.get("values");
        int rowCount = seriesValueRows.size();
        for (int i=0; i<rowCount; i++) {
            Object[] row = ((JSONArray)seriesValueRows.get(i)).toArray();
            valueList.add(row);
        }
        return valueList;
    }

    private List<Object[]> getSeriesValues(int resultIndex, int seriesIndex, Object results) {
        Map seriesMap = this.getSeriesMap(resultIndex, seriesIndex, results);
        return getSeriesValues(seriesMap);
    }

    public boolean validateConnection() throws OdaException {
        String query = "SHOW DATABASES";
        Object[][] results = this.executeQuery(query);
        return results.length > 0;
    }

    public static void main(String[] args) {
        String url = "http://10.8.159.107:8086/query";
        String db = "seecloud";
//        String sql = "SHOW DATABASES";
        String sql = "select last(value) from TONGWEB_POOL_maxActive  where time > '2016-11-12 00:00:00' and time < '2016-11-12 23:59:59' group by \"name\", \"subname\"";
        InfluxAdapter influxAdapter = new InfluxHttpAdapter(url, db);

        Object[][] result = null;
        try {
            result = influxAdapter.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int row = 0; row < result.length; row++) {
            for (int col = 0; col < result[0].length; col++) {
                System.out.print(result[row][col] + "\t");
            }
            System.out.println("");
        }
    }

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}

