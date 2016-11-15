package com.cpubliccn.birt.oda.influxdb.util;

import org.eclipse.datatools.connectivity.oda.OdaException;

public interface InfluxAdapter {
	public String[] getColumns(int resultIndex, int seriesIndex);
	public Object[][] executeQuery(String query) throws OdaException;
	public boolean validateConnection() throws OdaException;
	public void close();
}
