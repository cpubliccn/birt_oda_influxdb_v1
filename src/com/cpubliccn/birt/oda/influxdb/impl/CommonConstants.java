package com.cpubliccn.birt.oda.influxdb.impl;

public final class CommonConstants
{
	public static final String DELIMITER_COMMA = ","; //$NON-NLS-1$
	public static final String DELIMITER_SPACE = " "; //$NON-NLS-1$
	public static final String DELIMITER_DOUBLEQUOTE = "\""; //$NON-NLS-1$
	public static final String KEYWORD_SELECT = "SELECT"; //$NON-NLS-1$
	public static final String KEYWORD_FROM = "FROM"; //$NON-NLS-1$
	public static final String KEYWORD_AS = "AS"; //$NON-NLS-1$
	public static final String KEYWORD_ASTERISK = "*";//$NON-NLS-1$
	public static final String DRIVER_NAME = "ODA CSV FILE DRIVER";//$NON-NLS-1$
	public static final int MaxConnections = 0;
	public static final int MaxStatements = 0;

	public static final String CONN_HTTP_URL = "HTTPURL"; //$NON-NLS-1$
	public static final String CONN_DBNAME = "DBNAME"; //$NON-NLS-1$
	public static final String CONN_DBUSER = "DBUSER";
	public static final String CONN_DBPASS = "DBPASS";

	public static final String PRODUCT_VERSION = "3.0"; //$NON-NLS-1$
	
	/**
	 * Private contructure which ensure the non-instantiatial of the class
	 *  
	 */
	private CommonConstants( )
	{
	}

}