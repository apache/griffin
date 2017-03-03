package com.ebay.oss.griffin.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HDFSUtils {

	static Logger logger = LoggerFactory.getLogger(HDFSUtils.class);

	public static boolean checkHDFSFolder(String folderPath) {
		
		Process processMoveFolder;
		int result;
		try {
			processMoveFolder = Runtime.getRuntime().exec("hadoop fs -ls " + folderPath);
			
			result = processMoveFolder.waitFor();
			
			if(result == 0) 
			{
				return true;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
}

