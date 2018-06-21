package org.apache.griffin.core.job;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.tools.ant.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileExistPredicatorTest {
	
	static String fileName = "_SUCCESS";
	static String rootPath = "/tmp/";
	
	@BeforeClass
	public static void mkFile() throws IOException{
		File file = new File(rootPath + fileName);
		if(!file.exists()){
			file.createNewFile();
		}
	}
	
	@AfterClass
	public static void deleteFile(){
		File file = new File(rootPath + fileName);
		if(file.exists()){
			FileUtils.delete(file);
		}
	}
	
	@Test(expected = NullPointerException.class)
	public void test_predicate_null() throws IOException{
		SegmentPredicate predicate = new SegmentPredicate();
		predicate.setConfig("test config");
		
		Map<String, Object> configMap = new HashMap<>();	
		predicate.setConfigMap(configMap);
		
		FileExistPredicator predicator = new FileExistPredicator(predicate);
		assertTrue(predicator.predicate());
	}
	
	@Test
	public void test_predicate() throws IOException{
		SegmentPredicate predicate = new SegmentPredicate();
		predicate.setConfig("test config");
		
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("path", fileName);
		configMap.put("root.path", rootPath);
		
		predicate.setConfigMap(configMap);
		
		FileExistPredicator predicator = new FileExistPredicator(predicate);
		assertTrue(predicator.predicate());
		
		configMap.put("path", "fileName");
		predicate.setConfigMap(configMap);
		assertFalse(predicator.predicate());
		
	}
}
