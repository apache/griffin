package com.ebay.oss.griffin.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.service.LoginService;




@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class LoginServiceTest {
	@Autowired
	private LoginService loginService;

	@Test
	public void testLogin(){

		//login success
		String fullname = loginService.login("alex", "alex");
		assertEquals(fullname, "alex");

	}




}
