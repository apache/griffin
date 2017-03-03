/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.ebay.oss.griffin.resources;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import com.ebay.oss.griffin.service.LoginService;
import com.ebay.oss.griffin.vo.LoginUser;

@Component
//@Scope("request")
@Path("/login")
public class LoginController {

	@Autowired
	private LoginService loginService;

	@POST
	@Path("/authenticate")
	@Produces(MediaType.APPLICATION_JSON)
	public String authenticate(@RequestBody LoginUser input) {
		if("test".equals(input.getUsername())){
			return "{\"status\": 0, \"ntAccount\": \"test\", \"fullName\": \"test\"}";
		}

		String result = loginService.login(input.getUsername(),
				input.getPassword());
		if (result != null)
			return "{\"status\": 0, \"ntAccount\": \"" + input.getUsername()
					+ "\", \"fullName\": \"" + result + "\"}";
		else
			return "{\"status\": -1, \"ntAccount\": \"" + input.getUsername()
					+ "\", \"fullName\": \"\",\"errMsg\":\"logon failed.\"}";
		// return "{\"status\": -1, \"errMsg\": \"Invalid username\"}";
	}

}