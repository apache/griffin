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
package com.ebay.oss.griffin.error;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.springframework.stereotype.Component;

@Provider
@Component
public class BarkWebExceptionMapper implements
ExceptionMapper<BarkWebException> {

	@Override
	public Response toResponse(BarkWebException ex) {
		int status = ex.getStatus();
		if (status == 0) {
			status = 500;
		}

		return Response.status(status)
				.entity(new ErrorMessage(status, ex.getMessage()))
				.type(MediaType.APPLICATION_JSON) // this has to be set to get
				// the generated JSON
				.build();
	}

}
