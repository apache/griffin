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

import java.util.Date;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.error.BarkWebException;
import com.ebay.oss.griffin.error.ErrorMessage;
import com.ebay.oss.griffin.service.DqModelService;
import com.ebay.oss.griffin.service.NotificationService;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelInput;
import com.ebay.oss.griffin.vo.NotificationRecord;
import com.sun.jersey.api.Responses;

@Component
// @Scope("request")
@Path("/models")
public class DQModelController {

	@Autowired
	private DqModelService dqModelService;

	@Autowired
	private NotificationService notificationService;

	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public List<DqModelVo> allModels() {
		return dqModelService.getAllModles();
	}

	@GET
	@Path("/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public ModelInput getModel(@PathParam("name") String name) {

		ModelInput model = dqModelService.getModelByName(name);
		if (model == null) {
			throw new BarkWebException(Responses.NOT_FOUND,
					"The model you are looking for doesn't exist!");
		}
		return model;
	}

	@GET
	@Path("/enableModel/{name}")
	public void enableModel(@PathParam("name") String name) {
		dqModelService.enableSchedule4Model(dqModelService.getGeneralModel(name));
	}


	@DELETE
	@Path("/{name}")
	public void deleteModel(@PathParam("name") String name) throws BarkDbOperationException {

		try {
			dqModelService.deleteModel(name);
		} catch (BarkDbOperationException e) {
			throw new BarkWebException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
		}
	}

	@POST
	@Path("/")
	public void newModel(@RequestBody ModelInput input) {
		if (input != null) {

			ErrorMessage err = input.validate();
			if (err != null) {
				throw new BarkWebException(Response.Status.BAD_REQUEST.getStatusCode(), err.getMessage());
			}

			try {
				dqModelService.newModel(input);
				notificationService.insert(new NotificationRecord(new Date().getTime(), input.getBasic().getOwner(),
						"create", "model", input.getBasic().getName()));

			} catch (BarkDbOperationException e) {
				throw new BarkWebException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
			}
		}
	}

}
