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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.error.BarkWebException;
import com.ebay.oss.griffin.service.DataAssetService;
import com.ebay.oss.griffin.service.NotificationService;
import com.ebay.oss.griffin.vo.DataAssetInput;
import com.ebay.oss.griffin.vo.NotificationRecord;
import com.ebay.oss.griffin.vo.PlatformMetadata;
import com.sun.jersey.api.Responses;

@Component
//@Scope("request")
@Path("/dataassets")
public class DataAssetController {

	@Autowired
	private DataAssetService dataAssetService;
	@Autowired
	private NotificationService notificationService;

	/**
	 * Get data asset metadata
	 *
	 * @return
	 */
	@GET
	@Path("/metadata")
	@Produces(MediaType.APPLICATION_JSON)
	public List<PlatformMetadata> getSrcTree() {
		return dataAssetService.getSourceTree();
	}

	/**
	 * Register a new data asset
	 *
	 * @param input
	 * @return
	 * @throws BarkWebException
	 */
	@POST
	@Path("/")
	public void registerNewDataAsset(DataAssetInput input)
			throws BarkWebException {
		try {
			dataAssetService.createDataAsset(input);
			notificationService.insert(new NotificationRecord(new Date()
			.getTime(), input.getOwner(), "create", "dataasset", input
			.getAssetName()));
			//			return Response.status(Response.Status.CREATED).build();
		} catch (BarkDbOperationException e) {

			throw new BarkWebException(
					Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
					e.getMessage());
		}

	}

	/**
	 * Get all data asset list
	 *
	 * @return
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public List<DataAsset> getAssetList() {
		return dataAssetService.getAllDataAssets();
	}

	/**
	 * Get one data asset by id
	 *
	 * @return
	 * @throws BarkWebException
	 */
	@GET
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public DataAsset getOneAsset(@PathParam("id") Long id)
			throws BarkWebException {
		DataAsset da = dataAssetService.getDataAssetById(id);
		if (da == null) {
			throw new BarkWebException(Responses.NOT_FOUND,
					"The asset you are looking for doesn't exist");
		} else {
			return da;
		}
	}

	/**
	 * Remove a data asset by id
	 *
	 * @param id
	 * @return
	 */
	@DELETE
	@Path("/{id}")
	public void removeAssetById(@PathParam("id") Long id) throws BarkWebException {

		try {
			dataAssetService.removeAssetById(id);

		} catch (BarkDbOperationException e) {
			throw new BarkWebException(
					Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
					e.getMessage());
		}

	}

	/**
	 * Update a new data asset
	 *
	 * @param input
	 * @return
	 */
	@PUT
	@Path("/")
	public void updateDataAsset(DataAssetInput input) throws BarkWebException{

		try {
			dataAssetService.updateDataAsset(input);

		} catch (BarkDbOperationException e) {
			throw new BarkWebException(
					Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
					e.getMessage());
		}
	}

}