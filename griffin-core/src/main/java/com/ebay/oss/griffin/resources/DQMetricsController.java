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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.error.BarkWebException;
import com.ebay.oss.griffin.error.ErrorMessage;
import com.ebay.oss.griffin.service.DQMetricsService;
import com.ebay.oss.griffin.service.DqModelService;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;

@Component
// @Scope("request")
@Path("/metrics")
public class DQMetricsController {

	//

	@Autowired
	private DQMetricsService dqMetricsService;

	@Autowired
	private DqModelService dqModelService;

	@POST
	@Path("/")
	@Consumes({ "application/json" })
	public void insertMetadata(@Valid DqMetricsValue dq) {
		if (dq != null) {
			ErrorMessage err = dq.validate();
			if (err != null) {

				throw new BarkWebException(Response.Status.BAD_REQUEST.getStatusCode(),
					err.getMessage());
			}
		}
		
		try{
			dqMetricsService.insertMetadata(dq);
		}catch(BarkDbOperationException e){
			throw new BarkWebException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
					e.getMessage());
		}
		

	}

	@GET
	@Path("/{asset_id}/latest")
	@Produces(MediaType.APPLICATION_JSON)
	public DqMetricsValue getLatestMetricsValueById(
			@PathParam("asset_id") String assetId) {
		return dqMetricsService.getLatestlMetricsbyId(assetId);
	}

	@GET
	@Path("/heatmap")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SystemLevelMetrics> getHeatMap() {
		return dqMetricsService.heatMap();
	}

	@GET
	@Path("/briefmetrics")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SystemLevelMetrics> getAllBriefMetrics() {
		return dqMetricsService.briefMetrics("all");
	}

	@GET
	@Path("/briefmetrics/{system}")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SystemLevelMetrics> getBriefMetrics(
			@PathParam("system") String system) {
		return dqMetricsService.briefMetrics(system);
	}

	@GET
	@Path("/dashboard")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SystemLevelMetrics> getAllDashboard() {
		List<DqModelVo> models = dqModelService.getAllModles();
		Map<String, String> modelMap = new HashMap<String, String>();

		for (DqModelVo model : models) {
			modelMap.put(
					model.getName(),
					model.getAssetName() == null ? "unknow" : model
							.getAssetName());
		}

		List<SystemLevelMetrics> sysMetricsList = dqMetricsService
				.dashboard("all");
		for (SystemLevelMetrics sys : sysMetricsList) {
			List<AssetLevelMetrics> assetList = sys.getMetrics();
			if (assetList != null && assetList.size() > 0) {
				for (AssetLevelMetrics metrics : assetList) {
					metrics.setAssetName(modelMap.get(metrics.getName()));
				}
			}
		}

		return sysMetricsList;
	}

	@GET
	@Path("/dashboard/{system}")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SystemLevelMetrics> getDashboard(
			@PathParam("system") String system) {
		return dqMetricsService.dashboard(system);
	}

	@GET
	@Path("/mydashboard/{user}")
	public List<SystemLevelMetrics> getAllDashboard(
			@PathParam("user") String user) {
		return dqMetricsService.mydashboard(user);
	}

	@GET
	@Path("/complete/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public AssetLevelMetrics getCompelete(@PathParam("name") String name) {
		return dqMetricsService.oneDataCompleteDashboard(name);
	}

	@GET
	@Path("/brief/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public AssetLevelMetrics getBrief(@PathParam("name") String name) {
		return dqMetricsService.oneDataBriefDashboard(name);
	}

	@GET
	@Path("/refresh")
	public void refreshDQ() {
		dqMetricsService.updateLatestDQList();
	}

	@GET
	@Path("/statics")
	@Produces(MediaType.APPLICATION_JSON)
	public OverViewStatistics getOverViewStats() {
		return dqMetricsService.getOverViewStats();
	}

	@POST
	@Path("/sample")
	@Consumes({ "application/json" })
	public Response insertSamplePath(SampleFilePathLKP sample) {
		dqMetricsService.insertSampleFilePath(sample);
		return Response.status(Response.Status.CREATED).build();
	}

	@GET
	@Path("/sample/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SampleOut> getSampleList(@PathParam("name") String name) {
		return dqMetricsService.listSampleFile(name);
	}

	@GET
	@Path("/download/{path:.+}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadFile(@PathParam("path") final String path,
			@DefaultValue("100") @QueryParam("count") final Long count) {

		StreamingOutput fileStream = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException,
			WebApplicationException {
				try {
					Process pro = Runtime.getRuntime().exec(
							"hadoop fs -cat /" + path);

					BufferedReader buff = new BufferedReader(
							new InputStreamReader(pro.getInputStream()));

					int cnt = 0;
					String line = null;
					while ((line = buff.readLine()) != null) {
						if (cnt < count) {
							output.write((line.getBytes()));
							output.write("\n".getBytes());
							output.flush();
						} else {
							break;
						}
						cnt++;
					}
					buff.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};

		String[] pathTokens = path.split("/");
		String fileName = pathTokens[pathTokens.length-2] + "_" + pathTokens[pathTokens.length-1];

		ResponseBuilder response = Response.ok(fileStream);
		response.header("Content-Disposition",
				"attachment; filename=" + fileName);
		return response.build();

	}

}
