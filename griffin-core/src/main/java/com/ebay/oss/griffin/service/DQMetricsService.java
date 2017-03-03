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
package com.ebay.oss.griffin.service;

import java.util.List;

import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;

public interface DQMetricsService {

	public void insertMetadata(DqMetricsValue dq);

	public DqMetricsValue getLatestlMetricsbyId(String assetId);

	public List<SystemLevelMetrics> briefMetrics(String system);

	public void updateLatestDQList();

	public OverViewStatistics getOverViewStats();

	////////////// sample-file
	public List<SampleOut> listSampleFile(String modelName);

//	public void downloadSample(String filePath);

	public void insertSampleFilePath(SampleFilePathLKP samplePath);

	////////////// HeatMap
	public List<SystemLevelMetrics> heatMap();

	///////////// dashboard
	public List<SystemLevelMetrics> dashboard(String system);

	public AssetLevelMetrics oneDataCompleteDashboard(String name);

	public AssetLevelMetrics oneDataBriefDashboard(String name);

	public AssetLevelMetrics metricsForReport(String name);

	public List<SystemLevelMetrics> mydashboard(String user);

}
