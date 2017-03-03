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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
//import org.springframework.validation.annotation.Validated;



















import com.ebay.oss.griffin.common.HDFSUtils;
import com.ebay.oss.griffin.common.Pair;
import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.ScheduleType;
import com.ebay.oss.griffin.domain.SystemType;
import com.ebay.oss.griffin.domain.ValidityType;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.repo.DataAssetRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.vo.DataAssetIndex;
import com.ebay.oss.griffin.vo.DataAssetInput;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelBasicInputNew;
import com.ebay.oss.griffin.vo.ModelExtraInputNew;
import com.ebay.oss.griffin.vo.ModelInput;
import com.ebay.oss.griffin.vo.PlatformMetadata;
import com.ebay.oss.griffin.vo.SystemMetadata;
import com.mongodb.DBObject;

@Service
public class DataAssetServiceImpl implements DataAssetService {

	private static Logger logger = LoggerFactory.getLogger(DataAssetServiceImpl.class);

	 @Autowired
	 private DataAssetRepo dataAssetRepo;

	 @Autowired
	 private DqModelRepo dqModelRepo;
	 
	@Autowired
	private DqModelService dqModelService;

	@Autowired
	private Environment env;

	// FIXME ???
	public static List<DqModelVo> allModels;

	// FIXME ???
	public static HashMap<String, String> thresholds;

	@Override
	public List<DataAsset> getAllDataAssets() {
		return dataAssetRepo.getAll();
	}

	protected DataAsset ofEntity(DBObject o) {
	    return new DataAsset(o);
	}
    @Override
	public int createDataAsset(DataAssetInput input)
			throws BarkDbOperationException {

		DataAsset da = new DataAsset();

		List<Pair> queryList = new ArrayList<Pair>();
		queryList.add(new Pair("assetName", input.getAssetName()));
		queryList.add(new Pair("assetType", input.getAssetType()));
		queryList.add(new Pair("system", input.getSystem()));

		List<Pair> updateValues = new ArrayList<Pair>();
		updateValues.add(new Pair("schema", input.getSchema()));
		updateValues.add(new Pair("platform", input.getPlatform()));
		updateValues
		.add(new Pair("assetHDFSPath", input.getAssetHDFSPath()));
		updateValues.add(new Pair("owner", input.getOwner()));

		DBObject item = dataAssetRepo.getByCondition(queryList);

		if (item != null) {
			throw new BarkDbOperationException("Record already existing");
		}

		String hdfsPath = input.getAssetHDFSPath();
		
		String[] subs = hdfsPath.split("/");
		
		StringBuilder sb = new StringBuilder();
		
		for (String s : subs) {
			if(!s.contains("[")){
				sb.append("/");
				sb.append(s);	
			}
		}
		
		hdfsPath = sb.toString().replaceFirst("/", "");
		
		
		logger.info("HDFS Path: " + hdfsPath);

		if("prod".equals(env.getProperty("env"))){//in prod environment, need to validate the hdfs path

			if (!HDFSUtils.checkHDFSFolder(hdfsPath)) {


				throw new BarkDbOperationException(
						"Hdfs Path is invalid, please input valid hdfs path!");
			}
		}

		try {

			long seq = dataAssetRepo.getNextId();
			da.set_id(seq);
			da.setAssetName(input.getAssetName());
			da.setAssetType(input.getAssetType());
			da.setPlatform(input.getPlatform());
			da.setSystem(input.getSystem());
			da.setAssetHDFSPath(input.getAssetHDFSPath());
			da.setSchema(input.getSchema());
			da.setOwner(input.getOwner());
			da.setPartitions(input.getPartitions());
			da.setTimestamp(new Date());

			logger.debug("log: new record inserted" + seq);
			dataAssetRepo.save(da);

			//create new total count dqmodel
			{
				ModelInput tempCountModel = new ModelInput();
				ModelBasicInputNew basic = new ModelBasicInputNew();
				ModelExtraInputNew extra = new ModelExtraInputNew();
				basic.setDataaset(input.getAssetName());
				basic.setDataasetId(seq);
				basic.setDesc("Count for " + input.getAssetName());
				basic.setEmail(input.getOwner() + "@ebay.com");
				basic.setName("TotalCount_" + input.getAssetName());
				basic.setOwner(input.getOwner());
				basic.setScheduleType(ScheduleType.DAILY);
				basic.setStatus(ModelStatus.DEPLOYED);
				basic.setSystem(SystemType.indexOf(input.getSystem()));
				basic.setType(ModelType.VALIDITY);
				extra.setVaType(ValidityType.TOTAL_COUNT);
				extra.setColumn("wholeDataSet");
				extra.setSrcDataSet(input.getAssetName());
				extra.setSrcDb(input.getPlatform());
				tempCountModel.setBasic(basic);
				tempCountModel.setExtra(extra);
				dqModelService.newModel(tempCountModel);
			}

			return 0;

		} catch (Exception e) {
			throw new BarkDbOperationException(
					"Failed to create a new data asset", e);
		}

	}

	@Override
	public void updateDataAsset(DataAssetInput input)
			throws BarkDbOperationException {
		try {

			DataAsset da = new DataAsset();

			List<Pair> queryList = new ArrayList<Pair>();
			queryList.add(new Pair("assetName", input.getAssetName()));
			queryList.add(new Pair("assetType", input.getAssetType()));
			queryList.add(new Pair("system", input.getSystem()));

			List<Pair> updateValues = new ArrayList<Pair>();
			updateValues.add(new Pair("schema", input.getSchema()));
			updateValues.add(new Pair("platform", input.getPlatform()));
			updateValues.add(new Pair("assetHDFSPath", input
					.getAssetHDFSPath()));
			updateValues.add(new Pair("owner", input.getOwner()));

			DBObject item = dataAssetRepo.getByCondition(queryList);
			if (item == null) {
				throw new BarkDbOperationException( "The data asset doesn't exist");
			} 

			da.setAssetName(input.getAssetName());
			da.setAssetType(input.getAssetType());
			da.setPlatform(input.getPlatform());
			da.setSystem(input.getSystem());
			da.setAssetHDFSPath(input.getAssetHDFSPath());
			da.setSchema(input.getSchema());
			da.setOwner(input.getOwner());
			da.setPartitions(input.getPartitions());
			da.setTimestamp(new Date());

			logger.warn( "log: updated record, id is: "
			                + (long) Double.parseDouble(item.get("_id").toString()));
			da.set_id(new Long((long) Double.parseDouble(item.get("_id")
			                .toString())));
			dataAssetRepo.update(da, item);
		} catch (Exception e) {
			throw new BarkDbOperationException("Failed to update data asset", e);
		}

	}

	@Override
	public DataAsset getDataAssetById(Long id) throws BarkDbOperationException {
		return dataAssetRepo.getById(id);
	}

	@Override
	public List<PlatformMetadata> getSourceTree() {

		List<PlatformMetadata> alltree = new ArrayList<PlatformMetadata>();

		List<DataAsset> allAssets = dataAssetRepo.getAll();

		 
		for (String platform : getAllPlatforms(allAssets)) {

			PlatformMetadata plat = new PlatformMetadata();
			plat.setPlatform(platform);

			
			List<SystemMetadata> d = new ArrayList<SystemMetadata>();

			for (String dataset : getSystemsByPlatform( platform, allAssets)) {

				SystemMetadata db = new SystemMetadata();
				db.setName(dataset);

				List<DataAssetIndex> assets = new ArrayList<DataAssetIndex>();

				Map<Long, String> tableB = getAssetsBySystem(platform, dataset, allAssets);
				for (Entry<Long, String> entry : tableB.entrySet()) {
					DataAssetIndex dai = new DataAssetIndex();
					dai.setId(entry.getKey());
					dai.setName(entry.getValue());

					assets.add(dai);
				}

				db.setAssets(assets);
				d.add(db);

			}

			plat.setSystems(d);
			alltree.add(plat);

		}

		return alltree;

	}

	private Set<String> getAllPlatforms(List<DataAsset> records) {
		Set<String> p = new LinkedHashSet<>();
		for (DataAsset o : records) {
			p.add(o.getPlatform());
		}
		return p;
	}

	public Set<String> getSystemsByPlatform(String platform, List<DataAsset> records) {
		Set<String> p = new HashSet<String>();
		for (DataAsset o : records) {
			if (o.getPlatform().equals(platform)) {
				p.add(o.getSystem());
			}
		}
		return p;

	}

	public Map<Long, String> getAssetsBySystem(String platform, String system, List<DataAsset> records) {
		Map<Long, String> p = new HashMap<Long, String>();
		for (DataAsset o : records) {
			if (o.getPlatform().equals(platform) && o.getSystem().equals(system)) {
				p.put(o.get_id(), o.getAssetName());
			}
		}
		return p;

	}

	@Override
	public void removeAssetById(Long id) throws BarkDbOperationException {
		try {
			removeRelatedModels(id);

			dataAssetRepo.delete(id);
		} catch (Exception e) {
			throw new BarkDbOperationException("Failed to delete data asset with id of '" + id + "'", e);
		}

	}

	//remove all the models related with the given data asset
	private int removeRelatedModels(Long dataAssetId) {
		try {
			DataAsset da = dataAssetRepo.getById(dataAssetId);
			if (da != null) {
				//delete all the accuracy models with this given source asset
				List<DqModel> models = dqModelRepo.getByDataAsset(da);
				for(DqModel each : models) {
					dqModelService.deleteModel(each.getModelName());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

}
