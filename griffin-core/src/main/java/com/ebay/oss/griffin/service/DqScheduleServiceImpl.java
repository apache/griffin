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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
//import org.springframework.validation.annotation.Validated;












import com.ebay.oss.griffin.common.Pair;
import com.ebay.oss.griffin.common.ScheduleModelSeperator;
import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DqJob;
import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.DqSchedule;
import com.ebay.oss.griffin.domain.JobStatus;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.PartitionFormat;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.domain.ScheduleType;
import com.ebay.oss.griffin.domain.SystemType;
import com.ebay.oss.griffin.repo.DataAssetRepo;
import com.ebay.oss.griffin.repo.DqJobRepo;
import com.ebay.oss.griffin.repo.DqMetricsRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.repo.DqScheduleRepo;
import com.ebay.oss.griffin.vo.AccuracyHiveJobConfig;
import com.ebay.oss.griffin.vo.AccuracyHiveJobConfigDetail;
import com.ebay.oss.griffin.vo.PartitionConfig;
import com.ebay.oss.griffin.vo.ValidateHiveJobConfig;
import com.google.gson.Gson;
import com.mongodb.DBObject;

@PropertySource("classpath:application.properties")
@Service ("scheduleService")
public class DqScheduleServiceImpl implements DqScheduleService {
	private static Logger logger = LoggerFactory.getLogger(DqScheduleServiceImpl.class);

	public static String resultFile = "_RESULT";
	public static String startFile = "_START";
	public static String finishFile = "_FINISH";
	public static String logFile = "dqjoblog";

	@Autowired
	DQMetricsService dqMetricsService;

	@Autowired
	DqModelService dqModelService;
	
	@Autowired DqModelRepo dqModelRepo;

	@Autowired DqMetricsRepo metricsRepo;

	@Autowired
	private DqScheduleRepo scheduleRepo;

	@Autowired
    private DqJobRepo jobRepo;

	@Autowired
    private DataAssetRepo dataAssetRepo;
	
	@Override
	public void schedulingJobs() {
		logger.info("===========checking new jobs===============");

		createJobToRunBySchedule();

		generateAllWaitingJobsRunningConfigs();

		checkAllJOBSStatus();

		updateModelStatus(ModelStatus.TESTING, ModelStatus.VERIFIED);

		logger.info("===========checking jobs done===============");
	}

    void createJobToRunBySchedule() {
        for (DqSchedule schedule : scheduleRepo.getAll()) {
            long now = new Date().getTime();
            long startTime = schedule.getStarttime();
            if (now < startTime) {
                continue;
            }

            Calendar c = Calendar.getInstance();
            Date date = new Date(startTime);
            c.setTime(date);
            int type = schedule.getScheduleType();
            if (type == ScheduleType.DAILY) {
                c.add(Calendar.DATE, 1);
            } else if (type == ScheduleType.HOURLY) {
                c.add(Calendar.HOUR, 1);
            } else if (type == ScheduleType.WEEKLY) {
                c.add(Calendar.DATE, 7);
            } else if (type == ScheduleType.MONTHLY) {
                c.add(Calendar.MONTH, 1);
            } else {
                continue;
            }
            startTime = c.getTime().getTime();
            schedule.setStarttime(startTime);

            DqJob job = new DqJob();
            job.setModelList(schedule.getModelList());
            job.setStarttime(startTime);
            job.setStatus(0);
            job.setId(schedule.getModelList() + "_" + startTime); // this is the job.id generation logic
            job.setJobType(schedule.getJobType());
            int result = jobRepo.newJob(job);
            if (result == 0) {
                logger.info("===================new model failure");
                continue;
            }

            scheduleRepo.save(schedule);

        }
	}

	String updateHDFSDirTemplateString(String dir,String dateString,String hourString) {
		String result = dir;
		result = result.replaceAll("\\[YYYYMMDD\\]", dateString);
		result = result.replaceAll("\\[YYYY\\-MM\\-DD\\]", dateString.substring(0,4)+"-"+dateString.substring(4,6)+"-"+dateString.substring(6,8));
		result = result.replaceAll("\\[YYYY\\]", dateString.substring(0,4));
		result = result.replaceAll("\\[YY\\]", dateString.substring(2,4));
		result = result.replaceAll("\\[MM\\]", dateString.substring(4,6));
		result = result.replaceAll("\\[DD\\]", dateString.substring(6,8));
		result = result.replaceAll("\\[HH\\]", hourString);
		result = result.replaceAll("\\[yyyymmdd\\]", dateString);
		result = result.replaceAll("\\[yyyy\\-mm\\-dd\\]", dateString.substring(0,4)+"-"+dateString.substring(4,6)+"-"+dateString.substring(6,8));
		result = result.replaceAll("\\[yyyy\\]", dateString.substring(0,4));
		result = result.replaceAll("\\[yy\\]", dateString.substring(3,4));
		result = result.replaceAll("\\[mm\\]", dateString.substring(4,6));
		result = result.replaceAll("\\[dd\\]", dateString.substring(6,8));
		result = result.replaceAll("\\[hh\\]", hourString);
		return result;
	}

	void generateAllWaitingJobsRunningConfigs() {
		try{
			logger.info("===========generating running config===============");
			Properties env = new Properties();
			env.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("application.properties"));
			String environment = env.getProperty("env");

			for(DqJob eachJob : jobRepo.getByStatus(JobStatus.READY)) {
				String jobid = eachJob.getId();
				int jobtype = eachJob.getJobType();
				StringBuffer doneFiles = new StringBuffer();
				StringBuffer runningParameter = new StringBuffer();

				if(jobtype==ModelType.ACCURACY) {
					String modelid = eachJob.getModelList();
					long ts = eachJob.getStarttime();
					Date dt = new Date(ts);
					SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
					String dateString = formatter.format(dt);
					SimpleDateFormat formatter2 = new SimpleDateFormat("HH");
					String hourString = formatter2.format(dt);

					DqModel model = dqModelRepo.findByColumn("modelId", modelid);
					if(model==null) {
						logger.warn( "===================can not find model "+modelid);
						continue;
					}

					String content = model.getModelContent();

					String[] contents = content.split("\\|");
					String srcPlatform = contents[0];
					String srcSystem = contents[1];
					String tgtPlatform = contents[2];
					String tgtSystem = contents[3];

					String[] attributesArray = contents[4].split(";");
					String[] attributes = attributesArray[0].split(",");
					String srcDataset = attributes[0].substring(0,attributes[0].lastIndexOf("."));
					String tgtDataset = attributes[1].substring(0,attributes[1].lastIndexOf("."));

					//					runningParameter.append(System.getProperty("line.separator")+srcPlatform+" "+srcSystem+" "+srcDataset);
					//					runningParameter.append(System.getProperty("line.separator")+tgtPlatform+" "+tgtSystem+" "+tgtDataset);

					List<Pair> queryList = new ArrayList<Pair>();
					queryList.add(new Pair("platform", srcPlatform));
					queryList.add(new Pair("system", srcSystem));
					queryList.add(new Pair("assetName", srcDataset));
					logger.info( "===================find source object "+srcPlatform+" "+srcSystem+" "+srcDataset);
					DBObject srcObj = dataAssetRepo.getByCondition(queryList);
					DataAsset srcAsset = new DataAsset(srcObj);

					List<Pair> queryList2 = new ArrayList<Pair>();
					queryList2.add(new Pair("platform", tgtPlatform));
					queryList2.add(new Pair("system", tgtSystem));
					queryList2.add(new Pair("assetName", tgtDataset));
					logger.info( "===================find target object "+tgtPlatform+" "+tgtSystem+" "+tgtDataset);
					DBObject tgtObj = dataAssetRepo.getByCondition(queryList2);
					DataAsset tgtAsset = new DataAsset(tgtObj);

					doneFiles.append(updateHDFSDirTemplateString(srcAsset.getAssetHDFSPath(),dateString,hourString)
							+System.getProperty("line.separator")
							+updateHDFSDirTemplateString(tgtAsset.getAssetHDFSPath(),dateString,hourString)
							+System.getProperty("line.separator"));
					if(model.getSchedule()==ScheduleType.HOURLY && model.getSystem()==SystemType.BULLSEYE)
					{
						Date dt4be = new Date(ts+3600000);
//						SimpleDateFormat formatter4be = new SimpleDateFormat("yyyyMMdd");
						String dateString4be = formatter.format(dt4be);
//						SimpleDateFormat formatter24be = new SimpleDateFormat("HH");
						String hourString4be = formatter2.format(dt4be);
						doneFiles.append(updateHDFSDirTemplateString(tgtAsset.getAssetHDFSPath(),dateString4be,hourString4be)
								+System.getProperty("line.separator"));
					}


					AccuracyHiveJobConfig config = new AccuracyHiveJobConfig();
					List<AccuracyHiveJobConfigDetail> configDetailList = new ArrayList<AccuracyHiveJobConfigDetail>();
					for(String tempAttribute : attributesArray)
					{
						String[] tempAttributeArray = tempAttribute.split(",");
						String srcColName = tempAttributeArray[0].substring(tempAttributeArray[0].lastIndexOf(".")+1);
						String tgtColName = tempAttributeArray[1].substring(tempAttributeArray[1].lastIndexOf(".")+1);
						configDetailList.add(new AccuracyHiveJobConfigDetail(
								srcAsset.getColId(srcColName), srcColName
								, tgtAsset.getColId(tgtColName), tgtColName
								,tempAttributeArray[3], Boolean.parseBoolean(tempAttributeArray[2].toUpperCase())
								) );
					}
					config.setAccuracyMapping(configDetailList);
					config.setSource(srcAsset.getAssetName());
					config.setTarget(tgtAsset.getAssetName());

					config.setSrcPartitions(getPartitionList(srcAsset, ts));

					List<List<PartitionConfig>> tgtPartitions = new ArrayList<List<PartitionConfig>>();
					tgtPartitions.add(getPartitionList(tgtAsset, ts));
					if(model.getSchedule()==ScheduleType.HOURLY 
					                && model.getSystem()==SystemType.BULLSEYE) {
						tgtPartitions.add(getPartitionList(tgtAsset, ts+3600000));
					}

					config.setTgtPartitions(tgtPartitions);

					Gson gson = new Gson();
					runningParameter.append(gson.toJson(config)+System.getProperty("line.separator"));

				} else if(jobtype==ModelType.VALIDITY) {

					String modelList = eachJob.getModelList();
					long ts = eachJob.getStarttime();
					Date dt = new Date(ts);
					SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
					String dateString = formatter.format(dt);
					SimpleDateFormat formatter2 = new SimpleDateFormat("HH");
					String hourString = formatter2.format(dt);

					List<String> models = new ArrayList<String>();
					if(!modelList.contains(ScheduleModelSeperator.SEPERATOR))
					{
						models.add(modelList);
					}
					else
					{
						models = Arrays.asList(modelList.split(ScheduleModelSeperator.SPLIT_SEPERATOR));
					}


					if(models.size()==0) return;
					logger.debug("+++ model id value: " + models.get(0));
					DqModel model = dqModelRepo.findByColumn("modelId", models.get(0));
					logger.debug("--- model: " + model);
					if(model == null){
						continue ;
					}
					DataAsset srcAsset = dataAssetRepo.getById((long)model.getAssetId());


					doneFiles.append(updateHDFSDirTemplateString(srcAsset.getAssetHDFSPath(),dateString,hourString)
							+System.getProperty("line.separator"));

					ValidateHiveJobConfig config = new ValidateHiveJobConfig(srcAsset.getAssetName());
					config.setTimePartitions(getPartitionList(srcAsset, ts));

					for(String modelname : models) {
						model = dqModelRepo.findByColumn("modelId", modelname);
						if(model==null) {
							logger.warn("===================can not find model "+modelname);
							continue;
						}

						String content = model.getModelContent();
						String[] contents = content.split("\\|");
						String calType = contents[2];
						String calColname = contents[3];

						config.addColumnCalculation(srcAsset.getColId(calColname), calColname, Integer.parseInt(calType));
					}

					Gson gson = new Gson();
					runningParameter.append(gson.toJson(config)+System.getProperty("line.separator"));
				}




				logger.info( "===================="+env.getProperty("job.local.folder")+File.separator+jobid+File.separator+"cmd.txt");

				String dir = env.getProperty("job.local.folder")+File.separator+jobid+File.separator+"cmd.txt";
				createFile(dir);
				File file = new File(dir);
				FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(runningParameter.toString());
				bw.flush();
				bw.close();

				String dir2 = env.getProperty("job.local.folder")+File.separator+jobid+File.separator+"watchfile.txt";
				createFile(dir2);
				File file2 = new File(dir2);
				FileWriter fw2 = new FileWriter(file2.getAbsoluteFile());
				BufferedWriter bw2 = new BufferedWriter(fw2);
				bw2.write(doneFiles.toString());
				bw2.flush();
				bw2.close();

				logger.info("====================create file done");

				if(environment.equals("prod")) {
					String hdfs = env.getProperty("job.hdfs.folder")+"/"+env.getProperty("job.hdfs.runningfoldername");
					Process process1 = Runtime.getRuntime().exec("hadoop fs -mkdir "+hdfs+File.separator+jobid);
					logger.info("====================hadoop fs -mkdir "+hdfs+File.separator+jobid);
					process1.waitFor();;
					Process process2 = Runtime.getRuntime().exec("hadoop fs -put "+dir+" "+hdfs+File.separator+jobid+File.separator);
					logger.info( "====================hadoop fs -put "+dir+" "+hdfs+File.separator+jobid+File.separator);
					process2.waitFor();
					Process process2_1 = Runtime.getRuntime().exec("hadoop fs -put "+dir2+" "+hdfs+File.separator+jobid+File.separator+"_watchfile");
					logger.info("====================hadoop fs -put "+dir2+" "+hdfs+File.separator+jobid+File.separator+"_watchfile");
					process2_1.waitFor();
					Process process3 = Runtime.getRuntime().exec("hadoop fs -touchz "+hdfs+File.separator+jobid+File.separator+"_type_"+jobtype+".done");
					logger.info( "====================hadoop fs -touchz "+hdfs+File.separator+jobid+File.separator+"_type_"+jobtype+".done");
					process3.waitFor();

				}

				//file.delete();
				new File(env.getProperty("job.local.folder")+File.separator+jobid).delete();
				logger.info( "====================delete file done");

				eachJob.setStatus(JobStatus.WAITING);
				jobRepo.update(eachJob);
				logger.info("====================udpate status done");
			}


		} catch(Exception e) {
			logger.error(e.toString(), e);
		}

	}

	List<PartitionConfig> getPartitionList(DataAsset srcAsset, long ts) {
		Date dt = new Date(ts);
		List<PartitionConfig> partitions = new ArrayList<PartitionConfig>();
		List<PartitionFormat> lv1partitions = srcAsset.getPartitions();
		if(lv1partitions!=null)
		{
			for(PartitionFormat tempPartitionFormat : lv1partitions)
			{
				SimpleDateFormat tempFormatter = new SimpleDateFormat(tempPartitionFormat.getFormat());
				String tempdateString = tempFormatter.format(dt);
				partitions.add(new PartitionConfig(tempPartitionFormat.getName(), tempdateString));
			}
		}
		return partitions;
	}

	void checkAllJOBSStatus() {
		try {
			Properties env = new Properties();
			env.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("application.properties"));
			String hdfsbasedir = env.getProperty("job.hdfs.folder");
			String runningfoldername = env.getProperty("job.hdfs.runningfoldername");
			String historyfoldername = env.getProperty("job.hdfs.historyfoldername");
			String failurefoldername = env.getProperty("job.hdfs.failurefoldername");
			String environment = env.getProperty("env");
			String localdir = env.getProperty("job.local.folder");
			if(!environment.equals("prod")) return;

			int result;
			Process processLV1 = Runtime.getRuntime().exec("hadoop fs -ls "+hdfsbasedir+"/"+runningfoldername);
			result = processLV1.waitFor();
			if(result != 0) {
				logger.info("===================="+"hadoop fs -ls "+hdfsbasedir+"/"+runningfoldername+" error");
				return;
			}

			BufferedReader readerLV1 = new BufferedReader(new InputStreamReader(processLV1.getInputStream()));
			String lineLV1;
			int index;
			while ((lineLV1 = readerLV1.readLine()) != null) {
				index = lineLV1.indexOf("/");
				if(index==-1) continue;
				String runningJobFolderLV1Dir = lineLV1.substring(index);
				logger.info("===================checking hdfs folder"+runningJobFolderLV1Dir); 
				Process processLV2 = Runtime.getRuntime().exec("hadoop fs -ls "+runningJobFolderLV1Dir);
				result = processLV2.waitFor();
				if(result != 0)
				{
					logger.warn("===================="+"hadoop fs -ls "+runningJobFolderLV1Dir+" error");
					continue;
				}
				BufferedReader readerLV2 = new BufferedReader(new InputStreamReader(processLV2.getInputStream()));
//				if(readerLV2==null) return;

				String lineLV2;
				int startindi = 0;
				int resultindi = 0;
				int logindi = 0;
				while ((lineLV2 = readerLV2.readLine()) != null) {
					index = lineLV2.indexOf("/");
					if(index==-1) continue;
					String runningJobContentDir = lineLV2.substring(index);
					logger.info("===================checking hdfs folder"+runningJobContentDir);
					if(runningJobContentDir.indexOf(startFile)!=-1)
						startindi = 1;
					else if(runningJobContentDir.indexOf(resultFile)!=-1)
						resultindi = 1;
					else if(runningJobContentDir.indexOf(logFile)!=-1)
						logindi = 1;
				}

				String jobID = runningJobFolderLV1Dir.substring(runningJobFolderLV1Dir.indexOf(runningfoldername)+runningfoldername.length()+1);
				logger.info("===================job id: "+jobID);
				DqJob job = jobRepo.getById(jobID);
				if(job==null) {
					logger.warn("===================no such job: "+job);
					continue;
				}

				if(startindi == 1) {
					logger.info("===================start");
					if(environment.equals("prod")) {
					    job.setJobType(JobStatus.STARTED); // FIXME numeric issue???!!!
					    jobRepo.update(job);
					}
					logger.info("===================udpate job status to started");
					//					Process processChangeStartFile = Runtime.getRuntime().exec("hadoop fs -mv "+runningJobFolderLV1Dir+"/"+startFile+" "+runningJobFolderLV1Dir+"/_RUNNING");
					//					result = processChangeStartFile.waitFor();
				}

				if(resultindi ==1) {
					logger.info("===================finished");

					if(environment.equals("prod")) {
						String historyJobFolderLV1Dir = runningJobFolderLV1Dir.replaceAll(runningfoldername, historyfoldername);
						Process processMoveFolder = Runtime.getRuntime().exec("hadoop fs -mv "+runningJobFolderLV1Dir+" "+historyJobFolderLV1Dir);
						result = processMoveFolder.waitFor();
						if(result != 0)
						{
							logger.warn("===================="+"hadoop fs -mv "+runningJobFolderLV1Dir+" "+historyJobFolderLV1Dir+" error");
							continue;
						}
						logger.info("===================moved to history folder");
						
						logger.info("===================publish metrics.");

						String hdfs = env.getProperty("job.hdfs.folder")+"/"+env.getProperty("job.hdfs.historyfoldername");
						String resultLocalFileDir = localdir+File.separator+jobID+File.separator+resultFile;
						createFile(resultLocalFileDir);
						new File(resultLocalFileDir).delete();
						Process process1 = Runtime.getRuntime().exec("hadoop fs -get "+hdfs+File.separator+jobID+File.separator+resultFile+" "+resultLocalFileDir);
						logger.info("====================hadoop fs -get "+hdfs+File.separator+jobID+File.separator+resultFile+" "+resultLocalFileDir);
						process1.waitFor();

						File rFile = new File(resultLocalFileDir);
						BufferedReader reader = new BufferedReader(new FileReader(rFile));
						String resultValue = reader.readLine();
						IOUtils.closeQuietly(reader);

						String metricsNames = jobID.substring(0, jobID.lastIndexOf("_"));
						List<String> metricsNameArray = new ArrayList<String>();
						if(!metricsNames.contains(ScheduleModelSeperator.SEPERATOR))
						{
							metricsNameArray.add(metricsNames);
						} else {
							metricsNameArray = Arrays.asList(metricsNames.split(ScheduleModelSeperator.SPLIT_SEPERATOR));
						}

						for(String metricsName : metricsNameArray) {
							DqModel model = dqModelRepo.findByName(metricsName);
							if(model.getModelType() == ModelType.ACCURACY) {
								float floatResultValue = -1;
								long ts = -1;
								try{
									floatResultValue = Float.parseFloat(resultValue);
									ts = Long.parseLong(jobID.substring(jobID.lastIndexOf("_")+1));
								} catch(Exception e) {
									logger.error(e.toString(), e);
								}

								if(floatResultValue >= 0 && ts>= 0) {
									DqMetricsValue newDQMetricsValue = new DqMetricsValue();
									newDQMetricsValue.setMetricName(jobID.substring(0, jobID.lastIndexOf("_")));
									newDQMetricsValue.setTimestamp(ts);
									newDQMetricsValue.setValue(floatResultValue);
									logger.info("===================new accuracy dq metrics: "+newDQMetricsValue.getMetricName()+" "+newDQMetricsValue.getTimestamp()+" "+newDQMetricsValue.getTimestamp());
									dqMetricsService.insertMetadata(newDQMetricsValue);

//									object.put("endtime", new Date().getTime());
//									object.put("value", value);
									job.setEndtime(ts);
									job.setValue(ts);
									jobRepo.update(job);
								}

								//insert missing data path to mongo

								SampleFilePathLKP sfp = new SampleFilePathLKP();

								sfp.setHdfsPath(historyJobFolderLV1Dir + "/" + "missingRec.txt");
								sfp.setModelName(jobID.substring(0, jobID.lastIndexOf("_")));
								sfp.setTimestamp(ts);

								dqMetricsService.insertSampleFilePath(sfp);


							} else if(model.getModelType() == ModelType.VALIDITY) {
								Gson gson = new Gson();
								ValidateHiveJobConfig resultObject = gson.fromJson(resultValue.toString(), ValidateHiveJobConfig.class);
								String content = model.getModelContent();
								String[] contents = content.split("\\|");
								String calType = contents[2];
								String calColname = contents[3];
								long tempValue = resultObject.getValue(calColname, Integer.parseInt(calType));

								long ts = -1;
								try{
									ts = Long.parseLong(jobID.substring(jobID.lastIndexOf("_")+1));
								} catch(Exception e) {
									logger.warn(e.toString(), e);
								}

								if(tempValue >= 0 && ts>= 0) {
									DqMetricsValue newDQMetricsValue = new DqMetricsValue();
									newDQMetricsValue.setMetricName(metricsName);
									newDQMetricsValue.setTimestamp(ts);
									newDQMetricsValue.setValue(tempValue);
									logger.warn("===================new validity dq metrics: "+metricsName+" "+ts+" "+tempValue);
									dqMetricsService.insertMetadata(newDQMetricsValue);

									job.setEndtime(ts);
									job.setValue(tempValue);
									jobRepo.update(job);
								}
							}

							logger.warn("===================publish metrics done.");
						}
					}

					job.setJobType(JobStatus.FINISHED);
					jobRepo.update(job);

				} else if(logindi == 1 && resultindi == 0) {
					if(environment.equals("prod")) {
						String failureJobFolderLV1Dir = runningJobFolderLV1Dir.replaceAll(runningfoldername, failurefoldername);
						Process processMoveFolder = Runtime.getRuntime().exec("hadoop fs -mv "+runningJobFolderLV1Dir+" "+failureJobFolderLV1Dir);
						result = processMoveFolder.waitFor();
						if(result != 0)
						{
							logger.warn("===================="+"hadoop fs -mv "+runningJobFolderLV1Dir+" "+failureJobFolderLV1Dir+" error");
							continue;
						}
						logger.warn("===================moved to history folder");
					}
				} else {
					logger.warn("===================waiting");
				}

			}



		} catch (Exception e) {
			logger.warn(e.toString(), e);
		}
	}

	boolean createFile(String destFileName) {
		File file = new File(destFileName);
		if(file.exists()) {
			return false;
		}
		if (destFileName.endsWith(File.separator)) {
			return false;
		}
		if(!file.getParentFile().exists()) {
			if(!file.getParentFile().mkdirs()) {
				return false;
			}
		}
		try {
			if (file.createNewFile()) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

    protected void updateModelStatus(int fromStatus, int toStatus) {
        List<DqModel> allmodels = dqModelRepo.getByStatus(fromStatus);
        for (DqModel model : allmodels) {
            List<DqMetricsValue> allMetrics = metricsRepo.getByMetricsName(model.getModelName());
            if (allMetrics.size() >= DqModelCreator.MIN_TESTING_JOB_NUMBER) {
                model.setStatus(toStatus);
                dqModelRepo.update(model);
            }
        }
    }

}
