package com.ebay.oss.griffin.resources;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.UserSubscription;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelInput;
import com.ebay.oss.griffin.vo.NotificationRecord;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.PlatformMetadata;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.spi.component.ioc.IoCComponentProviderFactory;
import com.sun.jersey.spi.spring.container.SpringComponentProviderFactory;

@Ignore
public class ControllersTest {
	static final URI BASE_URI = getBaseURI();
	static final String REST_PATH = "rest";
	static HttpServer server;
	static WebResource service;

	private static URI getBaseURI() {
		return UriBuilder.fromUri("http://localhost/").port(9998).build();
	}

	@BeforeClass
	public static void startServer() throws IOException {
		System.out.println("Starting grizzly...");

		//		Injector injector = Guice.createInjector(new ServletModule() {
		//			@Override
		//			protected void configureServlets() {
		//				bind(LoginService.class).to(LoginServiceImpl.class);
		//
		//			}
		//		});

		ResourceConfig rc = new PackagesResourceConfig(
				"org.apache.bark.resources");

		//		IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc,
		//				injector);
		ConfigurableApplicationContext cac =
				new ClassPathXmlApplicationContext("classpath:context.xml");
		IoCComponentProviderFactory ioc = new SpringComponentProviderFactory(rc,
				cac);

		server = GrizzlyServerFactory.createHttpServer(BASE_URI + REST_PATH, rc,
				ioc);


		System.out
		.println(String
				.format("Jersey app started with WADL available at "
						+ "%srest/application.wadl\nTry out %s\nHit enter to stop it...",
						BASE_URI, BASE_URI));

		Client client = Client.create(new DefaultClientConfig());
		service = client.resource(getBaseURI());
	}

	@AfterClass
	public static void stopServer() {
		server.stop();
	}


	//-- LoginController test --
	@Test
	public void testLoginUser() throws IOException {
		//		Client client = Client.create(new DefaultClientConfig());
		//		WebResource service = client.resource(getBaseURI());

		// String input = "{\"username\":\"test\",\"password\":\"test\"}";
		//
		//
		// ClientResponse resp = service.path(REST_PATH).path("login/authenticate")
		// 		.header("Content-Type", MediaType.APPLICATION_JSON)
		// 		.accept(MediaType.APPLICATION_JSON)
		// 		.post(ClientResponse.class, input);
		// // .get(ClientResponse.class);
		// //System.out.println("Got stuff: " + resp);
		// String text = resp.getEntity(String.class);
		//
		// assertEquals(200, resp.getStatus());
		// Assert.assertNotNull(text);
	}


	//-- DataAssetController test --
	@Test
	public void testDataAssetControllerGetSrcTree() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("dataassets/metadata")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		List<PlatformMetadata> list = resp.getEntity(new GenericType<List<PlatformMetadata>>(){});

		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDataAssetControllerNewGetUpdateDeleteAndGetAll() throws IOException{
		String name = "testAsset_lliu13", owner = "lliu13";
		String input = "{\"assetName\":\"" + name + "\",\"assetType\":\"hdfsfile\",\"assetHDFSPath\":\"/user/b_des/bark/\",\"system\":\"Bullseye\",\"platform\":\"Apollo\",\"owner\":\"" + owner + "\"}";

		//add new data asset
		ClientResponse respAdd = service.path(REST_PATH).path("dataassets/")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, input);
		assertEquals(201, respAdd.getStatus());

		//get all data asset
		ClientResponse respGetAll = service.path(REST_PATH).path("dataassets/")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<DataAsset> list = respGetAll.getEntity(new GenericType<List<DataAsset>>(){});
		assertEquals(200, respGetAll.getStatus());
		Assert.assertNotNull(list);
		Assert.assertTrue(list.size() > 0);

		//find the new add data asset id
		long id = 0;
		Iterator<DataAsset> itr = list.iterator();
		while (itr.hasNext()) {
			DataAsset da = itr.next();
			if (da.getAssetName().equals(name)) {
				id = da.get_id();
				break ;
			}
		}

		//get new add data asset
		String prmGet = Long.toString(id);
		ClientResponse respGet = service.path(REST_PATH).path("dataassets/metadata/" + prmGet)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		DataAsset da = respGet.getEntity(DataAsset.class);
		assertEquals(200, respGet.getStatus());
		Assert.assertNotNull(da);

		//get one asset, this interface is the same with the previous one
		String prmGet1 = Long.toString(id);
		ClientResponse respGet1 = service.path(REST_PATH).path("dataassets/" + prmGet1)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		DataAsset da1 = respGet1.getEntity(DataAsset.class);
		assertEquals(200, respGet1.getStatus());
		Assert.assertNotNull(da1);

		//update the data asset
		String newOwner = "wenzhao";
		String updateInput = "{\"assetName\":\"" + name + "\",\"assetType\":\"hdfsfile\",\"assetHDFSPath\":\"/user/b_des/bark/\",\"system\":\"Bullseye\",\"platform\":\"Apollo\",\"owner\":\"" + newOwner + "\"}";
		ClientResponse respUpdate = service.path(REST_PATH).path("dataassets/")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.put(ClientResponse.class, updateInput);
		String updt = respUpdate.getEntity(String.class);
		assertEquals(200, respUpdate.getStatus());
		String sucUpdate = "{\"status\":\"0\" , \"result\":\"success\"}";
		assertEquals(sucUpdate, updt);

		//delete the data asset
		String prmDel = Long.toString(id);
		ClientResponse respDel = service.path(REST_PATH).path("dataassets/" + prmDel)
				.accept(MediaType.APPLICATION_JSON)
				.delete(ClientResponse.class);
		String rm = respDel.getEntity(String.class);
		assertEquals(200, respDel.getStatus());
		String sucDel = "{\"status\":\"0\" , \"result\":\"success\"}";
		assertEquals(sucDel, rm);
	}

	@Test
	public void testDataAssetControllerGetAssetList() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("dataassets/")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		List<DataAsset> list = resp.getEntity(new GenericType<List<DataAsset>>(){});

		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	//-- DQMetricsController test --
	@Test
	public void testDQMetricsControllerInsertAndGetMetadata() throws IOException{

		String asset_id = "test100", metricName = "test1001";
		float v = 1638.6f;

		//insert
		String dq = "{\"id\":12345678,\"assetId\":\"" + asset_id + "\",\"metricName\":\"" + metricName + "\",\"timestamp\":" + new Date().getTime() + ",\"value\":" + v + "}";
		ClientResponse respAdd = service.path(REST_PATH).path("metrics/")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, dq);
		assertEquals(201, respAdd.getStatus());

		ClientResponse respGet = service.path(REST_PATH).path("metrics/" + asset_id + "/latest")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		DqMetricsValue val = respGet.getEntity(DqMetricsValue.class);
		assertEquals(200, respGet.getStatus());
		Assert.assertNotNull(val);
		assertEquals(val.getValue(), v);
	}

	@Test
	public void testDQMetricsControllerGetHeatMap() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("metrics/heatmap")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetAllBriefMetrics() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("metrics/briefmetrics")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetBriefMetrics() throws IOException{
		String system = "unknown";
		ClientResponse resp = service.path(REST_PATH).path("metrics/briefmetrics/" + system)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetAllDashboard() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("metrics/dashboard")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetDashboard() throws IOException{
		String system = "IDLS";
		ClientResponse resp = service.path(REST_PATH).path("metrics/dashboard/" + system)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetMyDashboard() throws IOException{
		String user = "lliu13";
		ClientResponse resp = service.path(REST_PATH).path("metrics/mydashboard/" + user)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SystemLevelMetrics> list = resp.getEntity(new GenericType<List<SystemLevelMetrics>>(){});
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQMetricsControllerGetCompelete() throws IOException{
		String name = "TotalCount_asset1";
		ClientResponse resp = service.path(REST_PATH).path("metrics/complete/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		AssetLevelMetrics alm = resp.getEntity(AssetLevelMetrics.class);
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(alm);
		System.out.println(alm.getName() + " -> " + alm.getDq());
		System.out.println();
	}

	@Test
	public void testDQMetricsControllerGetBrief() throws IOException{
		String name = "TotalCount_asset1";
		ClientResponse resp = service.path(REST_PATH).path("metrics/brief/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		AssetLevelMetrics alm = resp.getEntity(AssetLevelMetrics.class);
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(alm);
		System.out.println(alm.getName() + " -> " + alm.getDq());
		System.out.println();
	}

	@Test
	public void testDQMetricsControllerRefreshDQ() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("metrics/refresh")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		assertEquals(204, resp.getStatus());
	}

	@Test
	public void testDQMetricsControllerGetOverViewStats() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("metrics/statics")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		OverViewStatistics ovs = resp.getEntity(OverViewStatistics.class);
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(ovs);
		System.out.println("metrics: " + ovs.getMetrics() + " assets: " + ovs.getAssets());
		System.out.println();
	}

	@Test
	public void testDQMetricsControllerInsertGetAndDownloadSamplePath() throws IOException{

		String name = "testModel100", path = "/user/b_des/bark/testPath/";

		//insertSamplePath
		String sample = "{\"modelName\":\"" + name + "\",\"hdfsPath\":\"" + path + "\",\"timestamp\":" + new Date().getTime() + "}";
		ClientResponse respInsert = service.path(REST_PATH).path("metrics/sample")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, sample);
		assertEquals(201, respInsert.getStatus());

		//getSampleList
		ClientResponse respGet = service.path(REST_PATH).path("metrics/sample/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		List<SampleOut> list = respGet.getEntity(new GenericType<List<SampleOut>>(){});
		assertEquals(200, respGet.getStatus());
		Assert.assertNotNull(list);
		Iterator<SampleOut> itr = list.iterator();
		while (itr.hasNext()) {
			SampleOut so = itr.next();
			System.out.println("SampleOut: " + new Date(so.getDate()).toString() + " " + so.getPath());
		}
		if (list.size() == 0) System.out.println("Sample File List is empty");

		//downloadFile
		ClientResponse respDownload = service.path(REST_PATH).path("metrics/download/" + path)
				.accept(MediaType.APPLICATION_OCTET_STREAM)
				.get(ClientResponse.class);
		assertEquals(200, respDownload.getStatus());
	}



	//-- DQModelController test --
	@Test
	public void testDQModelControllerAllModels() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("models/")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		List<DqModelVo> list = resp.getEntity(new GenericType<List<DqModelVo>>(){});

		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}

	@Test
	public void testDQModelControllerNewGetAndDeleteModel() throws IOException{
		String name = "testVali", owner = "lliu13", email = owner + "@ebay.com", db = "Apollo";

		//newModel
		String input = "{\"basic\":{\"type\":1,\"system\":0,\"status\":2,\"scheduleType\":0,\"owner\":\"" + owner +
				"\",\"name\":\"" + name + "\",\"email\":\"" + email + "\",\"desc\":\"total count for dmg\"," +
				"\"dataasetId\":6,\"dataaset\":\"dmg\"},\"extra\":{\"vaType\":1," +
				"\"column\":\"age\",\"srcDataSet\":\"Bullseye.dmg\",\"srcDb\":\"" + db + "\"}}";
		ClientResponse respNew = service.path(REST_PATH).path("models/")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, input);
		assertEquals(201, respNew.getStatus());

		//getModel
		ClientResponse respGet = service.path(REST_PATH).path("models/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		ModelInput mi = respGet.getEntity(ModelInput.class);
		assertEquals(200, respGet.getStatus());
		Assert.assertNotNull(mi);
		assertEquals(mi.getBasic().getOwner(), owner);
		assertEquals(mi.getExtra().getSrcDb(), db);

		//deleteModel
		ClientResponse respDel = service.path(REST_PATH).path("models/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.delete(ClientResponse.class);
		assertEquals(204, respDel.getStatus());
	}

	@Test
	public void testDQModelControllerEnableModel() throws IOException{
		String name = "accuracy_viewitem_e2e_user";
		ClientResponse resp = service.path(REST_PATH).path("models/enableModel/" + name)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		assertEquals(204, resp.getStatus());
	}


	//-- NotificationController test --
	@Test
	public void testNotificationControllerGetAll() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("notifications/")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		List<NotificationRecord> list = resp.getEntity(new GenericType<List<NotificationRecord>>(){});

		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(list);
	}



	//-- ScheduleController test --
	@Test
	public void testScheduleControllerSchedule() throws IOException{
		ClientResponse resp = service.path(REST_PATH).path("schedule/")
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		assertEquals(204, resp.getStatus());
	}



	//-- SubscribeController test --
	@Test
	public void testSubscribeControllerGet() throws IOException{
		String user = "lliu13";
		ClientResponse resp = service.path(REST_PATH).path("subscribe/" + user)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		UserSubscription usi = resp.getEntity(UserSubscription.class);

		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(usi);
	}

	@Test
	public void testSubscribeControllerSet() throws IOException{
		String input = "{}";
		ClientResponse resp = service.path(REST_PATH).path("subscribe/")
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, input);

		assertEquals(204, resp.getStatus());
	}

	//-- ReportController test --
	@Test
	public void testReportControllerGetBEReportByName() throws IOException{
		String name = "mean";
		ClientResponse resp = service.path(REST_PATH).path("report/be")
				.queryParam("name", name)
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		String str = resp.getEntity(String.class);
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(str);
	}

	@Test
	public void testReportControllerGetBrief() throws IOException{
		String name = "test1001";
		ClientResponse resp = service.path(REST_PATH).path("report/24h")
				.queryParam("name", name)
				.header("Content-Type", MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);

		AssetLevelMetrics alm = resp.getEntity(AssetLevelMetrics.class);
		assertEquals(200, resp.getStatus());
		Assert.assertNotNull(alm);
	}

}
