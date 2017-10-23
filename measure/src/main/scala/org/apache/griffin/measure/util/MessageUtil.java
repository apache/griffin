package org.apache.griffin.measure.util;


import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.datanucleus.store.types.backed.*;
import java.util.ArrayList;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by xiaoqiu.duan on 2017/9/11.
 */
public class MessageUtil {

    private static final String SYS_ID="JR_DATA";//sysId
    private static final String KEY="f4c87dd954e04afcacd8e36f8b4d553c";
    //发送短信
    public static String sendSMSCode(String teamPhone,String content){
        String url="http://osp-proxy-remote.vip.vip.com:9080/rest/com.vip.api.umc.producer.service.UmcAccessService-1.0.4/directSendSms";
        String sendContext="[金融DQP监控]: "+content ;

        System.out.println(" sendContext:  "+sendContext);

        Long timestamp=new Date().getTime()+20610;
        System.out.println(" timestamp:  "+timestamp);

        String[]  tels=teamPhone.split(",") ;

        String uuid = UUID.randomUUID().toString().replaceAll("-", "");

        Map<String,Object> param=new HashMap<String, Object>();
        param.put("apportionment","JR_DATA_ALARM");
        param.put("event",0);
        param.put("eventTime",0);
        param.put("isMasked",false);
        param.put("originator","ETC");
        param.put("reqId",uuid);//发送方的请求ID，必须唯一。对于java建议用UUID（请去除“-”）。
        param.put("schemaKey","");
        param.put("sysId",SYS_ID);
        param.put("template",sendContext);//自定义模板，如果没有在UMC系统注册消息模板可以直接以字符串形式提供模板
        param.put("templateId","");//消息模板ID，由UMC提供
        param.put("timestamp",timestamp);// 传入的timestamp与UMC服务器的时间相差超过60s，则返回“认证信息不正确”。
        param.put("token",Md5Util.md5(SYS_ID+timestamp+KEY));//Md5(sysId+ timestamp +key)，key在系统接入时分配。Md5工具类： Md5Utils.java
        param.put("typeCode","JR_DATA_ALARM");

        System.out.println("params:  "+param);



        List<Map<String, Object>> bodys = new ArrayList<Map<String, Object>>();
          for (int i=0;i<tels.length;i++) {
            Map<String, Object> body = new HashMap<String, Object>();
            body.put("phoneNo", tels[i]);//支持批量发送，建议一次批量20条
            body.put("params", "");//当templateId不为空时，该字段必填，格式是json，{"username":"xxx"}
            body.put("userId", 0);//唯品会用户id，如果实在拿不到userId，则填0。（注：非UUID）
            bodys.add(body);
        }


        System.out.println("bodys:  "+bodys);

//        Map<String,Object> httpParam=new HashMap<String, Object>();
//        httpParam.put("params",param);
//        httpParam.put("bodys",bodys);
//        System.out.println("httpParam:  "+httpParam);


        JSONObject jsonParam = new JSONObject();
        try {
            jsonParam.put("params",param);
            jsonParam.put("bodys",bodys);
            System.out.println("jsonParam:  "+jsonParam);
            System.out.println("jsonParam:  "+jsonParam.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }

        URL u=null;
        int smsnum=0;
        HttpURLConnection connection=null;
        try{
            //param.put("content", URLEncoder.encode(sendContext, "utf-8"));
           String result= postRequestUrl(url, jsonParam.toString(), "utf-8");
            return "发送成功";
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }

    }

//    public static String getDate(String formatStr) {
//        Calendar cal = Calendar.getInstance();
//        SimpleDateFormat formatter = new SimpleDateFormat(formatStr);
//        return formatter.format(cal.getTime());
//
//    }

//    public static String postRequestUrl(String urlStr, Map<String, Object> param) {
//        return postRequestUrl(urlStr,param,"utf-8");
//    }


    public static String postRequestUrl(String url, String param,String encode) {
        OutputStreamWriter out = null;
        BufferedReader reader = null;
        String response="";
        try {
            URL httpUrl = null; //HTTP URL类 用这个类来创建连接
            //创建URL
            httpUrl = new URL(url);
            //建立连接
            HttpURLConnection conn = (HttpURLConnection) httpUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");//x-www-form-urlencoded
            conn.setRequestProperty("connection", "keep-alive");
            conn.setUseCaches(false);//设置不要缓存
            conn.setInstanceFollowRedirects(true);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.connect();
            //POST请求
            out = new OutputStreamWriter(
                    conn.getOutputStream());
            out.write(param);

            out.flush();

            System.out.println("发送 POST 请求 "+conn.getResponseCode());
            //读取响应
            reader = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));

            String lines;
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), "utf-8");
                response+=lines;
                System.out.println("lines:  "+lines);
            }
            reader.close();
            // 断开连接
            conn.disconnect();

            //log.info(response.toString());
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(reader!=null){
                    reader.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }

        return response;
    }



    private static String readBufferedContent(BufferedReader bufferedReader) {
        if (bufferedReader == null)
            return null;
        StringBuffer result = new StringBuffer();
        String line = null;
        try {
            while (StringUtils.isNotBlank((line = bufferedReader.readLine()))) {
                result.append(line+"\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return result.toString();
    }

}
