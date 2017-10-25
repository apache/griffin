/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.util;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import java.util.ArrayList;
import java.io.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.griffin.measure.config.params.env.SMSParam;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by xiaoqiu.duan on 2017/9/11.
 */
public class MessageUtil {


    public static String sendSMSCode(String teamPhone, String content, SMSParam smsParam){
        String url=smsParam.host();
        String SYS_ID=smsParam.id();
        String KEY=smsParam.key();
        String sendContext="["+smsParam.uuid()+"]: "+content ;
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
        param.put("reqId",uuid);
        param.put("schemaKey","");
        param.put("sysId",SYS_ID);
        param.put("template",sendContext);
        param.put("templateId","");
        param.put("timestamp",timestamp);
        param.put("token",Md5Util.md5(SYS_ID+timestamp+KEY));
        param.put("typeCode","JR_DATA_ALARM");
        System.out.println("params:  "+param);
        List<Map<String, Object>> bodys = new ArrayList<Map<String, Object>>();
          for (int i=0;i<tels.length;i++) {
            Map<String, Object> body = new HashMap<String, Object>();
            body.put("phoneNo", tels[i]);
            body.put("params", "");
            body.put("userId", 0);
            bodys.add(body);
        }
        System.out.println("bodys:  "+bodys);
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
           String result= postRequestUrl(url, jsonParam.toString(), "utf-8");
            return "send success";
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }

    }


    public static String postRequestUrl(String url, String param,String encode) {
        OutputStreamWriter out = null;
        BufferedReader reader = null;
        String response="";
        try {
            URL httpUrl = null;
            httpUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) httpUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");//x-www-form-urlencoded
            conn.setRequestProperty("connection", "keep-alive");
            conn.setUseCaches(false);
            conn.setInstanceFollowRedirects(true);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.connect();
            //POST
            out = new OutputStreamWriter(
                    conn.getOutputStream());
            out.write(param);

            out.flush();

            System.out.println("send POST "+conn.getResponseCode());
            reader = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));

            String lines;
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), "utf-8");
                response+=lines;
                System.out.println("lines:  "+lines);
            }
            reader.close();
            conn.disconnect();

            //log.info(response.toString());
        } catch (Exception e) {
            System.out.println("send POST error！"+e);
            e.printStackTrace();
        }
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
