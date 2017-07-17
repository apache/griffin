package org.apache.griffin.core.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by xiangrchen on 6/15/17.
 */
public class JsonConvert {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonConvert.class);

    public static String toJson(Object obj) throws JsonProcessingException {
        ObjectMapper mapper=new ObjectMapper();
        String jsonStr=null;
        jsonStr=mapper.writeValueAsString(obj);
        return jsonStr;
    }

//    public static <T>T toEntity(String jsonStr,Class<T> type) throws IOException {
//        if (jsonStr==null || jsonStr.length()==0){
//            LOGGER.warn("jsonStr "+type+" is empty!");
//            return null;
//        }
//        Gson gson = new Gson();
//        return gson.fromJson(jsonStr, type);
//    }

//    public static <T>T toEntity(String jsonStr,Type type) {
//        if (jsonStr==null || jsonStr.length()==0){
//            LOGGER.warn("jsonStr "+type+" is empty!");
//            return null;
//        }
//        Gson gson = new Gson();
//        return gson.fromJson(jsonStr, type);
//    }

    public static <T>T toEntity(String jsonStr,Class<T> type) throws IOException {
        if (jsonStr==null || jsonStr.length()==0){
            LOGGER.warn("jsonStr "+type+" is empty!");
            return null;
        }
        ObjectMapper mapper=new ObjectMapper();
        return mapper.readValue(jsonStr,type);
    }

    public static <T>T toEntity(String jsonStr,TypeReference type) throws IOException {
        if (jsonStr==null || jsonStr.length()==0){
            LOGGER.warn("jsonStr "+type+" is empty!");
            return null;
        }
        ObjectMapper mapper=new ObjectMapper();
        return mapper.readValue(jsonStr,type);
    }
}
