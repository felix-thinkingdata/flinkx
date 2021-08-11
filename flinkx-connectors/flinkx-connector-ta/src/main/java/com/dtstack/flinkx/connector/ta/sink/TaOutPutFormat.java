package com.dtstack.flinkx.connector.ta.sink;

import org.apache.flink.table.data.RowData;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.ta.conf.TaConf;
import com.dtstack.flinkx.connector.ta.constant.TaConstants;
import com.dtstack.flinkx.connector.ta.converter.TaColumnConverter;
import com.dtstack.flinkx.connector.ta.dto.EventDo;
import com.dtstack.flinkx.connector.ta.dto.TaDataDo;
import com.dtstack.flinkx.connector.ta.dto.UserDo;
import com.dtstack.flinkx.connector.ta.util.CommonUtil;
import com.dtstack.flinkx.connector.ta.util.CompressUtil;
import com.dtstack.flinkx.connector.ta.util.HttpRequestUtil;
import com.dtstack.flinkx.connector.ta.util.RetryerUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.github.rholder.retry.Retryer;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class TaOutPutFormat extends BaseRichOutputFormat {


    private TaConf taConf;
    private CloseableHttpClient httpClient = null;
    private Retryer retryer;
    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        JSONArray dataArray = new JSONArray();
        Object[] data = new Object[taConf.getColumn().size()];
        try {
            data = (Object[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            throw new WriteRecordException("errorMessage", e, -1, rowData);
        }
        List<FieldConf> fieldConfList = taConf.getColumn();
        HashMap<String, Object> columnMap = new HashMap<>();
        for (int i = 0; i < taConf.getColumn().size(); i++) {
            columnMap.put(fieldConfList.get(i).getName(), data[i]);
        }
        TaDataDo taDataDo = null;

        if (TaConstants.TRACK.equals(taConf.getType()) || TaConstants.TRACK_UPDATE.equals(taConf.getType()) || TaConstants.TRACK_OVERWRITE.equals(taConf.getType())) {
            taDataDo = EventDo.toEvent(columnMap, taConf.getType());
        } else if (TaConstants.USER_SET.equals(taConf.getType())) {
            taDataDo = UserDo.toUser(columnMap, taConf.getType());
        }

        dataArray.add(taDataDo);
        sendDataByRestfulApi(taConf.getAppid(),dataArray);
        System.out.println(JSON.toJSONString(taDataDo));
        lastRow = rowData;

    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {

        JSONArray dataArray = new JSONArray();
        for (RowData rowData : rows) {
            Object[] data = new Object[taConf.getColumn().size()];
            try {
                data = (Object[]) rowConverter.toExternal(rowData, data);
            } catch (Exception e) {
                throw new WriteRecordException("errorMessage", e, -1, rowData);
            }
            List<FieldConf> fieldConfList = taConf.getColumn();
            HashMap<String, Object> columnMap = new HashMap<>();
            for (int i = 0; i < taConf.getColumn().size(); i++) {
                columnMap.put(fieldConfList.get(i).getName(), data[i]);
            }
            TaDataDo taDataDo = null;

            if (TaConstants.TRACK.equals(taConf.getType()) || TaConstants.TRACK_UPDATE.equals(taConf.getType()) || TaConstants.TRACK_OVERWRITE.equals(taConf.getType())) {
                taDataDo = EventDo.toEvent(columnMap, taConf.getType());
            } else if (TaConstants.USER_SET.equals(taConf.getType())) {
                taDataDo = UserDo.toUser(columnMap, taConf.getType());
            }
            dataArray.add(taDataDo);
        }
        sendDataByRestfulApi(taConf.getAppid(),dataArray);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        httpClient = HttpRequestUtil.getConnection();
        if (rowConverter instanceof TaColumnConverter) {
            ((TaColumnConverter) rowConverter).setColumnNameList(taConf.getColumn().stream().map(FieldConf::getName).collect(Collectors.toList()));
        }
        retryer = RetryerUtil.initRetryer();

    }

    @Override
    protected void closeInternal() throws IOException {

    }

    public void setTaConf(TaConf taConf) {
        this.taConf = taConf;
    }

    private void sendDataByRestfulApi(String appid, JSONArray dataArray)  {

        try {
            retryer.call(() -> {
                HttpPost httpPost;
                AbstractHttpEntity params = encodeRecord(dataArray);
                RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(600000).setConnectTimeout(300000).build();
                httpPost = new HttpPost(taConf.getPushUrl());
                httpPost.addHeader("appid", appid);
                httpPost.addHeader("user-agent", "datax-1.0");
                httpPost.addHeader("compress", taConf.getCompress());
                httpPost.setConfig(requestConfig);
                httpPost.addHeader("TA-Integration-Type", "datax");
                httpPost.addHeader("TA-Integration-Version", "1.0");
                httpPost.addHeader("TA-Integration-Count", String.valueOf(dataArray.size()));

                httpPost.setEntity(params);
                try (CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost)) {
                    int returnCode = closeableHttpResponse.getStatusLine().getStatusCode();
                    if (returnCode != 200) {
                        throw new Exception("http post with error ,please check data receiver . .return code is " + returnCode + ".");
                    } else {
                        HttpEntity responseEntity = closeableHttpResponse.getEntity();
                        String responseStr = EntityUtils.toString(responseEntity);
                        JSONObject obj = JSONObject.parseObject(responseStr);
                        Integer status = obj.getInteger("code");
                        if (status != 0) {
                            LOG.error("http post with error ,please check data receiver .return code is {}", status);
                        }
                    }

                }
                //LOG.info("data send size :" + dataArray.size() + ",compress is " + compress + ",appid is " + appid);
                return "";
            });
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }


    }

    /**
     * 压缩内容
     *
     * @Author: Felix.Wang
     * @Date: 2019/8/19 14:06
     */
    private AbstractHttpEntity encodeRecord(JSONArray contents) throws IOException {

        String data = contents.toString();
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

        byte[] dataCompressed = null;
        if ("gzip".equalsIgnoreCase(taConf.getCompress())) {
            dataCompressed = CompressUtil.gzipCompress(dataBytes);
        } else if ("lzo".equalsIgnoreCase(taConf.getCompress())) {
            dataCompressed = CompressUtil.lzoCompress(dataBytes);
        } else if ("lz4".equalsIgnoreCase(taConf.getCompress())) {
            dataCompressed = CompressUtil.lz4Compress(dataBytes);
        } else if ("snappy".equalsIgnoreCase(taConf.getCompress())) {
            dataCompressed = CompressUtil.snappyCompress(dataBytes);
        } else if ("none".equalsIgnoreCase(taConf.getCompress())) {
            dataCompressed = dataBytes;
        }
        return new ByteArrayEntity(dataCompressed);
    }


}
