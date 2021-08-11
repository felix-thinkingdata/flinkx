package com.dtstack.flinkx.connector.ta.dto;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.annotation.JSONField;
import com.dtstack.flinkx.connector.ta.util.ToDoubleStringSerializer;


import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhoujin on 2018/7/31.
 */
public class TaDataDo {
    @JSONField(name = "#account_id", serializeUsing = ToDoubleStringSerializer.class)
    protected Object accountId = null;
    @JSONField(name = "#distinct_id", serializeUsing = ToDoubleStringSerializer.class)
    protected Object distinctId = null;
    @JSONField(name = "#type")
    protected String type;
    @JSONField(name = "#uuid")
    protected String uuid = null;
    @JSONField(name = "#time", format = "yyyy-MM-dd HH:mm:ss.SSS")
    protected Date time;

    @JSONField(name = "properties")
    protected Map<String, Object> propertyMap = null;

    public Object getAccountId() {
        return accountId;
    }

    public void setAccountId(Object accountId) {
        this.accountId = accountId;
    }

    public Object getDistinctId() {
        return distinctId;
    }

    public void setDistinctId(Object distinctId) {
        this.distinctId = distinctId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, Object> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public static void main(String[] args) {
        Map<String, Object> splitedRows = new HashMap<String, Object>();


        Double num = new Double(9223372036854775807L);
        splitedRows.put("#account_id", num);
        splitedRows.put("#distinct_id", num);
        splitedRows.put("#event_name", "testbuy");
        splitedRows.put("#time", "2019-08-16 08:08:08.000");
        splitedRows.put("#type", "track");
        splitedRows.put("timenumber", 9223372036854775807L);
        TaDataDo taDataDo = EventDo.toEvent(splitedRows, "track");
        JSONArray dataArray = new JSONArray();
        dataArray.add(taDataDo);
        System.out.println(JSON.toJSONString(dataArray));
    }


}
