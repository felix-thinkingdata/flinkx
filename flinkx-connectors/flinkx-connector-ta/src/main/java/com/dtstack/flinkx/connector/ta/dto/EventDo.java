package com.dtstack.flinkx.connector.ta.dto;


import org.apache.commons.lang3.StringUtils;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.dtstack.flinkx.connector.ta.constant.TaConstants;
import com.dtstack.flinkx.connector.ta.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhoujin on 2018/7/30.
 */
public class EventDo extends TaDataDo {
    private static final Logger logger = LoggerFactory.getLogger(EventDo.class);

    @JSONField(name = "#ip")
    private String ip = null;

    @JSONField(name = "#event_name")
    private String eventName;

    @JSONField(name = "#event_id")
    private String eventId;

    @JSONField(name = "#first_check_id")
    private String firstCheckId;

    @JSONField(name = "#transaction_property")
    private String transactionProperty;

    public static EventDo toEvent(Map<String, Object> dataMap, String type) {
        EventDo eventDo = new EventDo();
        eventDo.type = type;
        Object accountIdObj = dataMap.get("#account_id");
        if (accountIdObj != null) {
            eventDo.accountId = accountIdObj;
        }
        Object distinctIdObj = dataMap.get("#distinct_id");
        if (distinctIdObj != null) {
            eventDo.distinctId = distinctIdObj;
        }
        if (eventDo.accountId == null && eventDo.distinctId == null) {
            logger.error("sql中[#account_id]与[#distinct_id]不能都为空");
            return null;
        }
        Object ipObj = dataMap.get("#ip");
        if (ipObj != null) {
            eventDo.ip = ipObj.toString();
        }
        Object uuidObj = dataMap.get("#uuid");
        if (uuidObj != null) {
            eventDo.uuid = uuidObj.toString();
        }

        Object timeObj = dataMap.get("#time");
        if (timeObj == null) {
            eventDo.time = null;
        } else if (timeObj instanceof Timestamp) {
            eventDo.time = ((Timestamp) timeObj);
        } else if (timeObj instanceof Date) {
            eventDo.time = ((Date) timeObj);
        } else if (timeObj instanceof String) {
            eventDo.time = DateUtil.parserDateStr((String) timeObj);
        }

        if (eventDo.time == null) {
            logger.error("sql中[#time]字段不能为空");
            return null;
        }
        Object eventNameObj = dataMap.get("#event_name");
        if (eventNameObj == null) {
            logger.error("sql中[#event_name]字段不能为空");
            return null;
        }
        eventDo.eventName = eventNameObj.toString();

        Object eventIdObj = dataMap.get("#event_id");
        if ((TaConstants.TRACK_UPDATE.equals(type) || TaConstants.TRACK_OVERWRITE.equals(type))) {
            if (eventIdObj == null) {
                logger.error("sql中[#event_id]字段不能为空");
                return null;
            } else {
                eventDo.eventId = eventIdObj.toString();
            }
        }

        Object firstCheckId = dataMap.get("#first_check_id");
        if (firstCheckId != null) {
            eventDo.firstCheckId = firstCheckId.toString();
        }

        Object transactionProperty = dataMap.get("#transaction_property");
        if (transactionProperty != null) {
            eventDo.transactionProperty = transactionProperty.toString();
        }

        eventDo.propertyMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (!TaConstants.TA_SYSTEM_COLUMN_SET.contains(key)) {
                    eventDo.propertyMap.put(key, val);
            }
        }

        return eventDo;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getFirstCheckId() {
        return firstCheckId;
    }

    public void setFirstCheckId(String firstCheckId) {
        this.firstCheckId = firstCheckId;
    }

    public String getTransactionProperty() {
        return transactionProperty;
    }

    public void setTransactionProperty(String transactionProperty) {
        this.transactionProperty = transactionProperty;
    }
}
