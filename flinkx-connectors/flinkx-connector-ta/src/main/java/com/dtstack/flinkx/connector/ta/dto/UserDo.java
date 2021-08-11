package com.dtstack.flinkx.connector.ta.dto;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.dtstack.flinkx.connector.ta.constant.TaConstants;
import com.dtstack.flinkx.connector.ta.util.DateUtil;

/**
 * Created by zhoujin on 2018/7/30.
 */
public class UserDo extends TaDataDo {
    private static final Logger logger = LoggerFactory.getLogger(UserDo.class);

    public static UserDo toUser(Map<String, Object> dataMap, String type) {
        UserDo userDo = new UserDo();
        userDo.type = type;
        Object accountIdObj = dataMap.get("#account_id");
        if (accountIdObj != null) {
            userDo.accountId = accountIdObj;
        }
        Object distinctIdObj = dataMap.get("#distinct_id");
        if (distinctIdObj != null) {
            userDo.distinctId = distinctIdObj;
        }
        if (userDo.accountId == null && userDo.distinctId == null) {
            logger.error("sql中[#account_id]与[#distinct_id]不能都为空");
            return null;
        }
        Object uuidObj = dataMap.get("#uuid");
        if (uuidObj != null) {
            userDo.uuid = uuidObj.toString();
        }
        Object timeObj = dataMap.get("#time");
        if (timeObj == null) {
            userDo.time = null;
        } else if (timeObj instanceof Timestamp) {
            userDo.time = ((Timestamp) timeObj);
        } else if (timeObj instanceof Date) {
            userDo.time = ((Date) timeObj);
        } else if (timeObj instanceof String) {
            userDo.time = DateUtil.parserDateStr((String) timeObj);
        }
        if (userDo.time == null) {
            logger.error("sql中[#time]字段不能为空");
            return null;
        }

        userDo.propertyMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (!TaConstants.TA_SYSTEM_COLUMN_SET.contains(key)) {
                userDo.propertyMap.put(key, val);
            }
        }
        return userDo;
    }

}
