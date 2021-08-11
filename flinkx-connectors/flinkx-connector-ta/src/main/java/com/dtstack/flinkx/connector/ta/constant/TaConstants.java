package com.dtstack.flinkx.connector.ta.constant;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class TaConstants {

    /**
     * @describe: 业务数据类型
     */
    public final static String TRACK = "track";
    public final static String TRACK_UPDATE = "track_update";
    public final static String TRACK_OVERWRITE = "track_overwrite";
    public final static String USER_SET = "user_set";
    public final static String USER_SETONCE = "user_setOnce";
    public final static String USER_ADD = "user_add";
    public final static String USER_UNSET = "user_unset";
    public final static String USER_APPEND = "user_append";
    public final static String USER_DEL = "user_del";
    public static final Set<String> TA_SYSTEM_COLUMN_SET = ImmutableSet.copyOf(new String[]{"#account_id", "#distinct_id", "#type", "#ip", "#time", "#event_name", "#event_id", "#uuid", "#first_check_id", "#transaction_property"});

}
