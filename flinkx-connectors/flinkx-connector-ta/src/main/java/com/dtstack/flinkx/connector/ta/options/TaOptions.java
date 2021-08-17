package com.dtstack.flinkx.connector.ta.options;

import static org.apache.flink.configuration.ConfigOptions.key;

import org.apache.flink.configuration.ConfigOption;

public class TaOptions {

    public static final ConfigOption<Integer> THREAD =
            key("thread")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Number of concurrent .");

    public static final ConfigOption<String> PUSH_URL =
            key("pushUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Report the address .");
    public static final ConfigOption<Boolean> UUID =
            key("uuid")
                    .booleanType().defaultValue(true)
                    .withDescription(
                            "uuid flag .");
    public static final ConfigOption<String> TYPE =
            key("type")
                    .stringType().noDefaultValue()
                    .withDescription(
                            "data type .");
    public static final ConfigOption<String> COMPRESS =
            key("compress")
                    .stringType().defaultValue("none")
                    .withDescription(
                            "compress .");
    public static final ConfigOption<String> APPID =
            key("appid")
                    .stringType().noDefaultValue()
                    .withDescription(
                            "appid .");

}
