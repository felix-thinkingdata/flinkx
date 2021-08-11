package com.dtstack.flinkx.connector.ta.util;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.alibaba.fastjson.serializer.ToStringSerializer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;

/**
 * @author: Felix.Wang
 * @email: Felix@thinkingdata.cn
 * @date: 2019/11/18-15:49
 * @module: 类所属模块
 * @describe: 当用户传account_id, distinct_id为Double，String会用科学技术法，用此处序列化兼容
 */
public class ToDoubleStringSerializer implements ObjectSerializer {
    public static final ToStringSerializer instance = new ToStringSerializer();

    public ToDoubleStringSerializer() {
    }

    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        SerializeWriter out = serializer.out;
        if (object == null) {
            out.writeNull();
        } else {
            if (object instanceof Number) {
                String strVal = new BigDecimal(String.valueOf(object)).toPlainString();
                out.writeString(strVal);
            } else {
                String strVal = object.toString();
                out.writeString(strVal);
            }


        }
    }
}
