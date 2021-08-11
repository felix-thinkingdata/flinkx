package com.dtstack.flinkx.connector.ta.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;

import java.util.List;

/**
 * class_name: TaConf
 * package: com.dtstack.flinkx.connector.ta.conf
 * describe: 通用配置信息
 *
 * @author: felix@thinkingdata.cn
 *         creat_date: 2021/8/10
 *         creat_time: 10:48
 **/
public class TaConf extends FlinkxCommonConf {
    /**
     * describe:发送并发数
     */
    protected int thread;
    /**
     * describe:上报地址
     */
    protected String pushUrl;
    /**
     * describe:是否开启uuid
     */
    protected boolean uuid;
    /**
     * describe:数据类型
     */
    protected String type;
    /**
     * describe:压缩类型
     */
    protected String compress;
    /**
     * describe: 项目appid
     */
    protected String appid;
    /**
     * describe: 类型列表
     */
    protected List<String> fullColumnTypes;
    /**
     * describe: 名字列表
     */
    protected List<String> fullColumnNames;

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }

    public String getPushUrl() {
        return pushUrl;
    }

    public void setPushUrl(String pushUrl) {
        this.pushUrl = pushUrl;
    }

    public boolean isUuid() {
        return uuid;
    }

    public void setUuid(boolean uuid) {
        this.uuid = uuid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public List<String> getFullColumnTypes() {
        return fullColumnTypes;
    }

    public void setFullColumnTypes(List<String> fullColumnTypes) {
        this.fullColumnTypes = fullColumnTypes;
    }

    public List<String> getFullColumnNames() {
        return fullColumnNames;
    }

    public void setFullColumnNames(List<String> fullColumnNames) {
        this.fullColumnNames = fullColumnNames;
    }

    @Override
    public String toString() {
        return "TaConf{" +
                "thread=" + thread +
                ", pushUrl='" + pushUrl + '\'' +
                ", uuid=" + uuid +
                ", type='" + type + '\'' +
                ", compress='" + compress + '\'' +
                ", appid='" + appid + '\'' +
                ", fullColumnTypes=" + fullColumnTypes +
                ", fullColumnNames=" + fullColumnNames +
                '}';
    }
}
