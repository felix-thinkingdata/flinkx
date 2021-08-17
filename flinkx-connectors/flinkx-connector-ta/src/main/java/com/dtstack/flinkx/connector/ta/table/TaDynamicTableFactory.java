package com.dtstack.flinkx.connector.ta.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.dtstack.flinkx.connector.ta.conf.TaConf;
import com.dtstack.flinkx.connector.ta.sink.TaDynamicTableSink;

import java.util.HashSet;
import java.util.Set;

import static com.dtstack.flinkx.connector.ta.options.TaOptions.APPID;
import static com.dtstack.flinkx.connector.ta.options.TaOptions.COMPRESS;
import static com.dtstack.flinkx.connector.ta.options.TaOptions.PUSH_URL;
import static com.dtstack.flinkx.connector.ta.options.TaOptions.THREAD;
import static com.dtstack.flinkx.connector.ta.options.TaOptions.TYPE;
import static com.dtstack.flinkx.connector.ta.options.TaOptions.UUID;

public class TaDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "ta-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        TaConf sinkConf = new TaConf();
        sinkConf.setThread(config.get(THREAD));
        sinkConf.setPushUrl(config.get(PUSH_URL));
        sinkConf.setUuid(config.get(UUID));
        sinkConf.setType(config.get(TYPE));
        sinkConf.setCompress(config.get(COMPRESS));
        sinkConf.setAppid(config.get(APPID));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new TaDynamicTableSink(
                sinkConf,
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                physicalSchema);
    }


    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(THREAD);
        options.add(PUSH_URL);
        options.add(UUID);
        options.add(TYPE);
        options.add(COMPRESS);
        options.add(APPID);
        return options;
    }
}
