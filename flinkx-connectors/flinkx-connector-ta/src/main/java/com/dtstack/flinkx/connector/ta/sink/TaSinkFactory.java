package com.dtstack.flinkx.connector.ta.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.ta.conf.TaConf;
import com.dtstack.flinkx.connector.ta.converter.TaColumnConverter;
import com.dtstack.flinkx.connector.ta.converter.TaRawTypeConverter;
import com.dtstack.flinkx.connector.ta.converter.TaRowConverter;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TableUtil;

public class TaSinkFactory extends SinkFactory {

    private final TaConf taConf;

    public TaSinkFactory(SyncConf config) {
        super(config);
        taConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        TaConf.class);
        taConf.setColumn(config.getWriter().getFieldList());
        super.initFlinkxCommonConf(taConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        TaOutPutFormatBuilder builder = new TaOutPutFormatBuilder();
        builder.setTaConf(taConf);
        AbstractRowConverter converter;
        if (useAbstractBaseColumn) {
            converter = new TaColumnConverter(taConf.getColumn());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(taConf.getColumn(), getRawTypeConverter());
            converter = new TaRowConverter(rowType);
        }
        builder.setRowConverter(converter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return TaRawTypeConverter::apply;
    }
}
