package com.dtstack.flinkx.connector.ta.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.ta.conf.TaConf;
import com.dtstack.flinkx.connector.ta.converter.TaRowConverter;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

public class TaDynamicTableSink  implements DynamicTableSink {

    private final TaConf taConf;
    private final DataType type;
    private final TableSchema tableSchema;

    public TaDynamicTableSink(TaConf taConf, DataType type, TableSchema tableSchema) {
        this.taConf = taConf;
        this.type = type;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        // 一些其他参数的封装,如果有
        List<FieldConf> fieldList = Arrays
                .stream(tableSchema.getFieldNames())
                .map(e -> {
                    FieldConf fieldConf = new FieldConf();
                    fieldConf.setName(e);
                    return fieldConf;
                })
                .collect(
                        Collectors.toList());
        taConf.setColumn(fieldList);

        TaOutPutFormatBuilder builder =new TaOutPutFormatBuilder();
        builder.setTaConf(taConf);
        builder.setRowConverter(new TaRowConverter(rowType));
        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction(builder.finish()), taConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new TaDynamicTableSink(taConf, type, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "TaDynamicTableSink";
    }
}
