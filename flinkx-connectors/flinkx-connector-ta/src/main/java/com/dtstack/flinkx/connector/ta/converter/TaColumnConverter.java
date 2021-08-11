package com.dtstack.flinkx.connector.ta.converter;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.ColumnTypeUtil.DecimalInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.iceberg.shaded.org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.iceberg.shaded.org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TaColumnConverter extends AbstractRowConverter<RowData, RowData,  Object[], String> {
    private List<String> ColumnNameList;
    private transient Map<String, DecimalInfo> decimalColInfo;
    public TaColumnConverter(List<FieldConf> fieldConfList) {
        super(fieldConfList.size());
        for (int i = 0; i < fieldConfList.size(); i++) {
            String type = fieldConfList.get(i).getType();
            toExternalConverters[i] = wrapIntoNullableExternalConverter(createExternalConverter(type), type);
        }
    }


    @Override
    public  Object[] toExternal(RowData rowData, Object[] data ) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Object[]> wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, String type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data[index] = null;
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        return null;
    }

    @Override
    protected ISerializationConverter<Object[]> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case "TINYINT":
                return (rowData, index, data) -> data[index] = rowData.getByte(index);
            case "SMALLINT":
                return (rowData, index, data) -> data[index] = rowData.getShort(index);
            case "INT":
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case "BIGINT":
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case "FLOAT":
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case "DOUBLE":
                return (rowData, index, data) -> data[index] = rowData.getDouble(index);
            case "DECIMAL":
                return (rowData, index, data) -> {
                    ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(ColumnNameList.get(index));
                    HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData.getString(index).toString()));
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
                    if(hiveDecimal == null){
                        String msg = String.format("The [%s] data data [%s] precision and scale do not match the metadata:decimal(%s, %s)", index, decimalInfo.getPrecision(), decimalInfo.getScale(), rowData);
                        throw new WriteRecordException(msg, new IllegalArgumentException());
                    }
                    data[index] = new HiveDecimalWritable(hiveDecimal);
                };
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, data) -> data[index] = rowData.getString(index).toString();
            case "TIMESTAMP":
                return (rowData, index, data) -> data[index] = rowData.getTimestamp(index, 6).toTimestamp();
            case "DATE":
                return (rowData, index, data) -> data[index] = new Date(rowData.getTimestamp(index, 6).getMillisecond());
            case "BINARY":
                return (rowData, index, data) -> data[index] = new BytesWritable(rowData.getBinary(index));
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }


    public void setColumnNameList(List<String> columnNameList) {
        this.ColumnNameList = columnNameList;
    }

    public void setDecimalColInfo(Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo) {
        this.decimalColInfo = decimalColInfo;
    }
}
