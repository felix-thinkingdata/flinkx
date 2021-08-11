package com.dtstack.flinkx.connector.ta.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.hadoop.io.BytesWritable;
import org.apache.iceberg.shaded.org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.iceberg.shaded.org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

public class TaRowConverter extends AbstractRowConverter<RowData, RowData, Object[], LogicalType> {

    private static final long serialVersionUID = 1L;

    public TaRowConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]);
        }
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Object[]> wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                data[index] = null;
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }
    @Override
    public Object[] toExternal(RowData rowData, Object[] data) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, data);
        }
        return data;
    }


    @Override
    protected ISerializationConverter<Object[]> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (rowData, index, data) -> data[index] = null;
            case BOOLEAN:
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case TINYINT:
                return (rowData, index, data) -> data[index] = rowData.getByte(index);
            case SMALLINT:
                return (rowData, index, data) -> data[index] = rowData.getShort(index);
            case INTEGER:
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case BIGINT:
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case DATE:
                return (rowData, index, data) -> {
                    data[index] = Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)));
                };
            case FLOAT:
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case DOUBLE:
                return (rowData, index, data) -> data[index] = rowData.getDouble(index);
            case CHAR:
            case VARCHAR:
                return (rowData, index, data) -> data[index] = rowData.getString(index).toString();
            case DECIMAL:
                return (rowData, index, data) -> {
                    int precision = ((DecimalType) type).getPrecision();
                    int scale = ((DecimalType) type).getScale();
                    HiveDecimal hiveDecimal = HiveDecimal.create(rowData.getDecimal(index, precision, scale).toBigDecimal());
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, precision, scale);
                    data[index] = new HiveDecimalWritable(hiveDecimal);
                };
            case BINARY:
            case VARBINARY:
                return (rowData, index, data) -> data[index] = new BytesWritable(rowData.getBinary(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, data) -> data[index] = rowData.getTimestamp(index, ((TimestampType)type).getPrecision()).toTimestamp();
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
