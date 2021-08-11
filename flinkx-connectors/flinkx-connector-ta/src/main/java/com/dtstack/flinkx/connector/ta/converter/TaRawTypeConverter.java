package com.dtstack.flinkx.connector.ta.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import java.util.Locale;

public class TaRawTypeConverter {
    public static DataType apply(String type) throws UnsupportedTypeException {
        switch (type.toUpperCase(Locale.ENGLISH)) {
                case "BOOLEAN":
                    return DataTypes.BOOLEAN();
                case "TINYINT":
                    return DataTypes.TINYINT();
                case "SMALLINT":
                    return DataTypes.SMALLINT();
                case "INT":
                    return DataTypes.INT();
                case "BIGINT":
                    return DataTypes.BIGINT();
                case "FLOAT":
                    return DataTypes.FLOAT();
                case "DOUBLE":
                    return DataTypes.DOUBLE();
                case "DECIMAL":

                    return DataTypes.DECIMAL(38, 18);
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                    return DataTypes.STRING();
                case "BINARY":
                    return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
                case "TIMESTAMP":
                    return DataTypes.TIMESTAMP(6);
                case "DATE":
                    return DataTypes.DATE();
                case "ARRAY":
                case "MAP":
                case "STRUCT":
                case "UNION":
                default:
                    throw new UnsupportedTypeException(type);

        }
    }
}
