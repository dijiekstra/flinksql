package org.apache.flink.formats.json.canal;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;


public class CanalJsonSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;
	private final JsonRowDataSerializationSchema jsonSerializer;
	private final RowType rowType;

	public CanalJsonSerializationSchema(RowType rowType, TimestampFormat timestampOption) {
		this.rowType = rowType;
		RowType finalType = updateRowType(fromLogicalToDataType(rowType));
		jsonSerializer = new JsonRowDataSerializationSchema(finalType, timestampOption);
	}

	@Override
	public byte[] serialize(RowData element) {

		//wrap element , json -> canal-json
		RowData rowData = wrapElement(element);

		// 序列化还交给他

		return jsonSerializer.serialize(rowData);
	}

	private RowData wrapElement(RowData element){
		RowKind rowKind = element.getRowKind();
		GenericRowData data = new GenericRowData(3);

		List<RowType.RowField> fields = rowType.getFields();
		GenericRowData realData = new GenericRowData(fields.size());
		for (int i = 0; i < fields.size(); i++) {
			RowType.RowField rowField = fields.get(i);

			realData.setField(i,RowData.get(element,i,rowField.getType()));

//			switch (typeRoot) {
//				case NULL:
//					realData.setField(i,null);
//				case BOOLEAN:
//					realData.setField(i,element.getBoolean(i));
//				case TINYINT:
//					realData.setField(i,element.getByte(i));
//				case SMALLINT:
//					realData.setField(i,element.getShort(i));
//				case INTEGER:
//				case INTERVAL_YEAR_MONTH:
//					realData.setField(i,element.getInt(i));
//				case BIGINT:
//				case INTERVAL_DAY_TIME:
//					realData.setField(i,element.getLong(i));
//				case FLOAT:
//					realData.setField(i,element.getFloat(i);
//				case DOUBLE:
//					realData.setField(i,element.getDouble(i));
//				case CHAR:
//				case VARCHAR:
//					// value is BinaryString
//					realData.setField(i,element.getString(i));
//				case BINARY:
//				case VARBINARY:
//					realData.setField(i,element.getBinary(i));
//				case DATE:
//					realData.setField(i,RowData.get(element,i,rowField.getType()));
//				case TIME_WITHOUT_TIME_ZONE:
//					realData.setField(i,element);
//				case TIMESTAMP_WITHOUT_TIME_ZONE:
//					realData.setField(i,element);
//				case DECIMAL:
//					realData.setField(i,element);
//				case ARRAY:
//					realData.setField(i,element);
//				case MAP:
//				case MULTISET:
//					realData.setField(i,element);
//				case ROW:
//					realData.setField(i,element);
//				case RAW:
//				default:
//					throw new UnsupportedOperationException("Not support to parse type: " + rowField.getType());
//			}
		}
		Object[] objects = new Object[1];

		objects[0] = realData;
		ArrayData genericArrayData = new GenericArrayData(objects);
		data.setField(0,genericArrayData);
		data.setField(1,null);


		// 参照了一下 CanalJsonDeserializationSchema ，update before和after好像需要组合在一起，
		// 先把before解成delete看看什么情况
		// 如果非要解析在一起，那么需要搞个map存一下before的值，等对应的after来了，把值塞进去，问题来了，你怎么知道是对应的after呢
		// 而且感觉这样内存会炸，先试一下delete
		// 2020年08月04日10:09:45 参考了一下https://help.aliyun.com/document_detail/65670.html?spm=a2c4g.11186623.6.859.6c507db5NcA7gg这个
		// 感觉还是把insert和update after解析成 insert、别的解析成delete靠谱
		// 2020年08月05日14:59:43 新思路，如果两条数据来的时间一样，那就认为是同一条数据，然后组合一起，下次再实现
		StringData sd = null;
		switch (rowKind) {
			case DELETE:
			case UPDATE_BEFORE:
				sd = new BinaryStringData("DELETE");
				break;
			default:
				sd = new BinaryStringData("INSERT");
				break;
		}
		data.setField(2,sd);

		return data;
	}

	private RowType updateRowType(DataType rowType) {


		// 因为需要增加字段，所以搞个方法包装一下
		// 看了一眼CanalJsonDeserializationSchema的createJsonRowType方法，好像只要3个字段？

		return (RowType) DataTypes.ROW(
			DataTypes.FIELD("data", DataTypes.ARRAY(rowType)),
			DataTypes.FIELD("old", DataTypes.ARRAY(rowType)),
			DataTypes.FIELD("type", DataTypes.STRING())).getLogicalType();

	}


}
