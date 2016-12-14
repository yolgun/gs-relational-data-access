package com.bytro.firefly.sql;

/**
 * This class represents an SQL column.
 */
public class SQLColumn {
	private String name;
	private boolean autoIncrement;
	private String type;
	private int typeCode;

	public String getName() {
		return name;
	}

	public SQLColumn setName(String name) {
		this.name = name;
		return this;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public SQLColumn setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
		return this;
	}

	public String getType() {
		return type;
	}

	public SQLColumn setType(String type) {
		this.type = type;
		return this;
	}

	public int getTypeCode() {
		return typeCode;
	}

	public SQLColumn setTypeCode(int typeCode) {
		this.typeCode = typeCode;
		return this;
	}

	@Override
	public String toString() {
		return "SQLColumn{" +
				"name='" + name + '\'' +
				", autoIncrement=" + autoIncrement +
				", type='" + type + '\'' +
				", typeCode=" + typeCode +
				'}';
	}
}
