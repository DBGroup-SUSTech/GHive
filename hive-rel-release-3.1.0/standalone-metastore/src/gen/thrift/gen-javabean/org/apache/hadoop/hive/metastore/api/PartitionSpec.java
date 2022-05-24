/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class PartitionSpec implements org.apache.thrift.TBase<PartitionSpec, PartitionSpec._Fields>, java.io.Serializable, Cloneable, Comparable<PartitionSpec> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PartitionSpec");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tableName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField ROOT_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("rootPath", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField SHARED_SDPARTITION_SPEC_FIELD_DESC = new org.apache.thrift.protocol.TField("sharedSDPartitionSpec", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField PARTITION_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("partitionList", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField CAT_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("catName", org.apache.thrift.protocol.TType.STRING, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new PartitionSpecStandardSchemeFactory());
    schemes.put(TupleScheme.class, new PartitionSpecTupleSchemeFactory());
  }

  private String dbName; // required
  private String tableName; // required
  private String rootPath; // required
  private PartitionSpecWithSharedSD sharedSDPartitionSpec; // optional
  private PartitionListComposingSpec partitionList; // optional
  private String catName; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short)1, "dbName"),
    TABLE_NAME((short)2, "tableName"),
    ROOT_PATH((short)3, "rootPath"),
    SHARED_SDPARTITION_SPEC((short)4, "sharedSDPartitionSpec"),
    PARTITION_LIST((short)5, "partitionList"),
    CAT_NAME((short)6, "catName");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DB_NAME
          return DB_NAME;
        case 2: // TABLE_NAME
          return TABLE_NAME;
        case 3: // ROOT_PATH
          return ROOT_PATH;
        case 4: // SHARED_SDPARTITION_SPEC
          return SHARED_SDPARTITION_SPEC;
        case 5: // PARTITION_LIST
          return PARTITION_LIST;
        case 6: // CAT_NAME
          return CAT_NAME;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.SHARED_SDPARTITION_SPEC,_Fields.PARTITION_LIST,_Fields.CAT_NAME};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLE_NAME, new org.apache.thrift.meta_data.FieldMetaData("tableName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ROOT_PATH, new org.apache.thrift.meta_data.FieldMetaData("rootPath", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SHARED_SDPARTITION_SPEC, new org.apache.thrift.meta_data.FieldMetaData("sharedSDPartitionSpec", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PartitionSpecWithSharedSD.class)));
    tmpMap.put(_Fields.PARTITION_LIST, new org.apache.thrift.meta_data.FieldMetaData("partitionList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PartitionListComposingSpec.class)));
    tmpMap.put(_Fields.CAT_NAME, new org.apache.thrift.meta_data.FieldMetaData("catName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PartitionSpec.class, metaDataMap);
  }

  public PartitionSpec() {
  }

  public PartitionSpec(
    String dbName,
    String tableName,
    String rootPath)
  {
    this();
    this.dbName = dbName;
    this.tableName = tableName;
    this.rootPath = rootPath;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PartitionSpec(PartitionSpec other) {
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetRootPath()) {
      this.rootPath = other.rootPath;
    }
    if (other.isSetSharedSDPartitionSpec()) {
      this.sharedSDPartitionSpec = new PartitionSpecWithSharedSD(other.sharedSDPartitionSpec);
    }
    if (other.isSetPartitionList()) {
      this.partitionList = new PartitionListComposingSpec(other.partitionList);
    }
    if (other.isSetCatName()) {
      this.catName = other.catName;
    }
  }

  public PartitionSpec deepCopy() {
    return new PartitionSpec(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tableName = null;
    this.rootPath = null;
    this.sharedSDPartitionSpec = null;
    this.partitionList = null;
    this.catName = null;
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  /** Returns true if field tableName is set (has been assigned a value) and false otherwise */
  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public void setTableNameIsSet(boolean value) {
    if (!value) {
      this.tableName = null;
    }
  }

  public String getRootPath() {
    return this.rootPath;
  }

  public void setRootPath(String rootPath) {
    this.rootPath = rootPath;
  }

  public void unsetRootPath() {
    this.rootPath = null;
  }

  /** Returns true if field rootPath is set (has been assigned a value) and false otherwise */
  public boolean isSetRootPath() {
    return this.rootPath != null;
  }

  public void setRootPathIsSet(boolean value) {
    if (!value) {
      this.rootPath = null;
    }
  }

  public PartitionSpecWithSharedSD getSharedSDPartitionSpec() {
    return this.sharedSDPartitionSpec;
  }

  public void setSharedSDPartitionSpec(PartitionSpecWithSharedSD sharedSDPartitionSpec) {
    this.sharedSDPartitionSpec = sharedSDPartitionSpec;
  }

  public void unsetSharedSDPartitionSpec() {
    this.sharedSDPartitionSpec = null;
  }

  /** Returns true if field sharedSDPartitionSpec is set (has been assigned a value) and false otherwise */
  public boolean isSetSharedSDPartitionSpec() {
    return this.sharedSDPartitionSpec != null;
  }

  public void setSharedSDPartitionSpecIsSet(boolean value) {
    if (!value) {
      this.sharedSDPartitionSpec = null;
    }
  }

  public PartitionListComposingSpec getPartitionList() {
    return this.partitionList;
  }

  public void setPartitionList(PartitionListComposingSpec partitionList) {
    this.partitionList = partitionList;
  }

  public void unsetPartitionList() {
    this.partitionList = null;
  }

  /** Returns true if field partitionList is set (has been assigned a value) and false otherwise */
  public boolean isSetPartitionList() {
    return this.partitionList != null;
  }

  public void setPartitionListIsSet(boolean value) {
    if (!value) {
      this.partitionList = null;
    }
  }

  public String getCatName() {
    return this.catName;
  }

  public void setCatName(String catName) {
    this.catName = catName;
  }

  public void unsetCatName() {
    this.catName = null;
  }

  /** Returns true if field catName is set (has been assigned a value) and false otherwise */
  public boolean isSetCatName() {
    return this.catName != null;
  }

  public void setCatNameIsSet(boolean value) {
    if (!value) {
      this.catName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String)value);
      }
      break;

    case ROOT_PATH:
      if (value == null) {
        unsetRootPath();
      } else {
        setRootPath((String)value);
      }
      break;

    case SHARED_SDPARTITION_SPEC:
      if (value == null) {
        unsetSharedSDPartitionSpec();
      } else {
        setSharedSDPartitionSpec((PartitionSpecWithSharedSD)value);
      }
      break;

    case PARTITION_LIST:
      if (value == null) {
        unsetPartitionList();
      } else {
        setPartitionList((PartitionListComposingSpec)value);
      }
      break;

    case CAT_NAME:
      if (value == null) {
        unsetCatName();
      } else {
        setCatName((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DB_NAME:
      return getDbName();

    case TABLE_NAME:
      return getTableName();

    case ROOT_PATH:
      return getRootPath();

    case SHARED_SDPARTITION_SPEC:
      return getSharedSDPartitionSpec();

    case PARTITION_LIST:
      return getPartitionList();

    case CAT_NAME:
      return getCatName();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DB_NAME:
      return isSetDbName();
    case TABLE_NAME:
      return isSetTableName();
    case ROOT_PATH:
      return isSetRootPath();
    case SHARED_SDPARTITION_SPEC:
      return isSetSharedSDPartitionSpec();
    case PARTITION_LIST:
      return isSetPartitionList();
    case CAT_NAME:
      return isSetCatName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof PartitionSpec)
      return this.equals((PartitionSpec)that);
    return false;
  }

  public boolean equals(PartitionSpec that) {
    if (that == null)
      return false;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_rootPath = true && this.isSetRootPath();
    boolean that_present_rootPath = true && that.isSetRootPath();
    if (this_present_rootPath || that_present_rootPath) {
      if (!(this_present_rootPath && that_present_rootPath))
        return false;
      if (!this.rootPath.equals(that.rootPath))
        return false;
    }

    boolean this_present_sharedSDPartitionSpec = true && this.isSetSharedSDPartitionSpec();
    boolean that_present_sharedSDPartitionSpec = true && that.isSetSharedSDPartitionSpec();
    if (this_present_sharedSDPartitionSpec || that_present_sharedSDPartitionSpec) {
      if (!(this_present_sharedSDPartitionSpec && that_present_sharedSDPartitionSpec))
        return false;
      if (!this.sharedSDPartitionSpec.equals(that.sharedSDPartitionSpec))
        return false;
    }

    boolean this_present_partitionList = true && this.isSetPartitionList();
    boolean that_present_partitionList = true && that.isSetPartitionList();
    if (this_present_partitionList || that_present_partitionList) {
      if (!(this_present_partitionList && that_present_partitionList))
        return false;
      if (!this.partitionList.equals(that.partitionList))
        return false;
    }

    boolean this_present_catName = true && this.isSetCatName();
    boolean that_present_catName = true && that.isSetCatName();
    if (this_present_catName || that_present_catName) {
      if (!(this_present_catName && that_present_catName))
        return false;
      if (!this.catName.equals(that.catName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_dbName = true && (isSetDbName());
    list.add(present_dbName);
    if (present_dbName)
      list.add(dbName);

    boolean present_tableName = true && (isSetTableName());
    list.add(present_tableName);
    if (present_tableName)
      list.add(tableName);

    boolean present_rootPath = true && (isSetRootPath());
    list.add(present_rootPath);
    if (present_rootPath)
      list.add(rootPath);

    boolean present_sharedSDPartitionSpec = true && (isSetSharedSDPartitionSpec());
    list.add(present_sharedSDPartitionSpec);
    if (present_sharedSDPartitionSpec)
      list.add(sharedSDPartitionSpec);

    boolean present_partitionList = true && (isSetPartitionList());
    list.add(present_partitionList);
    if (present_partitionList)
      list.add(partitionList);

    boolean present_catName = true && (isSetCatName());
    list.add(present_catName);
    if (present_catName)
      list.add(catName);

    return list.hashCode();
  }

  @Override
  public int compareTo(PartitionSpec other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(other.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, other.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTableName()).compareTo(other.isSetTableName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableName, other.tableName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRootPath()).compareTo(other.isSetRootPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRootPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rootPath, other.rootPath);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSharedSDPartitionSpec()).compareTo(other.isSetSharedSDPartitionSpec());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSharedSDPartitionSpec()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sharedSDPartitionSpec, other.sharedSDPartitionSpec);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPartitionList()).compareTo(other.isSetPartitionList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartitionList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partitionList, other.partitionList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCatName()).compareTo(other.isSetCatName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCatName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.catName, other.catName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PartitionSpec(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("rootPath:");
    if (this.rootPath == null) {
      sb.append("null");
    } else {
      sb.append(this.rootPath);
    }
    first = false;
    if (isSetSharedSDPartitionSpec()) {
      if (!first) sb.append(", ");
      sb.append("sharedSDPartitionSpec:");
      if (this.sharedSDPartitionSpec == null) {
        sb.append("null");
      } else {
        sb.append(this.sharedSDPartitionSpec);
      }
      first = false;
    }
    if (isSetPartitionList()) {
      if (!first) sb.append(", ");
      sb.append("partitionList:");
      if (this.partitionList == null) {
        sb.append("null");
      } else {
        sb.append(this.partitionList);
      }
      first = false;
    }
    if (isSetCatName()) {
      if (!first) sb.append(", ");
      sb.append("catName:");
      if (this.catName == null) {
        sb.append("null");
      } else {
        sb.append(this.catName);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (sharedSDPartitionSpec != null) {
      sharedSDPartitionSpec.validate();
    }
    if (partitionList != null) {
      partitionList.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class PartitionSpecStandardSchemeFactory implements SchemeFactory {
    public PartitionSpecStandardScheme getScheme() {
      return new PartitionSpecStandardScheme();
    }
  }

  private static class PartitionSpecStandardScheme extends StandardScheme<PartitionSpec> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, PartitionSpec struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TABLE_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tableName = iprot.readString();
              struct.setTableNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ROOT_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.rootPath = iprot.readString();
              struct.setRootPathIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // SHARED_SDPARTITION_SPEC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.sharedSDPartitionSpec = new PartitionSpecWithSharedSD();
              struct.sharedSDPartitionSpec.read(iprot);
              struct.setSharedSDPartitionSpecIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // PARTITION_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.partitionList = new PartitionListComposingSpec();
              struct.partitionList.read(iprot);
              struct.setPartitionListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // CAT_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.catName = iprot.readString();
              struct.setCatNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, PartitionSpec struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeString(struct.tableName);
        oprot.writeFieldEnd();
      }
      if (struct.rootPath != null) {
        oprot.writeFieldBegin(ROOT_PATH_FIELD_DESC);
        oprot.writeString(struct.rootPath);
        oprot.writeFieldEnd();
      }
      if (struct.sharedSDPartitionSpec != null) {
        if (struct.isSetSharedSDPartitionSpec()) {
          oprot.writeFieldBegin(SHARED_SDPARTITION_SPEC_FIELD_DESC);
          struct.sharedSDPartitionSpec.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.partitionList != null) {
        if (struct.isSetPartitionList()) {
          oprot.writeFieldBegin(PARTITION_LIST_FIELD_DESC);
          struct.partitionList.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.catName != null) {
        if (struct.isSetCatName()) {
          oprot.writeFieldBegin(CAT_NAME_FIELD_DESC);
          oprot.writeString(struct.catName);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class PartitionSpecTupleSchemeFactory implements SchemeFactory {
    public PartitionSpecTupleScheme getScheme() {
      return new PartitionSpecTupleScheme();
    }
  }

  private static class PartitionSpecTupleScheme extends TupleScheme<PartitionSpec> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, PartitionSpec struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDbName()) {
        optionals.set(0);
      }
      if (struct.isSetTableName()) {
        optionals.set(1);
      }
      if (struct.isSetRootPath()) {
        optionals.set(2);
      }
      if (struct.isSetSharedSDPartitionSpec()) {
        optionals.set(3);
      }
      if (struct.isSetPartitionList()) {
        optionals.set(4);
      }
      if (struct.isSetCatName()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetRootPath()) {
        oprot.writeString(struct.rootPath);
      }
      if (struct.isSetSharedSDPartitionSpec()) {
        struct.sharedSDPartitionSpec.write(oprot);
      }
      if (struct.isSetPartitionList()) {
        struct.partitionList.write(oprot);
      }
      if (struct.isSetCatName()) {
        oprot.writeString(struct.catName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, PartitionSpec struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.dbName = iprot.readString();
        struct.setDbNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tableName = iprot.readString();
        struct.setTableNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.rootPath = iprot.readString();
        struct.setRootPathIsSet(true);
      }
      if (incoming.get(3)) {
        struct.sharedSDPartitionSpec = new PartitionSpecWithSharedSD();
        struct.sharedSDPartitionSpec.read(iprot);
        struct.setSharedSDPartitionSpecIsSet(true);
      }
      if (incoming.get(4)) {
        struct.partitionList = new PartitionListComposingSpec();
        struct.partitionList.read(iprot);
        struct.setPartitionListIsSet(true);
      }
      if (incoming.get(5)) {
        struct.catName = iprot.readString();
        struct.setCatNameIsSet(true);
      }
    }
  }

}

