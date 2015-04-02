/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.phinence.spark.storage;  
@SuppressWarnings("all")
public class Company extends org.apache.gora.persistency.impl.PersistentBase implements org.apache.avro.specific.SpecificRecord, org.apache.gora.persistency.Persistent, org.apache.gora.orientdb.storage.test.Vertex {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Company\",\"namespace\":\"com.phinence.spark.storage\",\"fields\":[{\"name\":\"__g__dirty\",\"type\":\"bytes\",\"doc\":\"Bytes used to represent weather or not a field is dirty.\",\"default\":\"AA==\"},{\"name\":\"Name\",\"type\":[\"string\",\"null\"]},{\"name\":\"Code\",\"type\":[\"string\",\"null\"]},{\"name\":\"Ticker\",\"type\":[\"string\",\"null\"]},{\"name\":\"edges\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"EdgeComp\",\"fields\":[{\"name\":\"__g__dirty\",\"type\":\"bytes\",\"doc\":\"Bytes used to represent weather or not a field is dirty.\",\"default\":\"AA==\"},{\"name\":\"label\",\"type\":[\"string\",\"null\"]},{\"name\":\"target\",\"type\":[{\"type\":\"record\",\"name\":\"Ohlc\",\"fields\":[{\"name\":\"__g__dirty\",\"type\":\"bytes\",\"doc\":\"Bytes used to represent weather or not a field is dirty.\",\"default\":\"AA==\"},{\"name\":\"Open\",\"type\":\"float\",\"default\":0.0},{\"name\":\"High\",\"type\":\"float\",\"default\":0.0},{\"name\":\"Low\",\"type\":\"float\",\"default\":0.0},{\"name\":\"Close\",\"type\":\"float\",\"default\":0.0},{\"name\":\"Volume\",\"type\":\"long\",\"default\":0}]},\"null\"]}]}}}]}");
public int getFieldsCount(){
      return 2;
  }
  private Object key;
  
    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public void setKey(Object obj) {
        key = obj;
    }

  /** Enum containing all data bean's fields. */
  public static enum Field {
    __G__DIRTY(0, "__g__dirty"),
    NAME(1, "Name"),
    CODE(2, "Code"),
    TICKER(3, "Ticker"),
    EDGES(4, "edges"),
    ;
    /**
     * Field's index.
     */
    private int index;

    /**
     * Field's name.
     */
    private String name;

    /**
     * Field's constructor
     * @param index field's index.
     * @param name field's name.
     */
    Field(int index, String name) {this.index=index;this.name=name;}

    /**
     * Gets field's index.
     * @return int field's index.
     */
    public int getIndex() {return index;}

    /**
     * Gets field's name.
     * @return String field's name.
     */
    public String getName() {return name;}

    /**
     * Gets field's attributes to string.
     * @return String field's attributes to string.
     */
    public String toString() {return name;}
  };

  public static final String[] _ALL_FIELDS = {
  "__g__dirty",
  "Name",
  "Code",
  "Ticker",
  "edges",
  };

  /** Bytes used to represent weather or not a field is dirty. */
  private java.nio.ByteBuffer __g__dirty = java.nio.ByteBuffer.wrap(new byte[1]);
  private java.lang.CharSequence Name;
  private java.lang.CharSequence Code;
  private java.lang.CharSequence Ticker;
  private java.util.List<com.phinence.spark.storage.EdgeComp> edges;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return __g__dirty;
    case 1: return Name;
    case 2: return Code;
    case 3: return Ticker;
    case 4: return edges;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value) {
    switch (field$) {
    case 0: __g__dirty = (java.nio.ByteBuffer)(value); break;
    case 1: Name = (java.lang.CharSequence)(value); break;
    case 2: Code = (java.lang.CharSequence)(value); break;
    case 3: Ticker = (java.lang.CharSequence)(value); break;
    case 4: edges = (java.util.List<com.phinence.spark.storage.EdgeComp>)((value instanceof org.apache.gora.persistency.Dirtyable) ? value : new org.apache.gora.persistency.impl.DirtyListWrapper((java.util.List)value)); break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'Name' field.
   */
  public java.lang.CharSequence getName() {
    return Name;
  }

  /**
   * Sets the value of the 'Name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.Name = value;
    setDirty(1);
  }
  
  /**
   * Checks the dirty status of the 'Name' field. A field is dirty if it represents a change that has not yet been written to the database.
   * @param value the value to set.
   */
  public boolean isNameDirty(java.lang.CharSequence value) {
    return isDirty(1);
  }

  /**
   * Gets the value of the 'Code' field.
   */
  public java.lang.CharSequence getCode() {
    return Code;
  }

  /**
   * Sets the value of the 'Code' field.
   * @param value the value to set.
   */
  public void setCode(java.lang.CharSequence value) {
    this.Code = value;
    setDirty(2);
  }
  
  /**
   * Checks the dirty status of the 'Code' field. A field is dirty if it represents a change that has not yet been written to the database.
   * @param value the value to set.
   */
  public boolean isCodeDirty(java.lang.CharSequence value) {
    return isDirty(2);
  }

  /**
   * Gets the value of the 'Ticker' field.
   */
  public java.lang.CharSequence getTicker() {
    return Ticker;
  }

  /**
   * Sets the value of the 'Ticker' field.
   * @param value the value to set.
   */
  public void setTicker(java.lang.CharSequence value) {
    this.Ticker = value;
    setDirty(3);
  }
  
  /**
   * Checks the dirty status of the 'Ticker' field. A field is dirty if it represents a change that has not yet been written to the database.
   * @param value the value to set.
   */
  public boolean isTickerDirty(java.lang.CharSequence value) {
    return isDirty(3);
  }

  /**
   * Gets the value of the 'edges' field.
   */
  public java.util.List<com.phinence.spark.storage.EdgeComp> getEdges() {
    return edges;
  }

  /**
   * Sets the value of the 'edges' field.
   * @param value the value to set.
   */
  public void setEdges(java.util.List<com.phinence.spark.storage.EdgeComp> value) {
    this.edges = (value instanceof org.apache.gora.persistency.Dirtyable) ? value : new org.apache.gora.persistency.impl.DirtyListWrapper(value);
    setDirty(4);
  }
  
  /**
   * Checks the dirty status of the 'edges' field. A field is dirty if it represents a change that has not yet been written to the database.
   * @param value the value to set.
   */
  public boolean isEdgesDirty(java.util.List<com.phinence.spark.storage.EdgeComp> value) {
    return isDirty(4);
  }

  /** Creates a new Company RecordBuilder */
  public static com.phinence.spark.storage.Company.Builder newBuilder() {
    return new com.phinence.spark.storage.Company.Builder();
  }
  
  /** Creates a new Company RecordBuilder by copying an existing Builder */
  public static com.phinence.spark.storage.Company.Builder newBuilder(com.phinence.spark.storage.Company.Builder other) {
    return new com.phinence.spark.storage.Company.Builder(other);
  }
  
  /** Creates a new Company RecordBuilder by copying an existing Company instance */
  public static com.phinence.spark.storage.Company.Builder newBuilder(com.phinence.spark.storage.Company other) {
    return new com.phinence.spark.storage.Company.Builder(other);
  }
  
  private static java.nio.ByteBuffer deepCopyToWriteOnlyBuffer(
      java.nio.ByteBuffer input) {
    java.nio.ByteBuffer copy = java.nio.ByteBuffer.allocate(input.capacity());
    int position = input.position();
    input.reset();
    int mark = input.position();
    int limit = input.limit();
    input.rewind();
    input.limit(input.capacity());
    copy.put(input);
    input.rewind();
    copy.rewind();
    input.position(mark);
    input.mark();
    copy.position(mark);
    copy.mark();
    input.position(position);
    copy.position(position);
    input.limit(limit);
    copy.limit(limit);
    return copy.asReadOnlyBuffer();
  }
  
  /**
   * RecordBuilder for Company instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Company>
    implements org.apache.avro.data.RecordBuilder<Company> {

    private java.nio.ByteBuffer __g__dirty;
    private java.lang.CharSequence Name;
    private java.lang.CharSequence Code;
    private java.lang.CharSequence Ticker;
    private java.util.List<com.phinence.spark.storage.EdgeComp> edges;

    /** Creates a new Builder */
    private Builder() {
      super(com.phinence.spark.storage.Company.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.phinence.spark.storage.Company.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing Company instance */
    private Builder(com.phinence.spark.storage.Company other) {
            super(com.phinence.spark.storage.Company.SCHEMA$);
      if (isValidValue(fields()[0], other.__g__dirty)) {
        this.__g__dirty = (java.nio.ByteBuffer) data().deepCopy(fields()[0].schema(), other.__g__dirty);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Name)) {
        this.Name = (java.lang.CharSequence) data().deepCopy(fields()[1].schema(), other.Name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Code)) {
        this.Code = (java.lang.CharSequence) data().deepCopy(fields()[2].schema(), other.Code);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.Ticker)) {
        this.Ticker = (java.lang.CharSequence) data().deepCopy(fields()[3].schema(), other.Ticker);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.edges)) {
        this.edges = (java.util.List<com.phinence.spark.storage.EdgeComp>) data().deepCopy(fields()[4].schema(), other.edges);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'Name' field */
    public java.lang.CharSequence getName() {
      return Name;
    }
    
    /** Sets the value of the 'Name' field */
    public com.phinence.spark.storage.Company.Builder setName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.Name = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'Name' field has been set */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'Name' field */
    public com.phinence.spark.storage.Company.Builder clearName() {
      Name = null;
      fieldSetFlags()[1] = false;
      return this;
    }
    
    /** Gets the value of the 'Code' field */
    public java.lang.CharSequence getCode() {
      return Code;
    }
    
    /** Sets the value of the 'Code' field */
    public com.phinence.spark.storage.Company.Builder setCode(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.Code = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'Code' field has been set */
    public boolean hasCode() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'Code' field */
    public com.phinence.spark.storage.Company.Builder clearCode() {
      Code = null;
      fieldSetFlags()[2] = false;
      return this;
    }
    
    /** Gets the value of the 'Ticker' field */
    public java.lang.CharSequence getTicker() {
      return Ticker;
    }
    
    /** Sets the value of the 'Ticker' field */
    public com.phinence.spark.storage.Company.Builder setTicker(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.Ticker = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'Ticker' field has been set */
    public boolean hasTicker() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'Ticker' field */
    public com.phinence.spark.storage.Company.Builder clearTicker() {
      Ticker = null;
      fieldSetFlags()[3] = false;
      return this;
    }
    
    /** Gets the value of the 'edges' field */
    public java.util.List<com.phinence.spark.storage.EdgeComp> getEdges() {
      return edges;
    }
    
    /** Sets the value of the 'edges' field */
    public com.phinence.spark.storage.Company.Builder setEdges(java.util.List<com.phinence.spark.storage.EdgeComp> value) {
      validate(fields()[4], value);
      this.edges = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'edges' field has been set */
    public boolean hasEdges() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'edges' field */
    public com.phinence.spark.storage.Company.Builder clearEdges() {
      edges = null;
      fieldSetFlags()[4] = false;
      return this;
    }
    
    @Override
    public Company build() {
      try {
        Company record = new Company();
        record.__g__dirty = fieldSetFlags()[0] ? this.__g__dirty : (java.nio.ByteBuffer) java.nio.ByteBuffer.wrap(new byte[1]);
        record.Name = fieldSetFlags()[1] ? this.Name : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.Code = fieldSetFlags()[2] ? this.Code : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.Ticker = fieldSetFlags()[3] ? this.Ticker : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.edges = fieldSetFlags()[4] ? this.edges : (java.util.List<com.phinence.spark.storage.EdgeComp>) new org.apache.gora.persistency.impl.DirtyListWrapper((java.util.List)defaultValue(fields()[4]));
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
  
  public Company.Tombstone getTombstone(){
  	return TOMBSTONE;
  }

  public Company newInstance(){
    return newBuilder().build();
  }

  private static final Tombstone TOMBSTONE = new Tombstone();
  
  public static final class Tombstone extends Company implements org.apache.gora.persistency.Tombstone {
  
      private Tombstone() { }
  
	  				  /**
	   * Gets the value of the 'Name' field.
		   */
	  public java.lang.CharSequence getName() {
	    throw new java.lang.UnsupportedOperationException("Get is not supported on tombstones");
	  }
	
	  /**
	   * Sets the value of the 'Name' field.
		   * @param value the value to set.
	   */
	  public void setName(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("Set is not supported on tombstones");
	  }
	  
	  /**
	   * Checks the dirty status of the 'Name' field. A field is dirty if it represents a change that has not yet been written to the database.
		   * @param value the value to set.
	   */
	  public boolean isNameDirty(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("IsDirty is not supported on tombstones");
	  }
	
				  /**
	   * Gets the value of the 'Code' field.
		   */
	  public java.lang.CharSequence getCode() {
	    throw new java.lang.UnsupportedOperationException("Get is not supported on tombstones");
	  }
	
	  /**
	   * Sets the value of the 'Code' field.
		   * @param value the value to set.
	   */
	  public void setCode(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("Set is not supported on tombstones");
	  }
	  
	  /**
	   * Checks the dirty status of the 'Code' field. A field is dirty if it represents a change that has not yet been written to the database.
		   * @param value the value to set.
	   */
	  public boolean isCodeDirty(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("IsDirty is not supported on tombstones");
	  }
	
				  /**
	   * Gets the value of the 'Ticker' field.
		   */
	  public java.lang.CharSequence getTicker() {
	    throw new java.lang.UnsupportedOperationException("Get is not supported on tombstones");
	  }
	
	  /**
	   * Sets the value of the 'Ticker' field.
		   * @param value the value to set.
	   */
	  public void setTicker(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("Set is not supported on tombstones");
	  }
	  
	  /**
	   * Checks the dirty status of the 'Ticker' field. A field is dirty if it represents a change that has not yet been written to the database.
		   * @param value the value to set.
	   */
	  public boolean isTickerDirty(java.lang.CharSequence value) {
	    throw new java.lang.UnsupportedOperationException("IsDirty is not supported on tombstones");
	  }
	
				  /**
	   * Gets the value of the 'edges' field.
		   */
	  public java.util.List<com.phinence.spark.storage.EdgeComp> getEdges() {
	    throw new java.lang.UnsupportedOperationException("Get is not supported on tombstones");
	  }
	
	  /**
	   * Sets the value of the 'edges' field.
		   * @param value the value to set.
	   */
	  public void setEdges(java.util.List<com.phinence.spark.storage.EdgeComp> value) {
	    throw new java.lang.UnsupportedOperationException("Set is not supported on tombstones");
	  }
	  
	  /**
	   * Checks the dirty status of the 'edges' field. A field is dirty if it represents a change that has not yet been written to the database.
		   * @param value the value to set.
	   */
	  public boolean isEdgesDirty(java.util.List<com.phinence.spark.storage.EdgeComp> value) {
	    throw new java.lang.UnsupportedOperationException("IsDirty is not supported on tombstones");
	  }
	
		  
  }
  
}