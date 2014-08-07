/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.orientdb.store;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.gora.orientdb.query.OrientDBQuery;
import org.apache.gora.orientdb.query.OrientDBResult;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.BeanFactoryImpl;
import org.apache.gora.persistency.impl.DirtyListWrapper;
import org.apache.gora.persistency.impl.DirtyMapWrapper;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.ClassLoadingUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;


/**
 *
 * @author Nicolas
 * @param <K>
 * @param <T>
 */
public class OrientDBStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {
    
    /* Configuration Properties */
    protected static final String USERNAME_PROPERTY = "orientdb.user";
    protected static final String PASSWORD_PROPERTY = "orientdb.password";
    protected static final String HOST_PROPERTY = "orientdb.host";
    protected static final String URL_PROPERTY = "orientdb.url";
    protected static final String MAPPING_FILE_PROP = "orientdb.mapping.file";
    
    protected static final String DEFAULT_MAPPING_FILE = "gora-orientdb-mapping.xml";
    
    public static final int MAX_RETRIES = 5;
    
    /* OrientDB Client */
   // private OrientGraphFactory factory;  // Fix Me swwitch to Orient document factory
    public static ODatabaseDocumentPool factory;
    private ODatabaseDocumentTx odb;
    private OClass className;
    private OServerAdmin admin;
    
    
    /* Mapping definition */
    private OrientDBMapping mapping;
    
    public static final Logger LOG = LoggerFactory.getLogger(OrientDBStore.class);
    
    public static String nameOfPassword;
    public static String nameOfUser;
    public static String nameOfUrl;
    public static String nameOfHost;
    
    @Override
    public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
        try {
            LOG.debug("Initializing OrientDB store");
            super.initialize(keyClass, persistentClass, properties);
            //properties = DataStoreFactory.createProps();
            if(properties==null)
                throw new IllegalStateException("Impossible to initialize without properties");
            LOG.debug("Display url of orientdb : "+properties.getProperty("gora.datastore."+URL_PROPERTY));
            // Load of mapping
            OrientDBMappingBuilder builder = new OrientDBMappingBuilder();  
            String nameOfMappingFile = properties.getProperty("gora.datastore."+MAPPING_FILE_PROP,DEFAULT_MAPPING_FILE);
            nameOfHost = properties.getProperty("gora.datastore."+HOST_PROPERTY);
            nameOfUrl = properties.getProperty("gora.datastore."+URL_PROPERTY);
            nameOfUser = properties.getProperty("gora.datastore."+USERNAME_PROPERTY);
            nameOfPassword = properties.getProperty("gora.datastore."+PASSWORD_PROPERTY);
            
            LOG.debug("Initializing Orient store with mapping {}.",new Object[]{nameOfMappingFile});
            mapping = new OrientDBMapping();
            builder.fromFile(nameOfMappingFile);
            mapping = builder.build();
            
            OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
            Orient.instance().registerEngine(new OEngineRemote());
            factory = new ODatabaseDocumentPool();
            factory.setup(1,100);
            odb = factory.acquire(nameOfHost+":"+nameOfUrl, nameOfUser, nameOfPassword);  // replace by factory.acquire
            admin = new OServerAdmin(nameOfHost+":"+nameOfUrl);
            admin.connect(nameOfUser, nameOfPassword);
            //graph.getRawGraph().begin();
        
            LOG.info("Initialized Orient Store for host {} and url {}", 
                    new Object[]{
                        nameOfHost,
                        nameOfUrl
                    });
            
        } catch(IOException e) {
            LOG.error("Error while initializing OrientDB store: {}",
					new Object[]{e.getMessage()});
			throw new RuntimeException(e);
        } catch( Exception e) {
            LOG.error("Generic error while initializing"+e.getLocalizedMessage(),e.fillInStackTrace());
            //throw e;
        }
    }
    
    @Override
    public String getSchemaName() {
        return mapping.getOClassName();
    }

    @Override
    public void createSchema() {
        //graph = factory.getTx();
       // if( graph == null)
         //   throw new IllegalStateException("Impossible to create schema. No database loaded");
        odb = factory.acquire(nameOfHost+":"+nameOfUrl, nameOfUser, nameOfPassword);
        if(!schemaExists()) {
            className = odb.getMetadata().getSchema().createClass(mapping.getOClassName(),odb.getMetadata().getSchema().getClass("V"));
            className.createProperty("id",OType.STRING);
            className.createIndex(mapping.getOClassName()+".id", OClass.INDEX_TYPE.UNIQUE, "id");
            odb.getMetadata().getSchema().save();
            //odict = odb.getMetadata().getIndexManager().createIndex("kv", OClass.INDEX_TYPE.DICTIONARY.toString(), (OIndexDefinition)new OSimpleKeyIndexDefinition(OType.STRING), null, null, null);
        }else
            className = odb.getMetadata().getSchema().getClass(mapping.getOClassName());
        LOG.warn("Class {} has been created", new Object[]{mapping.getOClassName()});
    }

    @Override
    public void deleteSchema() {
        ODatabaseDocumentTx od = factory.acquire(nameOfHost+":"+nameOfUrl, nameOfUser, nameOfPassword);
        if(schemaExists())
            od.getMetadata().getSchema().dropClass(mapping.getOClassName());
        od.close();
    }

    @Override
    public boolean schemaExists() {
        boolean result = false;
        try {
            result = odb.getMetadata().getSchema().existsClass(mapping.getOClassName());
        }catch(NullPointerException e) {
            LOG.error("NullPointerException while checking the existence of the schema");
        }
        return result;
    }

    @Override
    public T get(K key, String[] fields) {
        // To do: Create the query with @param fields
        odb.begin();
        OIndex odict = odb.getMetadata().getIndexManager().getIndex(getSchemaName()+".id");
        //String oKey = key.toString().replace("\u00B7", ".");
        if(odict.get(key)==null)
            return null;
        ODocument result = odb.getRecord((OIdentifiable)odict.get(key));
        odb.commit();
        if(result==null)
            throw new IllegalStateException("No vertex with this key : "+key);
        T persistent = null;
        try{
            persistent = newInstanceT(result,fields);
        }catch(Exception e) {
            LOG.error("Unknown error : ", e.fillInStackTrace());
        }
        if(persistent!=null){
            LOG.debug("persistent class not empty");
            persistent.clearDirty();
        }else {
            //graph.shutdown();
            LOG.error("The persistentClass {} is empty", new Object[]{persistent});
            return null;
        }
        return persistent;
    }

    @Override
    public void put(K key, T obj) {
        if(obj.isDirty()) {
            try{
                putUpdate(key,obj);
                //odb.commit();
            }catch(Exception e){  //Temporary
                LOG.info("Error on commiting update",e.fillInStackTrace());
            }finally{
                
                
            }
            LOG.info("The object  with key : "+key.toString()+"  is added to the db");
        }
        else
            LOG.warn("Ignored putting object {} in the store as it is not dirty",new Object[]{obj});
        
        obj.clearDirty();
    }

    @Override
    public boolean delete(K key) {
        odb.begin();
        OIndex odict = odb.getMetadata().getIndexManager().getIndex(getSchemaName()+".id");
        if(odict.get(key)!=null)
            odb.delete((ORID)odict.get(key));
        return true;
    }

    @Override
    public long deleteByQuery(Query<K, T> query) {
        odb.begin();
        query.setLocalFilterEnabled(false);
        OrientDBResult orientResult = new OrientDBResult(this,query);
        String quer = OrientDBQuery.getSQLQuery(query, mapping.getOClassName());
        orientResult.setOdb(odb);
        List<ODocument> result;
        int cursor = 0;
        try{
            result = odb.query(new OSQLSynchQuery<ODocument>(quer));
            orientResult.setItVert(result);
            for(int i=0; i<result.size();i++){
                odb.delete((ORID)result.get(i));
                //orientResult.next();
                cursor = i;
            }
        } catch (Exception ex) {
            LOG.error("Error deleteByQuery at position {}"+orientResult.getCursor(),ex.fillInStackTrace());
            return cursor;
        }
        return 0;
    }

    @Override
    public Result<K, T> execute(Query<K, T> query) {
        odb.commit();
        odb.begin();
        OrientDBQuery orientQuery = new OrientDBQuery(this);
        OrientDBResult orientResult = new OrientDBResult(this,orientQuery);
        orientResult.setOdb(odb);
        List<ODocument> result = null;
        String quer = orientQuery.getSQLQuery(query, mapping.getOClassName());
        try{
            if(quer!=null)
                result = odb.query(new OSQLSynchQuery<ODocument>(quer));
            //odb.commit();
            LOG.warn("The query is : "+quer);
            orientResult.setItVert(result);
            LOG.warn("OrientResult size : "+orientResult.getItVert().size());
        }catch(OResponseProcessingException e){
            LOG.error("Unable to execute the query {}",e.fillInStackTrace());
        }
        return orientResult;
    }

    @Override
    public Query<K, T> newQuery() {
        OrientDBQuery orientQuery = new OrientDBQuery(this);
        orientQuery.setFields(getFieldsToQuery(null));
        return orientQuery;
    }

    @Override
    public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query) throws IOException {
        //Sharding not yet implemented, only one partition 
        List<PartitionQuery<K, T>> partitions = new ArrayList<PartitionQuery<K, T>>();
        PartitionQueryImpl<K, T> partitionQuery = new PartitionQueryImpl<K, T>(
            query);
        partitionQuery.setConf(getConf());
        partitions.add(partitionQuery);
        return partitions;
    }

    @Override
    public void flush() {
        try {
            odb.commit();
        } catch (OTransactionException e) {
            LOG.error("Error on freezing database"+e.getMessage(),e.fillInStackTrace());
            flush();
        }
    }

    @Override
    public void close() {
        try{
            factory.close();
            odb.close();
        }catch(Exception e){
            LOG.error("Generic error while closing database",e.fillInStackTrace());
        }
        //odict.unload();
    }

    public T newInstanceT(ODocument doc, String[] fields) throws Exception {
        T persistent = newPersistent();
        try{
        odb.begin();
        String[] dbFields = getFieldsToQuery(fields);
        
        for(String f : dbFields) {
            String vertf = mapping.getVertexField(f);
            Field field = fieldMap.get(f);
            if ((vertf == null) || (!doc.containsField(vertf)) ||(doc.field(vertf)==null)) {
                //persistent.put(field.pos(), field.defaultValue());
                continue;
            }
            
            Schema fieldSchem = field.schema();
            LOG.info("Name of all fields:  "+f+";     FieldType"+fieldSchem.getType().getName()+";     "+fieldSchem.getType().name());
            
            Object result = fromDBObject(doc,field,fieldSchem,vertf);
            if(result!=null)
                persistent.put(field.pos(), result);
        }
        persistent.clearDirty();
        }catch(Exception e) {
            LOG.error("Erreur in newInstanceT",e.fillInStackTrace());
        }
        return persistent;
        
    }
    
    private DirtyMapWrapper fromDBMap(ODocument doc, Field field, Schema schem, String vertf){
        DirtyMapWrapper result = null;
        Map<String,Object> map = doc.field(vertf);
        /*if(map.size()==0)
            return result;*/
        Map<Utf8, Object> rmap = new HashMap<Utf8, Object>();
        for (Entry<String, Object> e : map.entrySet()) {
                // ensure Key decoding -> middle dots replaced with dots
                // FIXME: better approach ?
                String oKey = e.getKey().replace("\u00B7", "."); 

                switch(schem.getValueType().getType()) {
                case STRING:
                        rmap.put(new Utf8(oKey), new Utf8((String) e.getValue()));
                        break;
                case BYTES:
                        rmap.put(new Utf8(oKey), ByteBuffer.wrap((byte[]) e.getValue()));
                        break;
                case NULL:
                        rmap.put(new Utf8(oKey), null);
                        break;
                case UNION:
                    Schema inner = schem.getValueType().getTypes().get(1);
                    switch(inner.getType()){
                        case STRING:
                            rmap.put(new Utf8(oKey), new Utf8((String) e.getValue()));
                            break;
                        case BYTES:
                            rmap.put(new Utf8(oKey), ByteBuffer.wrap((byte[]) e.getValue()));
                            break;
                        default:
                            rmap.put(new Utf8(oKey), e.getValue());
                            break;
                    }
                        break;
                default:  // Case union not treated
                       if(!(e.getValue() instanceof String)) {
                            rmap.put(new Utf8(oKey), e.getValue());
                        }
                        else
                            rmap.put(new Utf8(oKey), new Utf8((String) e.getValue()));
                        LOG.info(e.getValue().toString());
                        break;
                }
        }
        result = new DirtyMapWrapper(rmap);
        return result;
    }
    
    private Object fromDBObject(ODocument doc, Field field, Schema schem, String vertf) {
        Object result = null;
        if(!doc.containsField(vertf))
            return null;
        switch(schem.getType()) {
            case MAP:
                                result = fromDBMap(doc,field,schem,vertf);
				break;
			case ARRAY:
				List<Object> list = doc.field(vertf);
				switch(schem.getElementType().getType()) {
				case STRING:
					List<Utf8> arrS = new LinkedList<Utf8>();
					for(Object o: list) 
						arrS.add(new Utf8((String) o));
                                        result = new DirtyListWrapper(arrS);
					break;
                                case BYTES:
                                    // Beware of ByteBuffer not being safely serialized
                                    List<ByteBuffer> arrB = new LinkedList<ByteBuffer>();
                                    for(Object o: list) 
					arrB.add(ByteBuffer.wrap((byte[]) o));
                                    result = new DirtyListWrapper(arrB);
                                    break;
				default:
					List<Object> arrT = new LinkedList<Object>();
					for(Object o: list) 
						arrT.add(o);
					result = new DirtyListWrapper(arrT);
					break;
				}
				break;
			case RECORD:
                                LOG.info("The record : "+vertf+"    "+(ORID)doc.field(vertf,ORID.class));
                                //odb.save(doc);
                                //odb.commit();
                                //odb.close();
                                /*if(!odb.existsUserObjectByRID((ORID)doc.field(vertf,ORID.class))){
                                    LOG.info("No object with RID: "+(ORID)doc.field(vertf,ORID.class));
                                    break;
                                }*/
                                Class<?> clazz = null;
                                try {
                                    clazz = ClassLoadingUtils.loadClass(schem.getFullName());
                                } catch (ClassNotFoundException e) {
                                }
                                Persistent record = new BeanFactoryImpl(keyClass, clazz).newPersistent();
                                
                                LOG.info("The record : "+vertf);
                                
                                if(odb==null)
                                    throw new IllegalStateException("The database is not open");
                                
				ODocument rec = odb.getRecord((OIdentifiable)doc.field(vertf));
                                if(rec == null)
                                    break;
				for(Field recField: schem.getFields()) {
					// FIXME: need special mapping ?
					//record.put(recField.pos(), rec.field(recField.name()));
                                    String innerDocField = (mapping.getVertexField(recField.name()) != null) ? mapping
                                        .getVertexField(recField.name()) : recField.name();
                                    if(!isValidFieldName(innerDocField))
                                        continue;
                                    LOG.info("Field :  "+recField.schema().getType()+"   name of field : "+recField.name()+"   docField : "+innerDocField+"   "+recField.pos());
                                    record.put(recField.pos(),fromDBObject(rec,recField,recField.schema(),innerDocField));
				}
                                result = record;
				break;
			case STRING:
                                //LOG.info("fieldName :"+field.name()+"result.getproperty : "+(String)result.field(vertf)+"  ; Tostring :"+result.field(vertf).toString());
				result = new Utf8(doc.field(vertf).toString());
                                LOG.info("String : "+result.toString());
                                return result;
				//break;
                        case LONG:
                                result = (Long)doc.field(vertf);
                                break;
                        case INT:
                                result = (Integer)doc.field(vertf);
                                break;
                        case BYTES:
                            // Beware of ByteBuffer not being safely serialized
                            if (doc.field(vertf) instanceof byte[])
                                result = ByteBuffer.wrap((byte[]) doc.field(vertf));
                            else
                                result = (ByteBuffer)doc.field(vertf);
                            break;
			case NULL:
				break;
                        case UNION:
                                result = fromUnion(doc,field,schem,vertf);
                                break;
			default:
                                try {
                                    //className =odb.getMetadata().getSchema().getClass(mapping.getOClassName());
                                    if(doc.containsField(vertf))
                                        result = doc.field(vertf);
                                } catch(NullPointerException e){
                                    LOG.error("NullPointerExceptionwhile while creating new Perssistent class");
                                    //throw e;
                                }
				break;
			}
        return result;
    }
    
    private Object fromUnion(ODocument doc, Field field,Schema schem, String vertf) {
        Object result = null;
        
        Type type0 = schem.getTypes().get(0).getType();
        Type type1 = schem.getTypes().get(1).getType();
        
        
        if (!type0.equals(type1)
        && (type0.equals(Type.NULL) || type1.equals(Type.NULL))) {
            Schema innerSchem;
            if(type0.equals(Type.NULL))
                innerSchem = schem.getTypes().get(1);
            else
                innerSchem = schem.getTypes().get(0);
            LOG.info(
          "Load from DBObject (UNION), schemaType:{}, docField:{}",
          new Object[] { innerSchem.getType(), vertf});
            
            result = fromDBObject(doc,field,innerSchem,vertf);
            //LOG.info("Result dans union : "+result.toString());
        }
        
        return result;
    }

    private void putUpdate(K key, T obj) {
        ODocument v = null;
        odb.begin();
        if(odb==null)
            throw new IllegalStateException("The database has not been correctly initialized");
        OIndex<?> index = odb.getMetadata().getIndexManager().getIndex(mapping.getOClassName()+".id");
        OIdentifiable rec =null;
        if(index!=null)
            rec = (OIdentifiable)index.get(key);
        if(rec!=null){
            for(int i=0; i<MAX_RETRIES; i++){
                try{
                    v = (ODocument)rec.getRecord();
                    break;
                }catch(ODatabaseException e) {
                    v = (ODocument)rec.getRecord();
                }
            }
            v = updateInstance(obj,v);
            LOG.info("Updating record");
        }else
            v = newInstance(obj);
        if(v==null)
            throw new IllegalStateException("Error on creating a new instance of : "+mapping.getOClassName());
        //ODocument res;
        for(int i=1; i<MAX_RETRIES; i++) {
            try {
                //res = odb.newInstance(mapping.getOClassName());
                if(rec == null)
                    v.field("id",key);
                v.save();
                odb.commit();
                break;
            }catch(OConcurrentModificationException e) {
                //LOG.error("ConcurrentModificationException catched, retry the commit");
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(OTransactionException e){
                //LOG.error("Generic exception while putting an object to the database",e.fillInStackTrace());
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(OStorageException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(OResponseProcessingException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(ConcurrentModificationException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(ORecordNotFoundException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v);
            }catch(Exception e) {
                LOG.error("Generic Exception at putUpdate",e.fillInStackTrace());
            }
    }
    }
    private ODocument updateInstance(T persistent, ODocument v) {
        //odb.begin();  //Last comment
        if(persistent == null)
            return null;
        Schema schem = persistent.getSchema();
        if(!schemaExists())
            createSchema();
        
        Iterator<Field> iter = schem.getFields().iterator();
                
        for(int i=0; iter.hasNext(); i++)
        {
            Field field = iter.next();
            
            //LOG.warn("Inside updateInstance "+field.name());
            
            String docf = mapping.getVertexField(field.name());
            
            if(docf == null){
                LOG.warn("No corresponding VertexField for :"+field.name());
                continue;
            }
            if(persistent.get(i)!=null){
                putAsOrientVertex(field,v,docf,persistent.get(i),field.schema());
                
                persistent.clearDirty(i);
            }
        }
        try{
            //odb.commit();
        }catch(OTransactionException e) {
            LOG.error("Transaction exception  {}  {}",e.getCause(),e.getStackTrace());
        }catch(Exception e) {
            LOG.error("Generic exception on updateInstance",e.fillInStackTrace());
        }
        return v;
    }
    private ODocument newInstance(T persistent) {
        odb.begin();
        if(persistent == null)
            return null;
        Schema schem = persistent.getSchema();
        if(!schemaExists())
            createSchema();
            
        ODocument v = odb.newInstance(mapping.getOClassName());
        
        Iterator<Field> iter = schem.getFields().iterator();
                
        for(int i=0; iter.hasNext(); i++)
        {
            Field field = iter.next();
            
            String docf = mapping.getVertexField(field.name());
            
            if(docf == null)
                continue;
            
            if(persistent.get(i) != null ){
                putAsOrientVertex(field,v,docf,persistent.get(i),field.schema());
                persistent.clearDirty(i);
            }
        }
        try{
            //odb.commit();
        }catch(OTransactionException e) {
            LOG.error("Transaction exception  {}  {}",e.getCause(),e.getStackTrace());
        }
        return v;
    }
    
    private int getUnionSchema(Object pValue, Schema pUnionSchema){
    int unionSchemaPos = 0;
//    String valueType = pValue.getClass().getSimpleName();
    Iterator<Schema> it = pUnionSchema.getTypes().iterator();
    while ( it.hasNext() ){
      Type schemaType = it.next().getType();
      if (pValue instanceof Utf8 && schemaType.equals(Type.STRING))
        return unionSchemaPos;
      else if (((pValue instanceof ByteBuffer)) && schemaType.equals(Type.BYTES))
        return unionSchemaPos;
      else if (pValue instanceof Integer && schemaType.equals(Type.INT))
        return unionSchemaPos;
      else if (pValue instanceof Long && schemaType.equals(Type.LONG))
        return unionSchemaPos;
      else if (pValue instanceof Double && schemaType.equals(Type.DOUBLE))
        return unionSchemaPos;
      else if (pValue instanceof Float && schemaType.equals(Type.FLOAT))
        return unionSchemaPos;
      else if (pValue instanceof Boolean && schemaType.equals(Type.BOOLEAN))
        return unionSchemaPos;
      else if (pValue instanceof Map && schemaType.equals(Type.MAP))
        return unionSchemaPos;
      else if (pValue instanceof List && schemaType.equals(Type.ARRAY))
        return unionSchemaPos;
      else if (pValue instanceof Persistent && schemaType.equals(Type.RECORD))
        return unionSchemaPos;
      unionSchemaPos ++;
    }
    // if we weren't able to determine which data type it is, then we return the default
    return 0;
  }
    
    private void putAsOrientVertex(final Field field, final ODocument v ,final String key, final Object value, final Schema schem) {
        try{
            if(value==null) {
                v.removeField(key);
            }
        switch(schem.getType()) {
		case MAP:
                    if(value==null){
                        LOG.info("Put map null");
                        v.field(key,(Object)null);
                    }else
                        v.field(key, toOrientEmbeddedMap((Map<Utf8,?>)value,schem,field,v));
                    break;
		case ARRAY:
			v.field(key, toOrientEmbeddedList((DirtyListWrapper) value, schem.getElementType().getType()));
			break;
                case LONG:
                            v.field(key, (Long)value);
                        break;
                case INT:
                            v.field(key, (Integer)value);
                        break;
                case BYTES:
			// Beware of ByteBuffer not being safely serialized
                            LOG.warn("Insert Bytes"+field.name());  //Temporary
                            v.field(key, ((ByteBuffer)value).array());
                            //v.setProperty(key, ((ByteBuffer) value).array());
			break;
		case STRING:
                        v.field(key, value.toString());
			break;
                case UNION:
                        int schemaPos = getUnionSchema(value, schem);
                        if(value==null) break;
                        LOG.debug("Union with value: " + value.toString() + " at index: " + schemaPos + " supported for field: " + field.name()+"  type of field : "+field.schema().getTypes().get(schemaPos).getType().name());
                        Schema unionSchema = field.schema().getTypes().get(schemaPos);//.getTypes().get(schemaPos); // Nawak
                        putAsOrientVertex(field,v,key,value, unionSchema);
                        break;
		case RECORD:                            
                        schem.getClass();
                        ODocument record;
                        boolean emptyRecords;
                        try{
                            if(v.field(key)== null)
                                 record = odb.newInstance(mapping.getOClassName());
                            else
                                record = odb.load((ODocument)v.field(key));
                            for (Field member: schem.getFields()) {
                                    Object recValue = ((Persistent) value).get(member.pos());
                                    String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                                    LOG.info("Member name :  "+innerFieldName);
                                    if(!isValidFieldName(innerFieldName))
                                        continue;
                                    putAsOrientVertex(member,record,innerFieldName,recValue,member.schema());
                            }
                            //odb.get
                            v.field(key, record);
                            odb.save(record);
                        }catch(Exception e) {
                            LOG.error("Generic error on Internal records",e.fillInStackTrace());
                        }
                        //v.addEdge(null, record);
			break;
                case NULL:
                    v.field(key, (Object)null);
                    break;
		default:
                    LOG.debug("Insert default "+field.name()); //Temporary
			v.field(key, value);
			break;
		}
        odb.save(v);
        //graph.shutdown();
        } catch(OConcurrentModificationException e) {
            //odb.rollback();
            LOG.error("ConcurrentModificationException catched, retry the commit");
            //putAsOrientVertex(field,v,key,value);
        }
    }
    

    private Type getMapType(Object pValue) {
        if (pValue instanceof Utf8)
        return Type.STRING;
      else if (pValue instanceof ByteBuffer)
        return Type.BYTES;
      else if (pValue instanceof Integer)
        return Type.INT;
      else if (pValue instanceof Long)
        return Type.LONG;
      else if (pValue instanceof Double)
        return Type.DOUBLE;
      else if (pValue instanceof Float )
        return Type.FLOAT;
      else if (pValue instanceof Boolean)
        return Type.BOOLEAN;
      else if (pValue instanceof Map )
        return Type.MAP;
      else if (pValue instanceof List)
        return Type.ARRAY;
      else if (pValue instanceof Persistent)
        return Type.RECORD;
        
        return Type.NULL;
    }
    
    private Object toOrientEmbeddedMap(Map<Utf8, ?> jmap, Schema schem, Field field, ODocument v) {
        // Handle null case
	if (jmap == null) 
		return null;
	// Handle regular cases
	Map map = new HashMap(); 
        try{
	for(Entry<Utf8, ?> e: jmap.entrySet()) {
		// ensure Key encoding -> dots replaced with middle dot
		// FIXME: better approach ?
		String vKey = e.getKey().toString().replace(".","\u00B7");
                LOG.info("Get mapKey : "+vKey);
                if(e.getValue()==null)
                    map.remove(vKey);
		switch(getMapType(e.getValue())) {
		case STRING:
			// Beware of Utf8 not being safely serialized
			map.put(vKey,e.getValue().toString());
			break;
                case BYTES:
                    map.put(vKey,((ByteBuffer) e.getValue()).array());
                    break;
                case LONG:
                    map.put(vKey, (Long)e.getValue());
                    break;
                case INT:
                    map.put(vKey, (Integer)e.getValue());
                    break;
                case RECORD:
                    if ( e.getValue() == null ) break;
			// FIXME Handle subtypes... certainly a better way to do that!
                    LOG.info("Record in a map");
                        schem.getClass();
			ODocument record = odb.newInstance(mapping.getOClassName());
			for (Field member: schem.getFields()) {
				Object recValue = ((Persistent) e.getValue()).get(member.pos());
                                String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                                LOG.info("Member name :  "+innerFieldName);
                                if(!isValidFieldName(innerFieldName))
                                    continue;
                                putAsOrientVertex(member,record,innerFieldName,recValue,member.schema());
			}
                        map.put(vKey, record);
                    break;
                case UNION:
                    LOG.info("Union in a map");
                    int schemaPos = getUnionSchema(e.getValue(), schem);
                    if(e.getValue()==null) break;
                        LOG.debug("Union with value: " + e.getValue().toString() + " at index: " + schemaPos + " supported for field: " + field.name()+"  type of field : "+field.schema().getTypes().get(schemaPos).getType().name());
                        Schema unionSchema = field.schema().getTypes().get(schemaPos);//.getTypes().get(schemaPos); // Nawak
                        putAsOrientVertex(field,v,vKey,e.getValue(), unionSchema);
                    break;
                case NULL:
                    map.put(vKey,null);
                    break;
		default:
                    LOG.info("Unknown type");
			map.put(vKey, e.getValue());
			break;
		}				
	}
        }catch(Exception e){
            LOG.info(e.getMessage());
        }
	return map;
    }

    private Object toOrientEmbeddedList(DirtyListWrapper genericArray, Type type) {
        // Handle null case
	if (genericArray == null) {
            LOG.info("The given list is null");
		return null;
        }
	// Handle regular cases
	List list = new LinkedList();
	for(Object item: genericArray) {
		switch(type) {
		case STRING:
			// Beware of Utf8 not being safely serialized
			list.add( item.toString() );
			break;
			// FIXME Record 
                case BYTES:
                    list.add(((ByteBuffer)item).array());
		default:
			list.add( item );
			break;
		}
	}
	return list;
   }
    
    private boolean isValidFieldName(String fieldName) {
        if(fieldName.startsWith("_"))
            return false;
        else
            return true;
    }
    
    public class OrientDBMappingBuilder {
    // Document description
    static final String TAG_VERTEX = "vertex";
    static final String ATT_COLLECTION = "className";
    static final String STAG_VERTEX_FIELD = "field";
    static final String ATT_NAME = "name";
    static final String ATT_TYPE = "type";
    static final String STAG_SUBVERTEX = "subvertex";

    // Class description
    static final String TAG_CLASS = "class";
    static final String ATT_KEYCLASS = "keyClass";
    static final String ATT_VERTEX = "vertex";
    static final String TAG_FIELD = "field";
    static final String ATT_FIELD = "vertfield";
    

    void fromFile(String uri) throws IOException {
        try {
            SAXBuilder saxBuilder = new SAXBuilder();
            InputStream is = getClass().getResourceAsStream(uri);
            if (is == null) {
                String msg = "Unable to load the mapping from resource '"+uri+ "' as it does not appear to exist! "+"Trying local file.";
		LOG.warn(msg);
                try {
                    is = new FileInputStream(uri);
                }catch( Exception e) {
                    LOG.error("FileInputStream error "+ e.getMessage(),e.fillInStackTrace());
                    throw new NullPointerException("Unable to load file neither local or from resource");
                }
            }
            Document doc = saxBuilder.build(is);
            if(doc == null)
                throw new IllegalStateException("doc is null");
            Element root = doc.getRootElement();
            if(root == null)
                throw new IllegalStateException("root is null");
            // No need to use documents descriptions for now...
            // Extract class descriptions
            @SuppressWarnings("unchecked")
            List<Element> classElements = root.getChildren(TAG_CLASS);
            if(classElements == null)
                throw new IllegalStateException("classelem is null");
            for(Element classElement: classElements) {
                if(classElement.getAttributeValue(ATT_KEYCLASS).equals(getKeyClass().getName())&& classElement.getAttributeValue(ATT_NAME).equals(getPersistentClass().getName())) {
                    loadPersistentClass(classElement);
                    break; // only need that
                }
            }
        }catch( IOException e) {
            LOG.error("IOexcpetion loding file :"+e.getMessage());
            throw e;
        }catch (NullPointerException e) {
            LOG.error("NUllPOinter exception while loading file : ",e.fillInStackTrace());
        } catch (JDOMException e) {
            LOG.error("JDOM exception"+e.getMessage(),e.getCause());
        }
    }

    OrientDBMapping build() {
        if (mapping.getOClassName() == null) 
		throw new IllegalStateException("A OClass is not specified");
	return mapping;
    }

    private void loadPersistentClass(Element classElement) {
        String docNameFromMapping = classElement.getAttributeValue(ATT_VERTEX);
	String collName = getSchemaName(docNameFromMapping, persistentClass);

	mapping.setOClassName(collName);
	//docNameFromMapping could be null here
	if (!collName.equals(docNameFromMapping)) {
            LOG.info("Keyclass and nameclass match but mismatching table names " 
                    + " mappingfile schema is '" + docNameFromMapping 
                    + "' vs actual schema '" + collName + "' , assuming they are the same.");
            if (docNameFromMapping != null) {
		mapping.renameOClass(docNameFromMapping, collName);
            }else
                LOG.error("docNameFromMapping is null");
	}

        LOG.info("Vertex name : "+docNameFromMapping);
	// Process fields declaration
	@SuppressWarnings("unchecked")
	List<Element> fields = classElement.getChildren(TAG_FIELD);
        if(fields == null)
            LOG.error("cannot find "+TAG_FIELD);
	for(Element field: fields) {
	mapping.addClassField(docNameFromMapping,
		field.getAttributeValue(ATT_NAME),
		field.getAttributeValue(ATT_FIELD),
		field.getAttributeValue(ATT_TYPE));
	}
    }
    
}

}
