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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.ORoundRobinClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.gora.orientdb.query.OrientDBPartitionQuery;

import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.BeanFactoryImpl;
import org.apache.gora.persistency.impl.DirtyListWrapper;
import org.apache.gora.persistency.impl.DirtyMapWrapper;
import org.apache.gora.util.ClassLoadingUtils;

import org.apache.gora.orientdb.query.OrientDBQuery;
import org.apache.gora.orientdb.query.OrientDBResult;


/**
 *
 * @author Nicolas
 * @param <K>
 * @param <T>
 */
public class OrientDBStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {
    
    /* Configuration Properties */
    /**
     * Username property for connecting to OrientDB
     */
    protected static final String USERNAME_PROPERTY = "orientdb.user";
    /**
     * Password
     */
    protected static final String PASSWORD_PROPERTY = "orientdb.password";
    /**
     * Actually it's the protocol used like remote,plocal,local
     */
    protected static final String HOST_PROPERTY = "orientdb.host";
    /**
     * The Url 
     */
    protected static final String URL_PROPERTY = "orientdb.url";
    /**
     * Indicate the location of the mapping file
     */
    protected static final String MAPPING_FILE_PROP = "orientdb.mapping.file";
    /**
     * Default value for location of mapping file
     */
    protected static final String DEFAULT_MAPPING_FILE = "gora-orientdb-mapping.xml";
    /**
     * Use graph database
     */
    protected static final String GRAPH = "gora.datastore.orientdb.useGraph";
    /**
     * Name of all edge that will be used separed by a ","
     */
    protected static final String EDGE_NAME = "gora.datastore.orientdb.edge.name";
    
    public static final int MAX_RETRIES = 5; // TO DO replace this by a property
    
    /* OrientDB Client */
    public static ODatabaseDocumentPool factory;
    private ODatabaseDocumentTx odb;
    private OClass className;
    
    
    /* Mapping definition */
    private OrientDBMapping mapping;
    
    public static final Logger LOG = LoggerFactory.getLogger(OrientDBStore.class);
    
    public static String nameOfPassword;
    public static String nameOfUser;
    public static List<String> nameOfUrl;
    public static String nameOfHost;
    public static boolean useGraph;
    private String[] edgeNames;
    
    private ArrayList<String> clusterNames = new ArrayList();
    private int clusterPosition = 0;
    
    @Override
    public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
        try {
            LOG.debug("Initializing OrientDB store");
            super.initialize(keyClass, persistentClass, properties);
            if(properties==null)
                throw new IllegalStateException("Impossible to initialize without properties");
            LOG.debug("Display url of orientdb : "+properties.getProperty("gora.datastore."+URL_PROPERTY));
            // Load of mapping
            
            if(properties.getProperty(GRAPH,"false").equalsIgnoreCase("true"))
                useGraph = true;
            else
                useGraph = false;
            
            OrientDBMappingBuilder builder = new OrientDBMappingBuilder(this);  
            String nameOfMappingFile = properties.getProperty("gora.datastore."+MAPPING_FILE_PROP,DEFAULT_MAPPING_FILE);
            nameOfHost = properties.getProperty("gora.datastore."+HOST_PROPERTY);
            nameOfUrl = new ArrayList(Arrays.asList(properties.getProperty("gora.datastore."+URL_PROPERTY).split(";")));
            nameOfUser = properties.getProperty("gora.datastore."+USERNAME_PROPERTY);
            nameOfPassword = properties.getProperty("gora.datastore."+PASSWORD_PROPERTY);
            edgeNames = properties.getProperty(EDGE_NAME, "").split(",");
            
            LOG.debug("Initializing Orient store with mapping {}.",new Object[]{nameOfMappingFile});
            mapping = new OrientDBMapping();
            builder.fromFile(nameOfMappingFile);
            mapping = builder.build();
            
            //OGlobalConfiguration.CACHE_LOCAL_ENABLED.setValue(false);
            Orient.instance().registerEngine(new OEngineRemote());
            factory = new ODatabaseDocumentPool();
            factory.setup(1,500);
            odb = factory.acquire(nameOfHost+":"+nameOfUrl.get(0), nameOfUser, nameOfPassword);  // replace by factory.acquire
        
            LOG.info("Initialized Orient Store for host {} and url {}", 
                    new Object[]{
                        nameOfHost,
                        nameOfUrl
                    });
            
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
    public String getSchemaName(final String mappingSchemaName,
    final Class<?> persistentClass) {
        return super.getSchemaName(mappingSchemaName, persistentClass);
    }

    @Override
    public void createSchema() {
        ODatabaseDocumentTx od = factory.acquire(nameOfHost+":"+nameOfUrl.get(0), nameOfUser, nameOfPassword);
        if(!schemaExists()) {
            className = od.getMetadata().getSchema().createClass(mapping.getOClassName(),od.getMetadata().getSchema().getClass("V"));
            className.createProperty("id",OType.STRING);
            className.createIndex(mapping.getOClassName()+".id", OClass.INDEX_TYPE.UNIQUE, "id");
            className.addCluster(mapping.getOClassName().toLowerCase()+"_1");
            for(int i=0;i<2; i++){
                clusterNames.add(mapping.getOClassName().toLowerCase()+"_"+i);
            }
            od.getMetadata().getSchema().createClass("intern"+mapping.getOClassName(), od.getMetadata().getSchema().getClass("V"));
            od.getMetadata().getSchema().getClass("intern"+mapping.getOClassName()).addCluster("intern"+mapping.getOClassName()+"_1");
            if(useGraph){  // Not working well now
               for(String name:edgeNames){
                   String[] internal = name.split(":");
                   if(internal.length!=3)
                       throw new IllegalStateException("EdgeClassCreationError You must specify a name,an inClass and an outClass to create Edge class");
                   OClass edgeClass = od.getMetadata().getSchema().createClass(internal[0],od.getMetadata().getSchema().getClass("E"));
                   String inClass = internal[2];
                   String outClass = internal[1];
                   if(inClass.equals(mapping.getOClassName())){
                       edgeClass.createProperty("in", OType.LINK, className).setMandatory(true);
                   }else
                       edgeClass.createProperty("in", OType.LINK, od.getMetadata().getSchema().getClass(inClass)).setMandatory(true);
                   
                   if(outClass.equals(mapping.getOClassName())){
                       edgeClass.createProperty("out", OType.LINK, className).setMandatory(true);
                   }else
                       edgeClass.createProperty("out", OType.LINK,  od.getMetadata().getSchema().getClass(outClass)).setMandatory(true);
                   
                   edgeClass.createIndex("uni_"+internal[0], OClass.INDEX_TYPE.UNIQUE, "in","out");
               }
            }
            od.getMetadata().getSchema().save();
        }else
            className = od.getMetadata().getSchema().getClass(mapping.getOClassName());
        LOG.warn("Class {} and {} has been created", new Object[]{mapping.getOClassName(),"intern"+mapping.getOClassName()});
    }

    @Override
    public void deleteSchema() {
        
        if(schemaExists()){
            
            for(ODocument doc : odb.browseClass("intern"+mapping.getOClassName())){
                odb.delete(doc.getIdentity());
                //doc.delete();
            }
            //odb.commit();
            odb.getMetadata().getSchema().dropClass("intern"+mapping.getOClassName()); //TO DO add a property for internalRecord
            odb.getMetadata().getSchema().save();
            
            for(ODocument doc : odb.browseClass(mapping.getOClassName())){
                odb.delete(doc.getIdentity());
                //doc.delete();
            }
            //odb.commit();
            odb.getMetadata().getSchema().dropClass(mapping.getOClassName());
            odb.getMetadata().getSchema().save();
            
            
            if(useGraph){
               for(String name:edgeNames){
                   odb.getMetadata().getSchema().dropClass(name);
               }
            }
        }
        //od.close();
    }

    @Override
    public boolean schemaExists() {
        boolean result = false;
        odb.commit();
        try {
            result = (odb.getMetadata().getSchema().existsClass(mapping.getOClassName())&& odb.getMetadata().getSchema().existsClass("intern"+mapping.getOClassName()));
        }catch(NullPointerException e) {
            LOG.error("NullPointerException while checking the existence of the schema", e.fillInStackTrace());
        }
        LOG.debug("Schema exists : "+result+"    Exist class Employee: "+odb.getMetadata().getSchema().existsClass("intern"+mapping.getOClassName()));
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
        odb.commit();
        ODocument result = odb.getRecord((OIdentifiable)odict.get(key));
        odb.commit();
        if(result==null){
            return null;
        }
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
                LOG.error("Error on commiting update",e.fillInStackTrace());
            }finally{
                
                
            }
            LOG.debug("The object  with key : "+key.toString()+"  is added to the db");
        }
        else
            LOG.debug("Ignored putting object {} in the store as it is not dirty",new Object[]{obj});
        
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
        //odb.begin();
        ODatabaseDocumentTx od = factory.acquire(nameOfHost+":"+nameOfUrl.get(clusterPosition), nameOfUser, nameOfPassword);
        query.setLocalFilterEnabled(false);
        OrientDBQuery orientQuery = new OrientDBQuery(this);
        orientQuery.setFields(null);
        OrientDBResult orientResult = new OrientDBResult(this,orientQuery);
        query.setFields(null);
        String quer = orientQuery.getSQLQuery(query, mapping);
        orientResult.setOdb(od);
        try{
            od.begin();
            orientResult.initResult(quer, od);
            while(orientResult.next()){
                try{
                    od.delete(orientResult.getCurrentDocument());
                }catch(OTransactionException e){
                    LOG.debug("Unable to delete retry",e.fillInStackTrace());
                    od.delete(orientResult.getCurrentDocument());
                }
            }
            od.commit();
            //od.getMetadata().getIndexManager().getIndex(getSchemaName()+".id").rebuild(); // TODO make a big scale test 
        } catch (Exception ex) {
            LOG.error("Error deleteByQuery at position {}"+orientResult.getOffset(),ex.fillInStackTrace());
            return -1;
        }
        return 0;
    }

    @Override
    public Result<K, T> execute(Query<K, T> query) {
        odb.declareIntent(new OIntentMassiveRead());
        odb.commit();
        odb.begin();
        OrientDBQuery orientQuery = new OrientDBQuery(this);
        OrientDBResult orientResult = new OrientDBResult(this,orientQuery);
        orientResult.setOdb(odb);
        String quer = orientQuery.getSQLQuery(query, mapping);
        try{
            LOG.info("The query is : "+quer);  //TODO replace with debug
            orientResult.initResult(quer, odb);
            
            LOG.debug("OrientResult size : "+orientResult.getSize());
        }catch(OResponseProcessingException e){
            LOG.error("Unable to execute the query {}",e.fillInStackTrace());
        }finally{
            odb.declareIntent(null);
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
        //Sharding
        int[] clusterIds = odb.getMetadata().getSchema().getClass(mapping.getOClassName()).getClusterIds();
        List<PartitionQuery<K, T>> partitions = new ArrayList<PartitionQuery<K, T>>();
        for(int id: clusterIds){
            OrientDBPartitionQuery partitionQuery = new OrientDBPartitionQuery(
            query);
            partitionQuery.setConf(getConf());
            String clusterName = odb.getClusterNameById(id);
            partitionQuery.setClusterName(clusterName);
            partitions.add(partitionQuery);
            LOG.info("Name of cluster : "+partitionQuery.getClusterName());  //TODO replace with debug
        }
        return partitions;
    }

    @Override
    public void flush() { // Not really useful as the database instance used here is not the instance used in put
        try {
            odb.commit();
        } catch (OTransactionException e) {
            LOG.error("Error on commiting database",e.fillInStackTrace());
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
    
    // DESERIALIZATION
    
    /**
    * Build a new instance of the persisted class from the {@link ODocument}
    * retrieved from the database.
    * 
    * @param doc
    *          the {@link ODocument} that results from the query to the database
    * @param fields
    *          the list of fields to be mapped to the persistence class instance
    * @return a persistence class instance which content was deserialized from
    *         the {@link ODocument}
    * @throws Exception
    */
    public T newInstanceT(ODocument doc, String[] fields) throws Exception {
        T persistent = newPersistent();
        try{
            
        String[] dbFields = getFieldsToQuery(fields);
        for(String f : dbFields) {
            
            boolean isEdge = true;
            if(f.equals("id") || f.startsWith("in_") || f.startsWith("out_"))  
                continue;
            String vertf = mapping.getVertexField(f);
            Field field = fieldMap.get(f);
            LOG.debug("VertexField is: {}  and field is: {}",vertf,field.name());
            if(!f.startsWith("edge")){
                isEdge = false;
                if ((vertf == null) || (!doc.containsField(vertf)) ||(doc.field(vertf)==null)) {
                    //persistent.put(field.pos(), field.defaultValue());
                    continue;
                }
            }
            Schema fieldSchem = field.schema();
            LOG.debug("Name of all fields:  "+f+";     FieldType"+fieldSchem.getType().getName()+";     "+fieldSchem.getType().name());
            Object result = null;
            try{
            result = fromDBObject(doc,field,fieldSchem,vertf);  
            }catch(NullPointerException e){
                LOG.error("NullPointer on fromDBObject",e.fillInStackTrace());
            }
            if(result!=null){
                // LOG.info("TEMPORARY!! result!=null add to the persistent");
                persistent.put(field.pos(), result);

            }
        }
        //LOG.info("TEMPORARY!! Value of the first field: "+persistent.get(1)+"  second: "+persistent.get(2));
        persistent.clearDirty();
        }catch(NullPointerException e){
            LOG.error("NullPointerException in NewInstanceT ",e.fillInStackTrace());
        }catch(Exception e) {
            LOG.error("Generic error in newInstanceT",e.fillInStackTrace());
        }
        return persistent;
        
    }
    
    /**
     * Used to call the function linked to each type of field to reconstruct the Persistent class
     * 
     * @param doc
     *      the {@link ODocument} that results from the query to the database
     * @param field
     *      Single field 
     * @param schem
     * @param vertf
     *      The name of the field in the DB
     * @return 
     */
    private Object fromDBObject(ODocument doc, Field field, Schema schem, String vertf) {
        Object result = null;
        if(!doc.containsField(vertf)&& !vertf.equals("")){
            LOG.info("No corresponding fields with name : {}",vertf);
            return null;
        }
        switch(schem.getType()) {
            case MAP:
                    result = fromDBMap(doc,schem,vertf);
                    break;
            case ARRAY:
                    //LOG.info("TEMPORARY!! INside fromDBObject ");
                    result = fromDBArray(doc,schem,vertf);
                    break;
            case RECORD:
                    result = fromDBDocument(doc,schem,vertf);
                    break;
            case STRING:
                    result = new Utf8(doc.field(vertf).toString());
                    //return result;
                    break;
            case LONG:
                    result = (Long)doc.field(vertf);
                    break;
            case INT:
                    result = (Integer)doc.field(vertf);
                    //LOG.info("TEMPORARY!!!!  "+result);
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
                        if(doc.containsField(vertf))
                            result = doc.field(vertf);
                    } catch(NullPointerException e){
                        LOG.error("NullPointerExceptionwhile while creating new Perssistent class :", e.fillInStackTrace());
                        //throw e;
                    }
                    break;
            }
        return result;
    }
    
    /**
     * If the actual field was "Document" the recreate an instance of Persistent class associated with this inner Document
     * 
     * @param doc
     * @param schem
     * @param vertf
     * @return 
     *      Persistent class of the inner doc
     */
    private Persistent fromDBDocument(ODocument doc, Schema schem, String vertf){
        Class<?> clazz = null;
        try {
            clazz = ClassLoadingUtils.loadClass(schem.getFullName());
        } catch (ClassNotFoundException e) {
            LOG.error("Error on deserializing internal record : ",e.fillInStackTrace());
        }
        Persistent record = new BeanFactoryImpl(keyClass, clazz).newPersistent();
        if(odb==null)
            throw new IllegalStateException("The database is not open");

        ODocument rec = odb.getRecord((OIdentifiable)doc.field(vertf));
        if(rec == null)
            return null;
        for(Field recField: schem.getFields()) {
                // FIXME: need special mapping ?
                //record.put(recField.pos(), rec.field(recField.name()));
            String innerDocField = (mapping.getVertexField(recField.name()) != null) ? mapping
                .getVertexField(recField.name()) : recField.name();
            if(!isValidFieldName(innerDocField))
                continue;
            LOG.debug("Field :  "+recField.schema().getType()+"   name of field : "+recField.name()+"   docField : "+innerDocField+"   "+recField.pos());
            record.put(recField.pos(),fromDBObject(rec,recField,recField.schema(),innerDocField));
        }
        return record;
    }
    
    /**
     * 
     * @param doc
     * @param schem
     * @param vertf
     * @return 
     *      
     */
    private DirtyListWrapper fromDBArray(ODocument doc, Schema schem, String vertf){
        DirtyListWrapper result = null;
        List<Object> list = null;
        list = doc.field(vertf);
        
        switch(schem.getElementType().getType()) {
        case STRING:
                List<Utf8> arrS = new ArrayList<Utf8>();
                for(Object o: list) 
                        arrS.add(new Utf8((String) o));
                result = new DirtyListWrapper(arrS);
                break;
        case BYTES:
            // Beware of ByteBuffer not being safely serialized
            List<ByteBuffer> arrB = new ArrayList<ByteBuffer>();
            for(Object o: list) 
                arrB.add(ByteBuffer.wrap((byte[]) o));
            result = new DirtyListWrapper(arrB);
            break;
        case RECORD:
            List<Persistent> record = new ArrayList();
            OrientVertex actualVert = new OrientGraph(odb).getVertex(doc.getIdentity());
            Iterable<Edge> iterOut = actualVert.getEdges(Direction.OUT);
            Class<?> clazz = null;
            try {
                clazz = ClassLoadingUtils.loadClass(schem.getElementType().getFullName());
            } catch (ClassNotFoundException e) {
                LOG.error("Error on deserializing internal record in an array : ",e.fillInStackTrace());
            }
            
            for(Edge e : iterOut){
                Persistent edgeRec = new BeanFactoryImpl(keyClass, clazz).newPersistent();
                edgeRec.put(1, e.getLabel());
                OrientVertex v = (OrientVertex)e.getVertex(Direction.IN);
                ODocument innerDoc = v.getRecord();
            try {
                Persistent innerPersist = newInstanceT(innerDoc,innerDoc.fieldNames());
                edgeRec.put(2, innerPersist);
                record.add(edgeRec);
            } catch (Exception ex) {
                LOG.error("Unknown Exception while retrieving record from vertex",ex.fillInStackTrace());
            }
            }
            result = new DirtyListWrapper(record);
            break;
        default:
                List<Object> arrT = new ArrayList<Object>();
                for(Object o: list) 
                        arrT.add(o);
                result = new DirtyListWrapper(arrT);
                break;
        }
        return result;
    }
    
    /**
     * 
     * @param doc
     * @param schem
     * @param vertf
     * @return 
     */
    private DirtyMapWrapper fromDBMap(ODocument doc, Schema schem, String vertf){
        DirtyMapWrapper result = null;
        if(!doc.containsField(vertf))
            return null;
        Map<String,Object> map = doc.field(vertf);
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
                default:  
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
   
    /**
     * Only handle up to two field union not more
     * 
     * @param doc
     * @param field
     * @param schem
     * @param vertf
     * @return 
     */
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
            LOG.debug(
          "Load from DBObject (UNION), schemaType:{}, docField:{}",
          new Object[] { innerSchem.getType(), vertf});
            
            result = fromDBObject(doc,field,innerSchem,vertf);
            //LOG.info("Result dans union : "+result.toString());
        }
        
        return result;
    }
    
    /*
    *   SERIALIZATION
    */

    /**
     * The name is not appropriate as it is actually used every time, not only during update.
     * 
     * 
     * @param key
     * @param obj 
     */
    private void putUpdate(K key, T obj) {
        ODocument v = null;
        
        // Actually we use this as in OrientDB Distributed Database always use local for clustering
        // We should add an option to disable this if you wanna use a load balancer instead
        ODatabaseDocumentTx od = getCurrentClusterDatabase();

        od.begin();
        
        if(od==null)
            throw new IllegalStateException("The database has not been correctly initialized");
        OIndex<?> index = od.getMetadata().getIndexManager().getIndex(mapping.getOClassName()+".id");
        OIdentifiable rec =null;
        if(index!=null){
            try{
                rec = (OIdentifiable)index.get(key);
            }catch(Exception e){
                LOG.error("Error on retrieving record from index",e.fillInStackTrace());
            }
        }
        if(rec!=null){
            for(int i=0; i<MAX_RETRIES; i++){
                try{
                    v = (ODocument)rec.getRecord();
                    break;
                }catch(Exception e) {
                    LOG.warn("Unable to load the record Try n°:"+i+1);
                }
            }
            try{
                v = updateInstance(obj,v,od);
            }catch(Exception e){
                LOG.error("Unknown exception on updateInstance",e.fillInStackTrace());
            }
            LOG.debug("Updating record");
        }else
            v = newInstance(obj,od);
        if(v==null)
            throw new IllegalStateException("Error on creating a new instance of : "+mapping.getOClassName());
        for(int i=1; i<MAX_RETRIES; i++) {
            try {
                if(rec == null)
                    v.field("id",key);
                try{
                        od.save(v);                    
                }catch(Exception e) // Temporary
                {
                    LOG.error("unable to save doc  "+e.getMessage());
                }
                od.commit();
                break;
            }catch(OConcurrentModificationException e) {  // TODO Find a better way to handle all these Exception
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(OTransactionException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(OStorageException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(OResponseProcessingException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(ConcurrentModificationException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(ORecordNotFoundException e){
                LOG.warn("The document has been modified retries commit n°: "+i);
                v.reload();
                v = updateInstance(obj,v,od);
            }catch(Exception e) {
                od.rollback();
                LOG.error("Generic Exception at putUpdate for doc:"+v.getIdentity().toString(),e.fillInStackTrace());
            }finally {
                try{
                    od.close();
                }catch(Exception e){
                    LOG.error("ODatabaseException while closing database",e.fillInStackTrace());
                }
            }
    }
    }
    
    /**
     * Alternatively choose an instance of the database
     * 
     * @return 
     *      The {@link ODatabaseDocumentTx} associated to each server
     */
    private ODatabaseDocumentTx getCurrentClusterDatabase(){
        clusterPosition++;
        if(clusterPosition <= (nameOfUrl.size()-1)){
            LOG.debug("Url of database"+nameOfUrl.get(clusterPosition));
            return factory.acquire(nameOfHost+":"+nameOfUrl.get(clusterPosition), nameOfUser, nameOfPassword);
        }
        else{
            clusterPosition = 0;
            return factory.acquire(nameOfHost+":"+nameOfUrl.get(clusterPosition), nameOfUser, nameOfPassword);
        }
    }
    
    /**
     * 
     * @param persistent
     * @param v
     * @param od
     * @return 
     */
    private ODocument updateInstance(T persistent, ODocument v, ODatabaseDocumentTx od) {
        
        if(persistent == null)
            return null;
        Schema schem = persistent.getSchema();
        if(!schemaExists())
            createSchema();
        
        Iterator<Field> iter = schem.getFields().iterator();
                
        for(int i=0; iter.hasNext(); i++)
        {
            Field field = iter.next();            
            String docf = mapping.getVertexField(field.name());
            
            if(docf == null){
                LOG.warn("No corresponding VertexField for :"+field.name());
                continue;
            }
            if(persistent.isDirty(field.name())){
                try{
                putAsOrientVertex(field,v,docf,persistent.get(i),field.schema(),od);
                }catch(Exception e){
                    LOG.error("Error on putAsOrientVertex",e.fillInStackTrace());
                }
                
                persistent.clearDirty(i);
            }
        }
        return v;
    }
    
    /**
     * 
     * @param persistent
     * @param od
     * @return 
     *      The {@link ODocument} to be created in database
     */
    private ODocument newInstance(T persistent, ODatabaseDocumentTx od) {

        if(persistent == null)
            return null;
        Schema schem = persistent.getSchema();
        if(!schemaExists())
            createSchema();
            
        ODocument v = od.newInstance(mapping.getOClassName());
        
        Iterator<Field> iter = schem.getFields().iterator();
                
        for(int i=0; iter.hasNext(); i++)
        {
            Field field = iter.next();
            LOG.debug("internal fields new doc"+field.name());
            
            String docf = mapping.getVertexField(field.name());
            
            if(docf == null)
                continue;
            
            if(persistent.isDirty(field.name())){
                putAsOrientVertex(field,v,docf,persistent.get(i),field.schema(),od);
                persistent.clearDirty(i);
            }
        }

        return v;
    }
    
    /**
     * Retrieve the union schema
     * 
     * @param pValue
     * @param pUnionSchema
     * @return 
     */
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
    
    /**
     * 
     * 
     * @param field
     * @param v
     * @param key
     * @param value
     * @param schem
     * @param od 
     */
    private void putAsOrientVertex(final Field field, final ODocument v ,final String key, final Object value, final Schema schem, ODatabaseDocumentTx od) {
        try{
            if(value==null) {
                v.removeField(key);
            }
        switch(schem.getType()) {
		case MAP:
                    if(value==null){
                        LOG.debug("Put map null");
                        v.field(key,(Object)null);
                    }else
                        v.field(key, toOrientEmbeddedMap((Map<Utf8,?>)value,schem,field,v,od));
                    break;
		case ARRAY:
			//v.field(key, toOrientEmbeddedList((DirtyListWrapper) value, schem.getElementType().getType()));
                    toOrientEmbeddedList((List) value, schem.getElementType().getType(), schem, v, key, od);
                    break;
                case LONG:
                    v.field(key, (Long)value);
                    break;
                case INT:
                    v.field(key, (Integer)value);
                    break;
                case BYTES:
                    // Beware of ByteBuffer not being safely serialized
                    LOG.debug("Insert Bytes"+field.name());  //Temporary
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
                    Schema unionSchema = field.schema().getTypes().get(schemaPos);
                    putAsOrientVertex(field,v,key,value, unionSchema,od);
                    break;
		case RECORD:                            
                    v.field(key,toOrientDBDocument((Persistent)value,schem,v,key,od));
                    break;
                case NULL:
                    v.field(key, (Object)null);
                    break;
		default:
                    //LOG.debug("Insert default "+field.name()); //Temporary
                    v.field(key, value);
                    break;
		}
        } catch(OConcurrentModificationException e) {
            LOG.error("ConcurrentModificationException catched ",e.fillInStackTrace());
            
        }catch(Exception e){
            LOG.error("Error in putAsOrient... :"+e.getMessage());
        }
    }

    /**
     * 
     * @param pValue
     * @return 
     */
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
    
    /**
     * 
     * @param value
     * @param schem
     * @param v
     * @param key
     * @param od
     * @return 
     */
    private ODocument toOrientDBDocument(Persistent value,Schema schem, ODocument v, String key, ODatabaseDocumentTx od){ // TO DO don't use the key inside this function
        String intRecName = schem.getName();
        ODocument record = null;
        //od.begin();
        boolean emptyRecords;
        try{
            if(v.field(key)==null){
                if(intRecName.equals(mapping.getOClassName()))
                    record = od.newInstance(mapping.getOClassName());
                else
                    record = od.newInstance("intern"+mapping.getOClassName());
            }else
                record = od.load((ODocument)v.field(key)); 
            for (Field member: schem.getFields()) {
                    Object recValue = ((Persistent) value).get(member.pos());
                    String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                    LOG.debug("Member name :  "+innerFieldName);
                    if(!isValidFieldName(innerFieldName))
                        continue;
                    putAsOrientVertex(member,record,innerFieldName,recValue,member.schema(),od);
            }
            od.save(record);
            //od.commit();
        }catch(Exception e) {
            LOG.error("Generic error on Internal records",e.fillInStackTrace());
        }finally {
        }
        return record;
    }
    
    /**
     * 
     * @param jmap
     * @param schem
     * @param field
     * @param v
     * @param od
     * @return 
     */
    private Object toOrientEmbeddedMap(Map<Utf8, ?> jmap, Schema schem, Field field, ODocument v, ODatabaseDocumentTx od) {
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
            LOG.debug("Get mapKey : "+vKey);
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
                LOG.debug("Record in a map");
                    schem.getClass();
                    ODocument record = odb.newInstance(mapping.getOClassName());
                    for (Field member: schem.getFields()) {
                            Object recValue = ((Persistent) e.getValue()).get(member.pos());
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!isValidFieldName(innerFieldName))
                                continue;
                            putAsOrientVertex(member,record,innerFieldName,recValue,member.schema(),od);
                    }
                    map.put(vKey, record);
                break;
            case UNION:
                LOG.debug("Union in a map");
                int schemaPos = getUnionSchema(e.getValue(), schem);
                if(e.getValue()==null) break;
                    LOG.debug("Union with value: " + e.getValue().toString() + " at index: " + schemaPos + " supported for field: " + field.name()+"  type of field : "+field.schema().getTypes().get(schemaPos).getType().name());
                    Schema unionSchema = field.schema().getTypes().get(schemaPos);//.getTypes().get(schemaPos); // Nawak
                    putAsOrientVertex(field,v,vKey,e.getValue(), unionSchema,od);
                break;
            case NULL:
                map.put(vKey,null);
                break;
            default:
                LOG.debug("Unknown type");
                map.put(vKey, e.getValue());
                break;
            }				
	}
        }catch(Exception e){
            LOG.error("Generic exception while putting an embedded map",e.fillInStackTrace());
        }
	return map;
    }

    /**
     * 
     * @param genericArray
     * @param type
     * @param schem
     * @param v
     * @param key
     * @param od 
     */
    private void toOrientEmbeddedList(List genericArray, Type type,Schema schem, ODocument v,String key, ODatabaseDocumentTx od) {
        // Handle null case
	if (genericArray == null) {
            LOG.info("The given list is null");
		return;
        }
	// Handle regular cases
	List<Object> list = new ArrayList();
        try{
            boolean graphRec = false;
	for(Object item: genericArray) {
            switch(type) {
            case STRING:
                    // Beware of Utf8 not being safely serialized
                    list.add( item.toString() );
                    break;
                    // FIXME Record 
            case BYTES:
                list.add(((ByteBuffer)item).array());
                break;
            case RECORD:
                graphRec = true;
                Schema innerSchem = schem.getElementType();
                Field label = innerSchem.getFields().get(1);
                String edgeName = ((Persistent)item).get(1).toString();
                LOG.info("Temporary!!!   edgeName="+edgeName);
                Persistent persistDoc = (Persistent)((Persistent)item).get(2);
                OrientGraph graph = new OrientGraph(od);
                if(checkKey(item)){
                    OIndex odict = od.getMetadata().getIndexManager().getIndex(persistDoc.getSchema().getName()+".id");
                    Object docKey = getKey(((Persistent)item).get(2));
                    LOG.info("key of this vertex"+docKey);
                    if(odict.get(docKey)==null)
                        throw new IllegalStateException("You must provide a valid key to create the Edge");
                    Vertex outVert = graph.getVertex(v.getIdentity());
                    LOG.info("key of in vertex"+v.getIdentity());
                    Vertex inVert = graph.getVertex(odict.get(docKey));
                    OIndex index = od.getMetadata().getIndexManager().getIndex("uni_"+edgeName);
                    OCompositeKey compKey = new OCompositeKey();
                    compKey.addKey((ORecordId)inVert.getId());
                    compKey.addKey((ORecordId)outVert.getId());
                    OrientEdge e = null;
                    if(!index.contains(compKey))
                        e = graph.addEdge(null, outVert, inVert, edgeName);
                    else
                        e = graph.getEdge(index.get(compKey));

                    //OrientEdge e = (OrientEdge)(inVert.addEdge(edgeName, outVert));
                    int size = ((Persistent)item).getUnmanagedFields().size();
                    if(size>3){
                        for(int i=2;i<size; i++){
                            LOG.info("TEMPORARY!!!!  The edges has some properties");
                            Object recValue = ((Persistent) item).get(i);
                            Field member = innerSchem.getFields().get(i);
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!isValidFieldName(innerFieldName))
                                continue;
                            putAsOrientVertex(member,e.getRecord(),innerFieldName,recValue,member.schema(),od);
                        }
                    }

                }else{
                    Vertex inVert = graph.getVertex(v.getIdentity());
                    ODocument outDoc = toOrientDBDocument(persistDoc,persistDoc.getSchema(),v,"",od);

                    if(outDoc==null)
                        LOG.error("OutDoc is null");
                    if(inVert==null)
                        LOG.error("Invertex is null");
                    Vertex outVert = graph.getVertex(outDoc.getIdentity());
                    if(outVert==null)
                        LOG.error("OutVertex is null, identity of outDoc : "+outDoc.getIdentity());
                    OIndex index = od.getMetadata().getIndexManager().getIndex("uni_"+edgeName);
                    List<Vertex> vertKey = new ArrayList();
                    vertKey.add(inVert);
                    vertKey.add(outVert);
                    OrientEdge e = null;
                    if((e = (OrientEdge)index.get(vertKey))==null)
                        e = (OrientEdge)(inVert.addEdge(edgeName, outVert));
                    int size = ((Persistent)item).getUnmanagedFields().size();
                    if(size>1){
                        for(int i=2;i<size; i++){
                            Object recValue = ((Persistent) item).get(i);
                            Field member = innerSchem.getFields().get(i);
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!isValidFieldName(innerFieldName))
                                continue;
                            putAsOrientVertex(member,e.getRecord(),innerFieldName,recValue,member.schema(),od);
                        }
                    }
                }
                graph.commit();
                graph.shutdown();
                break;
            default:
                    list.add( item );
                    break;
            }
	}
        
        if(!graphRec)
            v.field(key,list);
        }catch(Exception e){
            LOG.error("Generic Error on creating graph edges",e.fillInStackTrace());
        }
   }
    
    /**
     * 
     * @param persistent
     * @return 
     */
    private boolean checkKey(Object persistent){
        if(persistent instanceof org.apache.gora.orientdb.storage.test.Vertex){
            LOG.info("The key is a vertex");
            return true;
        }
        return false;
    }
    
    /**
     * 
     * @param persistent
     * @return 
     */
    private Object getKey(Object persistent){
        return ((org.apache.gora.orientdb.storage.test.Vertex)persistent).getKey();
    }
    
    /**
     * 
     * @param fieldName
     * @return 
     */
    private boolean isValidFieldName(String fieldName) {
        if(fieldName.startsWith("_"))
            return false;
        else 
            return true;
    }
    
}
