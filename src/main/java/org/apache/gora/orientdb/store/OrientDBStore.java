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
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.gora.orientdb.query.OrientDBPartitionQuery;

import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;

import org.apache.gora.orientdb.query.OrientDBQuery;
import org.apache.gora.orientdb.query.OrientDBResult;
import org.apache.gora.orientdb.serialization.Deserialization;
import org.apache.gora.orientdb.serialization.Serialization;


/**
 *  TODO replace all the mapping.getSchemaName with getSchemaName to avoid some issue in future release
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
    
    /* Serializer */
    private Serialization serial;
    private Deserialization deserial;
    
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
    
    private final ArrayList<String> clusterNames = new ArrayList();
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
            
            // Init Serializer
            serial = new Serialization(odb, mapping);
            deserial = new Deserialization(odb, mapping, keyClass, this);
        
            LOG.info("Initialized Orient Store for host {} and url {}", 
                    new Object[]{
                        nameOfHost,
                        nameOfUrl
                    });
            
        } catch( Exception e) { // TODO Replace 
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
    public void createSchema() { // TODO maybe replace the acquired database with odb
        ODatabaseDocumentTx od = factory.acquire(nameOfHost+":"+nameOfUrl.get(0), nameOfUser, nameOfPassword); // TODO deprecated method
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
               for(String name:edgeNames){ // TODO add alterdatabaseminimumclusters to property of DB
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
        //odb.commit();
        ODocument result = odb.getRecord((OIdentifiable)odict.get(key)); //WTF?????
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
            // Actually we use this as in OrientDB Distributed Database always use local for clustering
            // TESTING PURPOSE ONLY Should be replaced by load balancer, or maybe be allowed by some properties 
            ODatabaseDocumentTx od = getCurrentClusterDatabase();
            try{                
                putInOdb(key,obj,od);
                //odb.commit();
            }catch(Exception e){  //Temporary
                LOG.error("Error on commiting update",e.fillInStackTrace());
            }finally{
                od.close();
                
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
        String quer = OrientDBQuery.getSQLQuery(query, mapping);
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
        }finally{
            od.close();
        }
        return 0;
    }

    @Override
    public Result<K, T> execute(Query<K, T> query) {
        odb.declareIntent(new OIntentMassiveRead());
        odb.commit(); // TODO check usefullness
        odb.begin();
        OrientDBQuery orientQuery = new OrientDBQuery(this);
        OrientDBResult orientResult = new OrientDBResult(this,orientQuery);
        orientResult.setOdb(odb);
        String quer = OrientDBQuery.getSQLQuery(query, mapping);
        try{
            LOG.debug("The query is : "+quer);
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
            OrientDBPartitionQuery partitionQuery = new OrientDBPartitionQuery(query);
            partitionQuery.setConf(getConf());
            String clusterName = odb.getClusterNameById(id);
            partitionQuery.setClusterName(clusterName);
            partitions.add(partitionQuery);
            LOG.debug("Name of cluster : "+partitionQuery.getClusterName());
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
            result = deserial.fromDBObject(doc,field,fieldSchem,vertf);  
            }catch(NullPointerException e){
                LOG.error("NullPointer on fromDBObject",e.fillInStackTrace());
            }
            if(result!=null){
                persistent.put(field.pos(), result);

            }
        }
        persistent.clearDirty();
        }catch(NullPointerException e){
            LOG.error("NullPointerException in NewInstanceT ",e.fillInStackTrace());
        }catch(Exception e) {
            LOG.error("Generic error in newInstanceT",e.fillInStackTrace());
        }
        return persistent;
        
    }
    
    /*
    *   SERIALIZATION
    */

    /**
     *
     * 
     * @param key
     * @param obj 
     */
    private void putInOdb(K key, T obj, ODatabaseDocumentTx od) {
        ODocument v = null;

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
            }
    }
    }
    
    /**
     * Alternatively choose an instance of the database
     * 
     * TODO Should not use this in prod, only for testing purpose replace this by load balancer
     * 
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
                serial.putAsOrientVertex(field,v,docf,persistent.get(i),field.schema(),od);
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
                serial.putAsOrientVertex(field,v,docf,persistent.get(i),field.schema(),od);
                persistent.clearDirty(i);
            }
        }

        return v;
    }
        
}
