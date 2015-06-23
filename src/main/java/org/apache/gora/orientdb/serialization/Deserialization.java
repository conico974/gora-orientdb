/*
 * Copyright 2015 The Apache Software Foundation.
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

package org.apache.gora.orientdb.serialization;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.gora.orientdb.store.OrientDBMapping;
import org.apache.gora.orientdb.store.OrientDBStore;
import static org.apache.gora.orientdb.store.OrientDBStore.LOG;
import org.apache.gora.orientdb.utils.Utils;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.BeanFactoryImpl;
import org.apache.gora.persistency.impl.DirtyListWrapper;
import org.apache.gora.persistency.impl.DirtyMapWrapper;
import org.apache.gora.util.ClassLoadingUtils;

/**
 *
 * @author Nicolas
 */
public class Deserialization {
    
    private ODatabaseDocumentTx odb;
    private OrientDBMapping mapping;
    
    private Class keyClass;
    
    private OrientDBStore store;
    
        public Deserialization(ODatabaseDocumentTx odb, OrientDBMapping mapping, Class keyClass, OrientDBStore store){
            this.odb = odb;
            this.mapping = mapping;
            this.keyClass = keyClass;
            this.store = store;
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
    public Object fromDBObject(ODocument doc, Schema.Field field, Schema schem, String vertf) throws NullPointerException{
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
                
        ODocument rec = null;
        try{
        OIndex index = odb.getMetadata().getIndexManager().getIndex(mapping.getOClassName());
        
        OIdentifiable oId = null;
        try{
        oId = (OIdentifiable) index.get(doc.field(vertf)); 
        }catch(NullPointerException e){
            LOG.error("Index in fromDBDocument ",e.fillInStackTrace());
        }
        
        if(oId == null)
            try{
            rec = odb.getRecord((OIdentifiable)doc.field(vertf)); 
            }catch(NullPointerException e){
                LOG.error("Unable to load the inner record :",e.fillInStackTrace());
            }
        else
            rec = oId.getRecord();
        }catch(NullPointerException e){
            LOG.error("Error for field: "+vertf, e.fillInStackTrace());
        }
        if(rec == null)
            LOG.error("The provided document ID is not stored inside the database");
        for(Schema.Field recField: schem.getFields()) {
                // FIXME: need special mapping ?
                //record.put(recField.pos(), rec.field(recField.name()));
            String innerDocField = (mapping.getVertexField(recField.name()) != null) ? mapping
                .getVertexField(recField.name()) : recField.name();
            if(!Utils.isValidFieldName(innerDocField))
                continue;
            LOG.debug("Field :  "+recField.schema().getType()+"   name of field : "+recField.name()+"   docField : "+innerDocField+"   "+recField.pos());
            try{
                record.put(recField.pos(),fromDBObject(rec,recField,recField.schema(),innerDocField));
            }catch(NullPointerException e){
                LOG.error("NullPointerException on nested fromDBObject",e.fillInStackTrace());
            }
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
            List<Persistent> record = new ArrayList(); // TODO change to Concurrent
            OrientVertex actualVert = new OrientGraph(odb).getVertex(doc.getIdentity());
            Iterable<Edge> iterOut = actualVert.getEdges(Direction.OUT);
            Class<?> clazz = null;
            try {
                clazz = ClassLoadingUtils.loadClass(schem.getElementType().getFullName());
            } catch (ClassNotFoundException e) {
                LOG.error("Error on deserializing internal record in an array : ",e.fillInStackTrace());
            }
            // TODO implement graph
            for(Edge e : iterOut){
                Persistent edgeRec = new BeanFactoryImpl(keyClass, clazz).newPersistent();
                edgeRec.put(1, e.getLabel());
                OrientVertex v = (OrientVertex)e.getVertex(Direction.IN);
                
                ODocument innerDoc = v.getRecord();
            try {
                Persistent innerPersist = store.newInstanceT(innerDoc,innerDoc.fieldNames());
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
        for (Map.Entry<String, Object> e : map.entrySet()) {
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
    private Object fromUnion(ODocument doc, Schema.Field field,Schema schem, String vertf) {
        Object result = null;
        
        Schema.Type type0 = schem.getTypes().get(0).getType();
        Schema.Type type1 = schem.getTypes().get(1).getType();
        
        
        if (!type0.equals(type1)
        && (type0.equals(Schema.Type.NULL) || type1.equals(Schema.Type.NULL))) {
            Schema innerSchem;
            if(type0.equals(Schema.Type.NULL))
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
    
}
