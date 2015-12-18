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
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.gora.orientdb.store.OrientDBMapping;
import static org.apache.gora.orientdb.store.OrientDBStore.LOG;
import org.apache.gora.orientdb.utils.Utils;
import org.apache.gora.persistency.Persistent;

/**
 *
 * @author Nicolas
 */
public class Serialization {
    
    private ODatabaseDocumentTx odb;
    private OrientDBMapping mapping;

    public Serialization(ODatabaseDocumentTx odb, OrientDBMapping mapping) {
        this.odb = odb;
        this.mapping = mapping;
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
    Iterator<Schema> it = pUnionSchema.getTypes().iterator();
    while ( it.hasNext() ){
      Schema.Type schemaType = it.next().getType();
      if (pValue instanceof Utf8 && schemaType.equals(Schema.Type.STRING))
        return unionSchemaPos;
      else if (((pValue instanceof ByteBuffer)) && schemaType.equals(Schema.Type.BYTES))
        return unionSchemaPos;
      else if (pValue instanceof Integer && schemaType.equals(Schema.Type.INT))
        return unionSchemaPos;
      else if (pValue instanceof Long && schemaType.equals(Schema.Type.LONG))
        return unionSchemaPos;
      else if (pValue instanceof Double && schemaType.equals(Schema.Type.DOUBLE))
        return unionSchemaPos;
      else if (pValue instanceof Float && schemaType.equals(Schema.Type.FLOAT))
        return unionSchemaPos;
      else if (pValue instanceof Boolean && schemaType.equals(Schema.Type.BOOLEAN))
        return unionSchemaPos;
      else if (pValue instanceof Map && schemaType.equals(Schema.Type.MAP))
        return unionSchemaPos;
      else if (pValue instanceof List && schemaType.equals(Schema.Type.ARRAY))
        return unionSchemaPos;
      else if (pValue instanceof Persistent && schemaType.equals(Schema.Type.RECORD))
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
    public void putAsOrientVertex(final Schema.Field field, final ODocument v ,final String key, final Object value, final Schema schem, ODatabaseDocumentTx od) {
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
                    ORecordBytes record = new ORecordBytes(((ByteBuffer)value).array());
                    odb.save(record);
                    v.field(key, record);
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
    private Schema.Type getMapType(Object pValue) {
        if (pValue instanceof Utf8)
        return Schema.Type.STRING;
      else if (pValue instanceof ByteBuffer)
        return Schema.Type.BYTES;
      else if (pValue instanceof Integer)
        return Schema.Type.INT;
      else if (pValue instanceof Long)
        return Schema.Type.LONG;
      else if (pValue instanceof Double)
        return Schema.Type.DOUBLE;
      else if (pValue instanceof Float )
        return Schema.Type.FLOAT;
      else if (pValue instanceof Boolean)
        return Schema.Type.BOOLEAN;
      else if (pValue instanceof Map )
        return Schema.Type.MAP;
      else if (pValue instanceof List)
        return Schema.Type.ARRAY;
      else if (pValue instanceof Persistent)
        return Schema.Type.RECORD;
        
        return Schema.Type.NULL;
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
        try{
            if(v.field(key)==null){
                if(intRecName.equals(mapping.getOClassName()))
                    record = od.newInstance(mapping.getOClassName());
                else
                    record = od.newInstance("intern"+mapping.getOClassName());  
            }else
                record = od.load((ODocument)v.field(key));
            for (Schema.Field member: schem.getFields()) {
                    Object recValue = ((Persistent) value).get(member.pos());
                    String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                    LOG.debug("Member name :  "+innerFieldName);
                    if(!Utils.isValidFieldName(innerFieldName))
                        continue;
                    putAsOrientVertex(member,record,innerFieldName,recValue,member.schema(),od);
            }
            od.save(record);
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
    private Object toOrientEmbeddedMap(Map<Utf8, ?> jmap, Schema schem, Schema.Field field, ODocument v, ODatabaseDocumentTx od) {
        // Handle null case
	if (jmap == null) 
		return null;
	// Handle regular cases
	Map map = new HashMap(); 
        try{
	for(Map.Entry<Utf8, ?> e: jmap.entrySet()) {
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
                ORecordBytes recordBytes = new ORecordBytes(((ByteBuffer)e.getValue()).array());
                odb.save(recordBytes);
                map.put(vKey,recordBytes);
                break;
            case LONG:
                map.put(vKey, (Long)e.getValue());
                break;
            case INT:
                map.put(vKey, (Integer)e.getValue());
                break;
            case RECORD:
                if ( e.getValue() == null ) break;
                    
                LOG.debug("Record in a map");
                    schem.getClass();
                    ODocument record = odb.newInstance(mapping.getOClassName()); // WTF?????????
                    for (Schema.Field member: schem.getFields()) {
                            Object recValue = ((Persistent) e.getValue()).get(member.pos());
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!Utils.isValidFieldName(innerFieldName))
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
     * Not Safe to use with record
     *
     * @param genericArray
     * @param type
     * @param schem
     * @param v
     * @param key
     * @param od 
     */
    private void toOrientEmbeddedList(List genericArray, Schema.Type type,Schema schem, ODocument v,String key, ODatabaseDocumentTx od) {
        // Handle null case
	if (genericArray == null) {
            LOG.info("The given list is null");
		return;
        }
	// Handle regular cases
	List<Object> list = null;
        
        //list = (ArrayList)v.field(key);
        if(list == null)
            list = new ArrayList();
       
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
                ORecordBytes record = new ORecordBytes(((ByteBuffer)item).array());
                odb.save(record);
                list.add(record);
                break;
            case RECORD:
                graphRec = true;
                Schema innerSchem = schem.getElementType();
                //Schema.Field label = innerSchem.getFields().get(1);
                String edgeName = ((Persistent)item).get(1).toString();
                LOG.debug("Temporary!!!   edgeName="+edgeName);
                Persistent persistDoc = (Persistent)((Persistent)item).get(2);
                //OrientGraph graph = new OrientGraph(od);
                if(checkKey(item)){
                    OIndex odict = od.getMetadata().getIndexManager().getIndex(persistDoc.getSchema().getName()+".id");
                    Object docKey = getKey(((Persistent)item).get(2));
                    LOG.info("key of this vertex"+docKey);
                    if(odict.get(docKey)==null)
                        throw new IllegalStateException("You must provide a valid key to create the Edge");
                    //Vertex outVert = graph.getVertex(v.getIdentity());
                    ODocument outVert = v;
                    LOG.info("key of in vertex"+v.getIdentity());
                    ODocument inVert = od.getRecord((OIdentifiable)odict.get(docKey)); // Possible incorrect cast
                    //Vertex inVert = graph.getVertex(odict.get(docKey));
                    OIndex index = od.getMetadata().getIndexManager().getIndex("uni_"+edgeName);
                    OCompositeKey compKey = new OCompositeKey();
                    compKey.addKey((ORecordId)inVert.getIdentity());
                    compKey.addKey((ORecordId)outVert.getIdentity());
                    //OrientEdge e = null;
                    ODocument edge = null;
                    if(!index.contains(compKey)){
                        //e = graph.addEdge(null, outVert, inVert, edgeName);
                        OrientGraph graph = new OrientGraph(od);
                        LOG.info("Edge don't exist in graph");
                        OrientVertex outVertex = graph.getVertex(outVert.getIdentity());
                        OrientVertex inVertex = graph.getVertex(inVert.getIdentity());
                        edge = outVertex.addEdge(null, inVertex, edgeName).getRecord();
//                        edge = od.newInstance(edgeName);
//                        edge.field("out",outVert);
//                        edge.field("in", inVert);
                        //index.put(compKey, edge);
                    }
                    else{
                        edge = od.getRecord((OIdentifiable)index.get(compKey));
                        //e = graph.getEdge(index.get(compKey));
                    }

                    //OrientEdge e = (OrientEdge)(inVert.addEdge(edgeName, outVert));
                    int size = ((Persistent)item).getUnmanagedFields().size();
                    if(size>3){
                        for(int i=2;i<size; i++){
                            LOG.debug("TEMPORARY!!!!  The edges has some properties");
                            Object recValue = ((Persistent) item).get(i);
                            Schema.Field member = innerSchem.getFields().get(i);
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!Utils.isValidFieldName(innerFieldName))
                                continue;
                            putAsOrientVertex(member,edge,innerFieldName,recValue,member.schema(),od);
                        }
                    }

                }else{
                    
                    //Vertex inVert = graph.getVertex(v.getIdentity());
                    ODocument outDoc = toOrientDBDocument(persistDoc,persistDoc.getSchema(),v,"",od);

                    if(outDoc==null)
                        LOG.error("OutDoc is null");
                    if(v==null)
                        LOG.error("In Vertex is null");
//                    OrientVertex outVerte = ;
                    //Vertex outVert = graph.getVertex(outDoc.getIdentity()); // TODO change this to OrientDBVertex
                    if(outDoc==null)
                        LOG.error("OutVertex is null, identity of outDoc : "+outDoc.getIdentity());
                    OIndex index = od.getMetadata().getIndexManager().getIndex("uni_"+edgeName);
                    List<ODocument> vertKey = new ArrayList(); // TODO replace list by a more efficient one
                    vertKey.add(v);
                    vertKey.add(outDoc);
//                    OrientEdge e = null;
                    ODocument edge = null;
                    if((edge = (ODocument)index.get(vertKey))==null){
                        //e = (OrientEdge)(inVert.addEdge(edgeName, outVert));
                        LOG.info("Document don't exist in graph");
                        edge = od.newInstance(edgeName);
                        edge.field("in", v); //Possible null pointer
                        edge.field("out", outDoc);
                        edge.save();
                    }
                    int size = ((Persistent)item).getUnmanagedFields().size();
                    if(size>1){
                        for(int i=2;i<size; i++){
                            Object recValue = ((Persistent) item).get(i);
                            Schema.Field member = innerSchem.getFields().get(i);
                            String innerFieldName = (mapping.getVertexField(member.name())!=null) ? mapping.getVertexField(member.name()) : member.name();
                            LOG.debug("Member name :  "+innerFieldName);
                            if(!Utils.isValidFieldName(innerFieldName))
                                continue;
                            putAsOrientVertex(member,edge,innerFieldName,recValue,member.schema(),od);
                        }
                    }
                }
                od.commit();
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
            LOG.debug("The key is a vertex");
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
    
}
