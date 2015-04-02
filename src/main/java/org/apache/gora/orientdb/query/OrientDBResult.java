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

package org.apache.gora.orientdb.query;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import org.apache.gora.orientdb.store.OrientDBStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Nicolas
 */
public class OrientDBResult<K, T extends PersistentBase> extends ResultBase<K, T> {
    
    public static final Logger LOG = LoggerFactory.getLogger(OrientDBResult.class);
        
    private final SynchronousQueue<ODocument> synchQueue = new SynchronousQueue();
    private String queryStr;
    private OSQLAsynchQuery<ODocument> asynchQuer;
    private ODatabaseDocumentTx odb;
    private long size;
    private ODocument currentDocument;

    public ODocument getCurrentDocument() {
        return currentDocument;
    }

    public void setOdb(ODatabaseDocumentTx odb) {
        this.odb = odb;
    }

    public long getSize() {
        return size;
    }
    
    
    
    public OrientDBResult(DataStore<K, T> dataStore, Query<K, T> query) {
        super(dataStore, query);
        offset=0;
    }
    
    public void initResult(String quer, final ODatabaseDocumentTx odb){
        this.queryStr= quer;
        size = ((ODocument)odb.query(new OSQLSynchQuery("select count(*) from ("+queryStr+")")).get(0)).field("count"); // Compute the number of result
        asynchQuer = new OSQLAsynchQuery(queryStr,new OCommandResultListener(){
        @Override
        public boolean result(Object o) {
            ODocument doc = (ODocument)o;
            try {
                synchQueue.put(doc);
            } catch (InterruptedException ex) {
                LOG.error("",ex.fillInStackTrace());                       
            }
            return true;
        }

        @Override
        public void end() {
        }
        
    });    
        Thread t = new Thread(new Runnable(){

            @Override
            public void run() {
                try{
                odb.command(asynchQuer).execute();
                }catch(Exception e){
                    LOG.error(e.getMessage());
                }
            }
            
        });
        t.start();
    }

    @Override
    protected boolean nextInner() throws IOException {
        if(offset==size)
            return false;
        ODocument obj = null;
        try {
            obj = synchQueue.take();
            currentDocument = obj;
        }catch(InterruptedException e){
            LOG.error("Interrupted Exception",e.fillInStackTrace());
        }
        this.key = (K)obj.field("id");
        try{
            this.persistent = ((OrientDBStore<K, T>) getDataStore()).newInstanceT(obj, getQuery().getFields());
            LOG.info("Retrieving record with key :"+this.key);
        }catch(Exception e){
            LOG.error("Generic exception on retrieving record from Query",e.fillInStackTrace());
        }
        return true;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float result = (float)offset/(float)size;
        return result;
    }
    
    @Override
    public void close() throws IOException{
        offset=0;
    }
    
}
