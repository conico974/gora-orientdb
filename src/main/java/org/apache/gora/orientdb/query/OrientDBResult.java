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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Vertex;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.gora.orientdb.store.OrientDBStore;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.gora.orientdb.store.OrientDBStore.nameOfHost;
import static org.apache.gora.orientdb.store.OrientDBStore.nameOfPassword;
import static org.apache.gora.orientdb.store.OrientDBStore.nameOfUrl;
import static org.apache.gora.orientdb.store.OrientDBStore.nameOfUser;

/**
 *
 * @author Nicolas
 */
public class OrientDBResult<K, T extends PersistentBase> extends ResultBase<K, T> {
    
    public static final Logger LOG = LoggerFactory.getLogger(OrientDBResult.class);
    
    private List<ODocument> itVert;
    private int cursor;
    private ODatabaseDocumentTx odb;

    public int getCursor() {
        return cursor;
    }

    public void setCursor(int cursor) {
        this.cursor = cursor;
    }
    
    public void setOdb(ODatabaseDocumentTx odb) {
        this.odb = odb;
    }

    
    public List<ODocument> getItVert() {
        return itVert;
    }

    public void setItVert(List<ODocument> itVert) {
        this.itVert = itVert;
    }

    public OrientDBResult(DataStore<K, T> dataStore, Query<K, T> query) {
        super(dataStore, query);
        cursor = 1;
    }

    @Override
    protected boolean nextInner() throws IOException {
        if(itVert==null)
            return false;
        odb = OrientDBStore.factory.acquire(nameOfHost+":"+nameOfUrl, nameOfUser, nameOfPassword);
        odb.commit();
        odb.begin();
        ODocument obj=null;
        if(cursor <= itVert.size()) {
            
                //if(odb.existsUserObjectByRID((ORID)itVert.get(cursor-1)))
            for(int i=1;i<OrientDBStore.MAX_RETRIES; i++){
                try{
                    obj = odb.load((ORID)itVert.get(cursor-1));
                }catch(ORecordNotFoundException e){
                    odb.reload();
                    LOG.warn("No object with this id : "+(ORID)itVert.get(cursor-1)+"   Retry n° : "+i);
                }
            }
                //else
                if(obj==null)
                    throw new IOException("Object don't exist");
                
                cursor++;
                offset=cursor;
                this.key = (K) obj.field("id");
                try{
                this.persistent = ((OrientDBStore<K, T>) getDataStore()).newInstanceT(obj, getQuery().getFields());
                odb.commit();
                odb.close();
            }catch(Exception e) {
                LOG.error("Error in OrientDBResult at nextInner :",e.fillInStackTrace());
                throw new IOException(e.initCause(e));
            }
            //this.persistent.clearDirty();
            LOG.info("OrientDBResult cursor : "+cursor+"  For key :"+obj.field("id").toString());
            return true;
        }else
            return false;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float result = (float)itVert.size()/(float)cursor;
        return result;
    }
    
    @Override
    public void close() throws IOException{
        cursor=1;
    }
    
}
