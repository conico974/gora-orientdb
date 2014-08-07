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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;

/**
 *
 * @author Nicolas
 * @param <K>
 * @param <T>
 */
public class OrientDBQuery<K,T extends PersistentBase> extends QueryBase<K,T> {
    

    public OrientDBQuery(DataStore<K, T> dataStore) {
        super(dataStore);
        
    }
    
    public OrientDBQuery() {
        super(null);
    }
    
    public static String getSQLQuery(Query<?,?> query,String className) {
        String quer="select expand(rid) from index:"+className+".id";
        if ((query.getStartKey() != null) && (query.getEndKey() != null)
        && query.getStartKey().equals(query.getEndKey())) {
            quer = "select expand(rid) from index:"+className+".id where key=\""+query.getStartKey()+"\"";
        } else {
            if (query.getStartKey() != null && query.getEndKey()!=null)
                quer = "select expand(rid) from index:"+className+".id where key between \""+query.getStartKey()+"\" and \""+query.getEndKey()+"\"";
            else if (query.getEndKey() == null && query.getStartKey()!=null)
                quer = "select expand(rid) from index:"+className+".id where key >=\""+query.getStartKey()+"\"";
            else if (query.getEndKey() != null)
                quer = "select expand(rid) from index:"+className+".id where key <=\""+query.getEndKey()+"\"";
        }
        return quer;
    }
    
}