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

package org.apache.gora.orientdb.query;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.IOUtils;

/**
 *
 * @author Nicolas
 * 
 * Used to create a query over a particular cluster instead of over the whole class
 */
public class OrientDBPartitionQuery extends QueryBase
    implements PartitionQuery{
    
    protected Query baseQuery;
    
    protected String[] clusterName;
    
    public OrientDBPartitionQuery(){
        super(null);
        clusterName = new String[1];
    }
    
    public OrientDBPartitionQuery(Query query){
        super(query.getDataStore());
        this.baseQuery = query;
        setStartKey(query.getStartKey());
        setEndKey(query.getEndKey());
        this.dataStore = (DataStoreBase) baseQuery.getDataStore();
        clusterName = new String[1];
    }
    
    public void setClusterName(String clusterName){
        this.clusterName[0] = clusterName;
    }
    
    public String getClusterName(){
        return clusterName[0];
    }
    
      @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    IOUtils.serialize(getConf(), out, baseQuery);
    IOUtils.writeStringArray(out, clusterName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    try {
      baseQuery = IOUtils.deserialize(getConf(), in, null);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
    clusterName = IOUtils.readStringArray(in);
    //we should override the data store as basequery's data store
    //also we may not call super.readFields so that temporary this.dataStore
    //is not created at all
    this.dataStore = (DataStoreBase) baseQuery.getDataStore();
  }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    
}
