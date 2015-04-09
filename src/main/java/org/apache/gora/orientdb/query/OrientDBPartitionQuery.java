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

import org.apache.gora.query.Query;
import org.apache.gora.query.impl.PartitionQueryImpl;

/**
 *
 * @author Nicolas
 * 
 * Used to create a query over a particular cluster instead of over the whole class
 */
public class OrientDBPartitionQuery extends PartitionQueryImpl{
    
    private String clusterName;
    
    public OrientDBPartitionQuery(Query query){
        super(query);
    }
    
    public void setClusterName(String clusterName){
        this.clusterName = clusterName;
    }
    
    public String getClusterName(){
        return clusterName;
    }
    
    
}
