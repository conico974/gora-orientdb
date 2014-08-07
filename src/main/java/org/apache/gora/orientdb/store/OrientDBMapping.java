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

import java.util.HashMap;
import java.util.regex.Pattern;

/**
 *
 * @author Nicolas
 */
public class OrientDBMapping {
    
    public static enum VertexFieldType {
		BINARY,  // byte[]
                BYTE,    // byte
		INT32,   // 4 bytes, signed integer
		INT64,   // 8 bytes, signed integer
		DOUBLE,  // 8 bytes, float
		STRING,  // string
		DATE,    // date
		LIST,    // a list
                MAP,     //a map
		VERTEX, // a Vertex
                UNION
	}
    
    private Pattern validOrientVertexField = 
			Pattern.compile("[a-z0-9\\-]+", Pattern.CASE_INSENSITIVE);
    
    private String OClassName;  // Name of the cluster of your class
    
    /** Mapping between the class fields and the OrienDB Vertex fields */
    private HashMap<String, String> classToVertex = new HashMap<String, String>();
    /** Mapping between the OrienDB Vertex fields and the class fields */
    private HashMap<String, String> vertexToClass = new HashMap<String, String>();
 
    /** Mongo document description */
    private HashMap<String, VertexFieldType> vertexFields = new HashMap<String, VertexFieldType>();
    
    public void renameOClass(String oldName, String newName) {
		// FIXME
	OClassName = newName;
    }
	
	/** Getter for the name of the collection
     * @return  */
    public String getOClassName() {
	return OClassName;
    }

	/** Setter for the name of the collection
     * @param clustName */
    public void setOClassName(String clustName) {
		// FIXME check name is ok
	OClassName = clustName;
    }
    
    	private void newVertexField(String name, VertexFieldType type) {
		// First of all, split the field to identify the various levels
		String[] breadcrumb = name.split("\\.");
		// Process each intermediate field to check they are of Map type
		StringBuilder partialFieldName = new StringBuilder();
		for(int i=0 ; i<breadcrumb.length-1 ; i++) {
			// Build intermediate field name
			String f = breadcrumb[i];
                        if(!isValidFieldName(f))
                            throw new IllegalArgumentException(
						"'"+f+"' is an invalid field name for a Orient vertex");
			partialFieldName.append(f);
			// Check field exists or not and is of valid type
			String intermediateFieldName = partialFieldName.toString();
			if ( vertexFields.containsKey(intermediateFieldName) ) {
				if (vertexFields.get(intermediateFieldName) != VertexFieldType.VERTEX)
					throw new IllegalStateException("The field '" 
							+intermediateFieldName+"' is already registered in "
							+"a type not compatible with the new definition of "
							+"field '"+name+"'.");
			} else {
				vertexFields.put(intermediateFieldName, VertexFieldType.VERTEX);
			}
			partialFieldName.append(".");
		}
		// Check the field does not already exist, insert the complete field
		if ( vertexFields.containsKey(name) 
				&& (vertexFields.get(name) != type) )
			throw new IllegalStateException("The field '"+name+"' is already "
					+ "registered with a different type.");
		vertexFields.put(name, type);
	}
    public void addClassField(String docNameFromMapping,
			String classFieldName, String docFieldName, String fieldType) {
		// Register a new field for the Orient vertex
		newVertexField(docFieldName, 
				VertexFieldType.valueOf(fieldType.toUpperCase()));
		// Register the mapping
		if ( classToVertex.containsKey(classFieldName) ) {
			if ( !classToVertex.get(classFieldName).equals(docFieldName) ) {
				throw new IllegalStateException("The class field '" 
					+ classFieldName + "' is already registered in the mapping"
					+ " with the document field '" + classToVertex.get(classFieldName)
					+ " which differs from the new one '" + docFieldName + "'." );
			}
		} else {
			classToVertex.put(classFieldName, docFieldName);
			vertexToClass.put(docFieldName, classFieldName);
		}
	}
    
    public String getVertexField(String field) {
		return classToVertex.get(field);
	}
	
	/**
	 * Package private method to retrieve the type of a document field.
     * @param field
     * @return 
	 */
	protected VertexFieldType getVertexFieldType(String field) {
		return vertexFields.get(field);
	}
        
    private boolean isValidFieldName(String f) {
        return validOrientVertexField.matcher(f).matches();
    }
}
