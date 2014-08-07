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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.gora.orientdb.store.OrientDBStore.LOG;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Nicolas
 */
public class OrientDBMappingBuilder {
    // Document description
    static final String TAG_VERTEX = "vertex";
    static final String ATT_COLLECTION = "className";
    static final String STAG_VERTEX_FIELD = "field";
    static final String ATT_NAME = "name";
    static final String ATT_TYPE = "type";
    static final String STAG_SUBVERTEX = "subvertex";

    // Class description
    static final String TAG_CLASS = "class";
    static final String ATT_KEYCLASS = "keyClass";
    static final String ATT_VERTEX = "vertex";
    static final String TAG_FIELD = "field";
    static final String ATT_FIELD = "vertfield";
    
    private final OrientDBStore datastore;
    
    private OrientDBMapping mapping;
    
    public OrientDBMappingBuilder(final OrientDBStore store){
        this.datastore = store;
        this.mapping = new OrientDBMapping();
    }

    void fromFile(String uri) throws IOException {
        try {
            SAXBuilder saxBuilder = new SAXBuilder();
            InputStream is = getClass().getResourceAsStream(uri);
            if (is == null) {
                String msg = "Unable to load the mapping from resource '"+uri+ "' as it does not appear to exist! "+"Trying local file.";
		LOG.warn(msg);
                try {
                    is = new FileInputStream(uri);
                }catch( Exception e) {
                    LOG.error("FileInputStream error "+ e.getMessage(),e.fillInStackTrace());
                    throw new NullPointerException("Unable to load file neither local or from resource");
                }
            }
            Document doc = saxBuilder.build(is);
            if(doc == null)
                throw new IllegalStateException("doc is null");
            Element root = doc.getRootElement();
            if(root == null)
                throw new IllegalStateException("root is null");
            // No need to use documents descriptions for now...
            // Extract class descriptions
            @SuppressWarnings("unchecked")
            List<Element> classElements = root.getChildren(TAG_CLASS);
            if(classElements == null)
                throw new IllegalStateException("classelem is null");
            for(Element classElement: classElements) {
                if(classElement.getAttributeValue(ATT_KEYCLASS).equals(datastore.getKeyClass().getName())&& classElement.getAttributeValue(ATT_NAME).equals(datastore.getPersistentClass().getName())) {
                    loadPersistentClass(classElement, datastore.getPersistentClass());
                    break; // only need that
                }
            }
        }catch( IOException e) {
            LOG.error("IOexcpetion loding file :"+e.getMessage());
            throw e;
        }catch (NullPointerException e) {
            LOG.error("NUllPOinter exception while loading file : ",e.fillInStackTrace());
        } catch (JDOMException e) {
            LOG.error("JDOM exception"+e.getMessage(),e.getCause());
        }
    }

    OrientDBMapping build() {
        if (mapping.getOClassName() == null) 
		throw new IllegalStateException("A OClass is not specified");
	return mapping;
    }

    private void loadPersistentClass(Element classElement, Class<?> persistentClass) {
        String docNameFromMapping = classElement.getAttributeValue(ATT_VERTEX);
	String collName = datastore.getSchemaName(docNameFromMapping, persistentClass);

	mapping.setOClassName(collName);
	//docNameFromMapping could be null here
	if (!collName.equals(docNameFromMapping)) {
            LOG.info("Keyclass and nameclass match but mismatching table names " 
                    + " mappingfile schema is '" + docNameFromMapping 
                    + "' vs actual schema '" + collName + "' , assuming they are the same.");
            if (docNameFromMapping != null) {
		mapping.renameOClass(docNameFromMapping, collName);
            }else
                LOG.error("docNameFromMapping is null");
	}

        LOG.info("Vertex name : "+docNameFromMapping);
	// Process fields declaration
	@SuppressWarnings("unchecked")
	List<Element> fields = classElement.getChildren(TAG_FIELD);
        if(fields == null)
            LOG.error("cannot find "+TAG_FIELD);
	for(Element field: fields) {
	mapping.addClassField(docNameFromMapping,
		field.getAttributeValue(ATT_NAME),
		field.getAttributeValue(ATT_FIELD),
		field.getAttributeValue(ATT_TYPE));
	}
    }
    
}