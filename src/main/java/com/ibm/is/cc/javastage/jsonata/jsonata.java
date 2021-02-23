package com.ibm.is.cc.javastage.jsonata;


import java.io.IOException;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.math.BigInteger;

import com.ibm.is.cc.javastage.api.Capabilities;
import com.ibm.is.cc.javastage.api.ColumnMetadata;
import com.ibm.is.cc.javastage.api.ColumnMetadataImpl;
import com.ibm.is.cc.javastage.api.Configuration;
import com.ibm.is.cc.javastage.api.InputLink;
import com.ibm.is.cc.javastage.api.InputRecord;
import com.ibm.is.cc.javastage.api.Link;
import com.ibm.is.cc.javastage.api.OutputLink;
import com.ibm.is.cc.javastage.api.OutputRecord;
import com.ibm.is.cc.javastage.api.Processor;
import com.ibm.is.cc.javastage.api.PropertyDefinition;



public class jsonata extends Processor {
    private InputLink m_inputLink;
    private OutputLink m_outputLink;
    private String jsondata;
    private String outputField;
    private String query;
    private Boolean serialize;
    private Boolean expand;


    public jsonata() {
        super();
    }

    public Capabilities getCapabilities() {
        Capabilities capabilities = new Capabilities();
        // Set minimum number of input links to 1
        capabilities.setMinimumInputLinkCount(1);
        // Set maximum number of input links to 1
        capabilities.setMaximumInputLinkCount(1);
        // Set minimum number of output stream links to 1
        capabilities.setMinimumOutputStreamLinkCount(1);
        // Set maximum number of output stream links to 1
        capabilities.setMaximumOutputStreamLinkCount(1);
        // Set maximum number of reject links to 0
        capabilities.setMaximumRejectLinkCount(0);
        // Set is Wave Generator to false
        capabilities.setIsWaveGenerator(false);
        return capabilities;
    }

    public List < PropertyDefinition > getUserPropertyDefinitions() {
        List < PropertyDefinition > propList = new ArrayList < PropertyDefinition > ();

        propList.add(new PropertyDefinition("jsondata",
            "",
            "jsondata",
            "jsondata",
            PropertyDefinition.Scope.STAGE));
        propList.add(new PropertyDefinition("query",
            "$",
            "query",
            "query",
            PropertyDefinition.Scope.STAGE));
        propList.add(new PropertyDefinition("output",
            "",
            "output field",
            "output field",
            PropertyDefinition.Scope.STAGE));
        propList.add(new PropertyDefinition("serialize",
            "false",
            "serialize",
            "serialize",
            PropertyDefinition.Scope.STAGE));
        propList.add(new PropertyDefinition("expand",
            "false",
            "expand",
            "expand",
            PropertyDefinition.Scope.STAGE));
        return propList;
    }

    public boolean validateConfiguration(Configuration configuration,
        boolean isRuntime) throws Exception {
        m_inputLink = configuration.getInputLink(0);
        m_outputLink = configuration.getOutputLink(0);
        Properties userStageProperties = configuration.getUserProperties();
        jsondata = userStageProperties.getProperty("jsondata");
        outputField = userStageProperties.getProperty("output");
        query = userStageProperties.getProperty("query");
        if (userStageProperties.getProperty("serialize").toUpperCase().equals("TRUE")) {
            serialize = true;
        } else {
            serialize = false;
        }

        if (userStageProperties.getProperty("expand").toUpperCase().equals("TRUE")) {

            expand = true;
        } else {
            expand = false;
        }


        return true;
    }
    public List < ColumnMetadata > getAdditionalOutputColumns(Link outputLink, List < Link > inputLinks, Properties stageProperties) {
        List < ColumnMetadata > addtionalColumns = new ArrayList < ColumnMetadata > ();
        ColumnMetadataImpl output = new ColumnMetadataImpl(stageProperties.getProperty("output"), ColumnMetadata.SQL_TYPE_VARCHAR);
        output.setNullable(true);
        addtionalColumns.add(output);
        return addtionalColumns;
    }


    public void process() throws Exception {
        Expressions expr = null;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonObj = null;
        //jedis.set("foo", "bar");
        do {
            InputRecord inputRecord = m_inputLink.readRecord();
            if (inputRecord == null) {
                // No more input
                break;
            }
            String json = (String) inputRecord.getValue(jsondata);
            List < ColumnMetadata > outCols = m_outputLink.getColumnMetadata();
            List < String > outColsNames = new ArrayList < String > ();

            for (ColumnMetadata s: outCols) {

                if (s.getName() != outputField) {
                    outColsNames.add(s.getName());
                }
            }
            String expression = query;
            try {
                jsonObj = mapper.readTree(json);
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            try {
                expr = Expressions.parse(expression);
            } catch (ParseException e) {
                System.err.println(e.getLocalizedMessage());
            } catch (EvaluateRuntimeException ere) {
                System.out.println(ere.getLocalizedMessage());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            JsonNode result = expr.evaluate(jsonObj);

/*

            if (result == null) {
                System.out.println("** no match **");
            }
 */
            System.out.println("expand: " + expand);
            System.out.println("serialize: " + serialize);
            System.out.println("isArray: " + result.isArray());
            if (expand && result.isArray()) {
                for (int i = 0; i < result.size(); i++) {
                    OutputRecord outputRecord = m_outputLink.getOutputRecord();
                    System.out.println("isObject: " + result.get(i).isObject());
                    if (serialize && result.get(i).isObject()) {
                        //Iterator < String > fieldNames = result.get(i).fieldNames();
                    	Iterator < String > fieldNames = outColsNames.iterator();
                        while (fieldNames.hasNext()) {
                            String fieldName = fieldNames.next();
                            //if (outColsNames.contains(fieldName)) {
                            if (result.get(i).get(fieldName) != null) {

                            	if (result.get(i).get(fieldName).isObject()){
                                outputRecord.setValue(fieldName, mapper.writeValueAsString(result.get(i).get(fieldName)));
                            	}
                            	else {
                            		outputRecord.setValue(fieldName, result.get(i).get(fieldName).asText());
                            	}
                            
                            }
                            else {
                            	int pos = outColsNames.indexOf(fieldName);
                            	String derivation = outCols.get(pos).getDerivation();
                            	System.out.println(fieldName + "->" + derivation);
                            	if (derivation != null) {
                            	  outputRecord.setValue(fieldName, result.get(i).at(derivation).asText());
                            	}
                            }
                        }
                    } //else {
                    	if (result.get(i).isObject()){
                            outputRecord.setValue(outputField, mapper.writeValueAsString(result.get(i)));
                      	}
                      	else {
                      	  outputRecord.setValue(outputField, result.get(i).asText());
                      	}

                   // }
                    m_outputLink.writeRecord(outputRecord);
                }
            } else {
                System.out.println("isObject: " + result.isObject());
                OutputRecord outputRecord = m_outputLink.getOutputRecord();
                if (serialize && result.isObject()) {
                    Iterator < String > fieldNames = outColsNames.iterator();
                    while (fieldNames.hasNext()) {

                        String fieldName = fieldNames.next();
                        if (result.get(fieldName) != null) {
                            outputRecord.setValue(fieldName, mapper.writeValueAsString(result.get(fieldName)));
                        }
                        else {
                        	int pos = outColsNames.indexOf(fieldName);
                        	String derivation = outCols.get(pos).getDerivation();
                        	System.out.println(fieldName + "->" + derivation);
                        	if (derivation != null) {
                        	  outputRecord.setValue(fieldName, result.at(derivation).asText());
                        	}
                        }
                    }
                } //else {
                	System.out.println(result.getNodeType());
                	if (result.isObject() || result.isArray()){
                      outputRecord.setValue(outputField, mapper.writeValueAsString(result));
                	}
                	else {
                	  outputRecord.setValue(outputField, result.asText());
                	}

               // }
                m_outputLink.writeRecord(outputRecord);
            }


        } while (true);
    }


}