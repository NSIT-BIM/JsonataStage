package com.ibm.is.cc.javastage.jsonata;

import java.io.IOException;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.time.*;
import java.util.Date;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;


//import java.math.BigInteger;

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
import com.ibm.is.cc.javastage.api.RejectRecord;
import com.ibm.is.cc.javastage.api.PropertyDefinition;
import com.ibm.is.cc.javastage.api.Logger;


public class jsonata extends Processor {
  private InputLink m_inputLink;
  private OutputLink m_outputLink;
  private OutputLink m_rejectLink;
  private String jsondata;
  private String outputField;
  private Boolean serialize;
  private String mode;
  private Boolean expand;
  private Expressions expr;
  private int inputRecords = 0;
  private int outputRecords = 0;
  private int rejectRecords = 0;
  private int dropRecords = 0;
  private int m_nodeID = -1;  

  public jsonata() {
    super();
    Logger.setComponentID("JSONATA");
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
    // Set maximum number of reject links to 1
    capabilities.setMaximumRejectLinkCount(1);
    // Set is Wave Generator to false
    capabilities.setIsWaveGenerator(false);
    capabilities.setIsRunOnConductor(true); 
    return capabilities;
  }

  public List < PropertyDefinition > getUserPropertyDefinitions() {
    List < PropertyDefinition > propList = new ArrayList < PropertyDefinition > ();

    propList.add(new PropertyDefinition("jsondata", "", "jsondata", "Input field with json data", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("query", "$", "query", "Jsonata expression", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("output", "", "output field", "Output field to populate with result set", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("serialize", "false", "serialize", "Serialize objet", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("expand", "false", "expand", "Expand arrays to records", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("mode", "drop", "mode", "Drop/Warn/Reject when result set is empty", PropertyDefinition.Scope.STAGE));
    return propList;
  }

  public boolean validateConfiguration(Configuration configuration, boolean isRuntime) throws Exception {
	m_nodeID = configuration.getNodeNumber();      
	
    m_inputLink = configuration.getInputLink(0);
    m_outputLink = configuration.getOutputLink(0);
    m_rejectLink = m_inputLink.getAssociatedRejectLink();
    Properties userStageProperties = configuration.getUserProperties();
    jsondata = userStageProperties.getProperty("jsondata");
    outputField = userStageProperties.getProperty("output");
    String query = userStageProperties.getProperty("query");
   

    if (userStageProperties.getProperty("serialize").equalsIgnoreCase("TRUE")) {
      serialize = true;
    } else {
      serialize = false;
    }
    if (userStageProperties.getProperty("expand").equalsIgnoreCase("TRUE")) {
      expand = true;
    } else {
      expand = false;
    }
    if (userStageProperties.getProperty("mode").equalsIgnoreCase("DROP")) {
      mode = "drop";
    } else if (userStageProperties.getProperty("mode").equalsIgnoreCase("REJECT")) {
      mode = "reject";
    }
    else {
      mode = "warn";
    }
    if (m_rejectLink == null && mode.equals("reject")) {
      Logger.warning(1,"Mode set to reject but no reject link, records could be silently dropped. Set mode to drop/warn or add reject link");
    }
    //Logger.information(1,"Parsing query");
    expr = null;
    try {
      expr = Expressions.parse(query);
    } catch(ParseException e) {
      Logger.fatal("Parsing error 1 for query " + query);
      terminate(true);
    } catch(EvaluateRuntimeException ere) {
      Logger.fatal("Parsing error 2 for query " + query);
      terminate(true);
    } catch(JsonProcessingException e) {
      Logger.fatal("Parsing error 3 for query " + query);
      terminate(true);
    } catch(IOException e) {
      Logger.fatal("Parsing error 4 for query " + query);
      terminate(true);
    }
    if (m_nodeID == -1) {
	    Logger.information(2,"query: " + expr);
	    Logger.information(3,"input link: "+jsondata);
	    Logger.information(4,"output link: "+outputField);
	    Logger.information(5,"expand: "+expand);
	    Logger.information(6,"serialize: "+serialize);
	    Logger.information(7,"mode: "+mode);
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
	 
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonObj = null;
    JsonNode result = null;
    boolean validJson = true;
    
    List < ColumnMetadata > outCols = m_outputLink.getColumnMetadata();
    List < String > outColsNames = new ArrayList < String > ();

    for (ColumnMetadata s: outCols) {

      if (s.getName() != outputField) {
        outColsNames.add(s.getName());
      }
    }
    
    Logger.information(10,"Processing input data");
    do {
      Logger.debug(99,"Processing input record");
      InputRecord inputRecord = m_inputLink.readRecord();
      if (inputRecord == null) {
        // No more input
        break;
      }
      String json = (String) inputRecord.getValue(jsondata);
      inputRecords++;
      if (Logger.isDebugEnabled()) {
      Logger.debug(98,json);
      }


      try {
        jsonObj = mapper.readTree(json);
      } catch(IOException e1) {
        if (m_rejectLink != null) {
          if (Logger.isDebugEnabled()) {
          Logger.debug(97,"record rejected because invalid json");
          }
          RejectRecord rejRecord = m_rejectLink.getRejectRecord(inputRecord);
          rejRecord.setErrorText("Invalid json data");
          rejRecord.setErrorCode(1);
          m_rejectLink.writeRecord(rejRecord);
          rejectRecords++;
        }
        else {
          Logger.warning(4,"Invalid json data, add a reject link to handle without warnings");
          //Logger.debug(json);
        }
        validJson = false;

      }
      if (validJson) {
        try {
          //Logger.information("evaluating");
          result = expr.evaluate(jsonObj);
        } catch(EvaluateException e) {
          Logger.warning(5,"Error evaluating");
        }
        //Logger.information("evaluation done");
        if (result == null) {
          if (mode.equals("warn")) {
            Logger.warning(6,"No match");
            dropRecords++;
          }
          else if (mode.equals("reject") && m_rejectLink != null) {
            RejectRecord rejRecord = m_rejectLink.getRejectRecord(inputRecord);
            rejRecord.setErrorText("No match");
            rejRecord.setErrorCode(2);
            if (Logger.isDebugEnabled()) {
            Logger.debug(96,"record rejected because no match");
            }
            m_rejectLink.writeRecord(rejRecord);
            rejectRecords++;
          }
          else {
        	 dropRecords++;
          }
        }
        else {

          if (expand && result.isArray()) {
            for (int i = 0; i < result.size(); i++) {
              OutputRecord outputRecord = m_outputLink.getOutputRecord();
              if (serialize && result.get(i).isObject()) {
                serializeRecord(result.get(i), outCols, outColsNames, outputRecord);
              }
              if (result.get(i).isObject()) {
                try {
                  outputRecord.setValue(outputField, mapper.writeValueAsString(result.get(i)));
                }
                catch(JsonProcessingException e) {
                  Logger.warning(7,"Error processing json result set");
                }
              }
              else {
                outputRecord.setValue(outputField, result.get(i).asText());
              }

              m_outputLink.writeRecord(outputRecord);
              outputRecords++;
            }
          } else {
            OutputRecord outputRecord = m_outputLink.getOutputRecord();
            if (serialize && result.isObject()) {
              serializeRecord( result, outCols, outColsNames, outputRecord);
            }
            if (result.isObject() || result.isArray()) {
              try {
                outputRecord.setValue(outputField, mapper.writeValueAsString(result));
              }
              catch(JsonProcessingException e) {
            	  Logger.warning(8,"Error processing json result set");
              }
            }
            else {
              outputRecord.setValue(outputField, result.asText());
            }
            m_outputLink.writeRecord(outputRecord);
            outputRecords++;
          }

        }
      }
    } while ( true );
    
    Logger.information(100,"Input records:"+inputRecords);
    Logger.information(100,"Output records:"+outputRecords);
    Logger.information(100,"Rejected records:"+rejectRecords);
    Logger.information(100,"Droped records:"+dropRecords);
  }

  private void writeData(String fieldName, JsonNode data, OutputRecord outputRecord, ColumnMetadata OutputCol) {

    String TargetType = "" + OutputCol.getType();
    int TargetLength = OutputCol.getPrecision();
    String SourceType = "" + data.getNodeType();
    if (!SourceType.equals("MISSING")) {
    if (SourceType.equalsIgnoreCase("STRING") && ! TargetType.equals("class java.lang.String") && ! TargetType.equals("class java.sql.Timestamp") && ! TargetType.equals("class java.sql.Date")){
    	Logger.warning(fieldName+" converting "+SourceType+" to "+TargetType);
    }
    if (Logger.isDebugEnabled()) {
    Logger.debug(95,fieldName+":"+SourceType+"->"+TargetType);
    }
    if (TargetType.equals("class java.lang.String")) {
      if (TargetLength > 0 && data.asText().length() > TargetLength) {
        Logger.warning(fieldName + " data truncation");
      }
      outputRecord.setValue(fieldName, data.asText());
    }
    else if (TargetType.equals("class java.lang.Long")) {
      outputRecord.setValue(fieldName, data.asLong());
    }
    else if (TargetType.equals("class java.math.BigDecimal")) {
      outputRecord.setValue(fieldName, data.decimalValue());
    }
    else if (TargetType.equals("class java.lang.Float")) {
      outputRecord.setValue(fieldName, data.floatValue());
    }
    else if (TargetType.equals("class java.lang.Double")) {
      outputRecord.setValue(fieldName, data.doubleValue());
    }
    else if (TargetType.equals("class java.sql.Timestamp") || TargetType.equals("class java.sql.Date")) {
    	try {
	    	Date date = Date.from( Instant.parse( data.asText() ));
	    	outputRecord.setValue(fieldName, date);
    	}
    	catch(DateTimeParseException e){
    	    Logger.debug(94,"Could not convert "+data.asText()+" to UTC timestamp");
    	    try  {
    	    ZonedDateTime date2 = ZonedDateTime.parse(data.asText());
    	    outputRecord.setValue(fieldName, Date.from(date2.toInstant()));
    	    }
    	    catch(DateTimeParseException e1){
    	    	Logger.debug(94,"Could not convert "+data.asText()+" to zoned timestamp");
    	    }
    	}
	 }
   // else if (TargetType.equals("class java.sql.Date")) {
   // 	Logger.warning(11,"Dates are not yet handled");	
//	}
    else {
      outputRecord.setValue(fieldName, data.asText());
    }
    }
    //outputRecord.setValue(fieldName, data.asText());
  }

  private void serializeRecord( JsonNode result, List < ColumnMetadata > outCols, List < String > outColsNames, OutputRecord outputRecord) {
    Iterator < String > fieldNames = outColsNames.iterator();
    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      int pos = outColsNames.indexOf(fieldName);
      ColumnMetadata OutputCol = outCols.get(pos);
      String derivation = outCols.get(pos).getDerivation();
      if (derivation != null) {
        writeData(fieldName, result.at(derivation), outputRecord, OutputCol);
      }
      else if (result.get(fieldName) != null) {
        writeData(fieldName, result.get(fieldName), outputRecord, OutputCol);
      }
    }
  }
}