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
    // Set maximum number of reject links to 1
    capabilities.setMaximumRejectLinkCount(1);
    // Set is Wave Generator to false
    capabilities.setIsWaveGenerator(false);
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
      Logger.warning("Mode set to reject but no reject link, records could be silently dropped. Set mode to drop/warn or add reject link");
    }
    Logger.information("parsing query");
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
    Logger.information("query " + expr);

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
    do {
      Logger.information("processing");
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

      try {
        jsonObj = mapper.readTree(json);
      } catch(IOException e1) {
        if (m_rejectLink != null) {
          RejectRecord rejRecord = m_rejectLink.getRejectRecord(inputRecord);
          rejRecord.setErrorText("Invalid json data");
          rejRecord.setErrorCode(1);
          m_rejectLink.writeRecord(rejRecord);
        }
        else {
          Logger.warning("Invalid json data, add a reject link to handle without warnings");
          Logger.debug(json);
        }
        validJson = false;

      }
      if (validJson) {
        try {
          Logger.information("evaluating");
          result = expr.evaluate(jsonObj);
        } catch(EvaluateException e) {
          Logger.warning("error evaluating");
        }
        Logger.information("evaluation done");
        if (result == null) {
          if (mode.equals("warn")) {
            Logger.warning("no match");
          }
          else if (mode.equals("reject") && m_rejectLink != null) {
            RejectRecord rejRecord = m_rejectLink.getRejectRecord(inputRecord);
            rejRecord.setErrorText("No match");
            rejRecord.setErrorCode(2);
            m_rejectLink.writeRecord(rejRecord);
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
                  Logger.warning("error");
                }
              }
              else {
                outputRecord.setValue(outputField, result.get(i).asText());
              }

              m_outputLink.writeRecord(outputRecord);
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
                Logger.warning("error");
              }
            }
            else {
              outputRecord.setValue(outputField, result.asText());
            }
            m_outputLink.writeRecord(outputRecord);
          }

        }
      }
    } while ( true );
  }

  private void writeData(String fieldName, JsonNode data, OutputRecord outputRecord, ColumnMetadata OutputCol) {

    String TargetType = "" + OutputCol.getType();
    int TargetLength = OutputCol.getPrecision();
    String SourceType = "" + data.getNodeType();
    Logger.information(fieldName+":"+SourceType+"->"+TargetType);
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
    else if (TargetType.equals("class java.sql.Timestamp")) {
    	Logger.warning("Timestamps are not yet handled");	
	 }
    else if (TargetType.equals("class java.sql.Date")) {
    	Logger.warning("Dates are not yet handled");	
	}
    else {
      outputRecord.setValue(fieldName, data.asText());
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