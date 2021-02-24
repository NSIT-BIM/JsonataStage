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
  String query;
  private Boolean serialize;
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

    propList.add(new PropertyDefinition("jsondata", "", "jsondata", "jsondata", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("query", "$", "query", "query", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("output", "", "output field", "output field", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("serialize", "false", "serialize", "serialize", PropertyDefinition.Scope.STAGE));
    propList.add(new PropertyDefinition("expand", "false", "expand", "expand", PropertyDefinition.Scope.STAGE));
    return propList;
  }

  public boolean validateConfiguration(Configuration configuration, boolean isRuntime) throws Exception {
    m_inputLink = configuration.getInputLink(0);
    m_outputLink = configuration.getOutputLink(0);
    m_rejectLink = m_inputLink.getAssociatedRejectLink();
    Properties userStageProperties = configuration.getUserProperties();
    jsondata = userStageProperties.getProperty("jsondata");
    outputField = userStageProperties.getProperty("output");
    query = userStageProperties.getProperty("query");
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
    Logger.information("parsing query");
    expr = null;
    try {
      expr = Expressions.parse(query);
    } catch(ParseException e) {
      Logger.warning("error parsing");
    } catch(EvaluateRuntimeException ere) {
      Logger.warning("error parsing");
    } catch(JsonProcessingException e) {
      Logger.warning("error parsing");
    } catch(IOException e) {
      Logger.warning("error parsing");
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

      try {
        Logger.information("evaluating");
        result = expr.evaluate(jsonObj);
      } catch(EvaluateException e) {
        Logger.warning("error evaluating");
      }
      Logger.information("evaluation done");
      if (result == null) {
        Logger.warning("no match");

      }
      else if (validJson) {

        if (expand && result.isArray()) {
          for (int i = 0; i < result.size(); i++) {
            OutputRecord outputRecord = m_outputLink.getOutputRecord();
            if (serialize && result.get(i).isObject()) {
              Iterator < String > fieldNames = outColsNames.iterator();
              while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();

                if (result.get(i).get(fieldName) != null) {

                  if (result.get(i).get(fieldName).isObject()) {
                    try {
                      outputRecord.setValue(fieldName, mapper.writeValueAsString(result.get(i).get(fieldName)));
                    }
                    catch(JsonProcessingException e) {
                      Logger.warning("error");
                    }
                  }
                  else {
                    outputRecord.setValue(fieldName, result.get(i).get(fieldName).asText());
                  }

                }
                else {
                  int pos = outColsNames.indexOf(fieldName);
                  String derivation = outCols.get(pos).getDerivation();
                  if (derivation != null) {
                    outputRecord.setValue(fieldName, result.get(i).at(derivation).asText());
                  }
                }
              }
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
            Iterator < String > fieldNames = outColsNames.iterator();
            while (fieldNames.hasNext()) {

              String fieldName = fieldNames.next();
              if (result.get(fieldName) != null) {
                try {
                  outputRecord.setValue(fieldName, mapper.writeValueAsString(result.get(fieldName)));
                }
                catch(JsonProcessingException e) {
                  Logger.warning("error");
                }
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
    } while ( true );
  }

}