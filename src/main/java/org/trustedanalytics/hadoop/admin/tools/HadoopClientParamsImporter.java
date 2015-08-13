/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.hadoop.admin.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.trustedanalytics.hadoop.config.ConfigConstants;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public final class HadoopClientParamsImporter {

  private final static Logger LOGGER = LogManager.getLogger(HadoopClientParamsImporter.class);

  private static String CONF_PROPERTY_XPATH ="/configuration/property";

  private HadoopClientParamsImporter() {
  }

  public static void main(String[] args) {
    CLIParameters params = new CLIParameters();
    try {
      if (validateArgs(args, params)) {
        performAction(params);
      }
    } catch (IOException | XPathExpressionException | RuntimeException e) {
      if(params.isVerbose()) {
        LOGGER.error("Ops!", e);
      } else {
        LOGGER.info(e);
      }
      System.exit(1);
    }
  }

  static boolean validateArgs(String[] args, CLIParameters params) {
    JCommander jc = new JCommander(params);
    boolean res = true;
    try {
      jc.parse(args);
      if (params.isHelp()) {
        jc.usage();
        res = false;
      }
    } catch (ParameterException e) {
      LOGGER.info(e);
      jc.usage();
      res = false;
    }
    return res;
  }

  static void performAction(CLIParameters params) throws IOException, XPathExpressionException {
    Optional<InputStream> inputStreamOptional = getSourceInputStream(params);
    if (inputStreamOptional.isPresent()) {
      try (InputStream sourceStream = inputStreamOptional.get()) {
        LOGGER.info(returnJSON(scanConfigZipArchive(sourceStream).get()));
      }
    }
  }

  static Optional<InputStream> getSourceInputStream(CLIParameters params) throws IOException {
    Optional<InputStream> res = Optional.of(System.in);
    if (params.getClientConfigUrl() != null) {
      res = Optional.of(new URL(params.getClientConfigUrl()).openStream());
    }
    return res;
  }

  static Optional<Map<String, String>> scanConfigZipArchive(InputStream source)
      throws IOException, XPathExpressionException {

    InputStream zipInputStream = new ZipInputStream(
        new BufferedInputStream(source));
    ZipEntry zipFileEntry;
    Map<String, String> ret = new HashMap<>();
    while ((zipFileEntry =
                ((ZipInputStream) zipInputStream).getNextEntry()) != null) {
      if (!zipFileEntry.getName().endsWith("-site.xml")) {
        continue;
      }
      byte[] bytes = IOUtils.toByteArray(zipInputStream);
      InputSource is = new InputSource(new ByteArrayInputStream(bytes));
      XPath xPath = XPathFactory.newInstance().newXPath();
      NodeList nodeList =
          (NodeList) xPath.evaluate(CONF_PROPERTY_XPATH, is, XPathConstants.NODESET);

      for (int i = 0; i < nodeList.getLength(); i++) {
        Node propNode = nodeList.item(i);
        String key = (String) xPath.evaluate("name/text()", propNode, XPathConstants.STRING);
        String value = (String) xPath.evaluate("value/text()", propNode, XPathConstants.STRING);
        ret.put(key, value);
      }
    }
    return Optional.of(ret);
  }

  static String returnJSON(Map<String, String> props) {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.createObjectNode();
    props.forEach((k, v) ->
                      ((ObjectNode) rootNode).with(ConfigConstants.HADOOP_CONFIG_KEY_VALUE).put(k, v));
    return rootNode.toString();
  }
}