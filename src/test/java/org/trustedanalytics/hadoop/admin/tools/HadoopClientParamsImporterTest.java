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

import org.trustedanalytics.hadoop.config.ConfigConstants;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HadoopClientParamsImporterTest {

  private static String CONFIG_ARCH_PATH = "/hdfs-clientconfig.zip";

  @Test
  public void testGetSourceInputStream_readFromPipeParamSet_returnStdin() throws Exception {
    CLIParameters params = mock(CLIParameters.class);
    when(params.getClientConfigUrl()).thenReturn(null);
    Optional<InputStream> is = HadoopClientParamsImporter.getSourceInputStream(params);
    assertEquals(is.get(), System.in);
  }

  @Test
  public void testGetSourceInputStream_configUrlParamSet_returnInputStream() throws Exception {
    CLIParameters params = mock(CLIParameters.class);
    int TEST_BUFFER_SIZE = 10;
    String urlToConf = getClass().getResource(CONFIG_ARCH_PATH).toURI().toString();
    when(params.getClientConfigUrl()).thenReturn(urlToConf);

    Optional<InputStream> is = HadoopClientParamsImporter.getSourceInputStream(params);
    byte[] buffer = new byte[TEST_BUFFER_SIZE];
    try (InputStream in = is.get()) {
      int res = in.read(buffer);
      String expected = "[80, 75, 3, 4, 10, 0, 0, 0, 0, 0]";

      assertEquals(Arrays.toString(buffer), expected);
      assertEquals(res, TEST_BUFFER_SIZE);
    }
  }

  @Test
  public void testReturnJSON_propertiesMap_returnJSONString() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "val1");
    properties.put("prop2", "val2");

    String actual = HadoopClientParamsImporter.returnJSON(properties);
    String expected = "{\"" + ConfigConstants.HADOOP_CONFIG_KEY_VALUE
                      + "\":{\"prop2\":\"val2\",\"prop1\":\"val1\"}}";

    assertEquals(actual, expected);
  }

  @Test
  public void testScanConfigZipArchive_openedInputStream_returnConfigPropsMap() throws Exception {
    try (InputStream is = getClass().getResourceAsStream(CONFIG_ARCH_PATH)) {
      Optional<Map<String, String>> optParams = HadoopClientParamsImporter.scanConfigZipArchive(is);
      Map<String, String> expected = new HashMap<String, String>() {
        {
          put("fs.defaultFS","hdfs://nameservice1");
          put("hadoop.security.authentication", "simple");
          put("hadoop.rpc.protection", "authentication");
          put("hadoop.security.authorization", "false");
          put("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
          put("dfs.nameservices", "nameservice1");
        }
      };

      assertTrue(optParams.isPresent());
      assertEquals(optParams.get(), expected);
    }
  }

  @Test
  public void testValidateArgs_usageDisplaySet_returnFalse() throws Exception {
    CLIParameters params = new CLIParameters();
    String[] args = {"-h"};
    assertFalse(HadoopClientParamsImporter.validateArgs(args, params));
  }

  @Test
  public void testValidateArgs_incorrectConfigUrlSet_returnFalse() throws Exception {
    CLIParameters params = new CLIParameters();
    String[] args = {"-cu", "sldasd"};
    assertFalse(HadoopClientParamsImporter.validateArgs(args, params));
  }

  @Test
  public void testValidateArgs_correctConfigUrlSet_returnFalse() throws Exception {
    CLIParameters params = new CLIParameters();
    String[] args = {"-cu", "http://localhost/config-zip"};
    assertTrue(HadoopClientParamsImporter.validateArgs(args, params));
  }

  @Test
  public void testValidateArgs_noArgs_returnTrue() throws Exception {
    CLIParameters params = new CLIParameters();
    String[] args = {"-cu", "http://localhost/config-zip"};
    assertTrue(HadoopClientParamsImporter.validateArgs(args, params));
  }
}