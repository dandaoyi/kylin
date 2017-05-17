/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.kylin.job.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * Created by danda on 2017/5/16.
 */
public class HttpUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static final HttpComponentsClientHttpRequestFactory httpComponentsClientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory();

    private static final String plainCreds = "zabbix:H*ymfQwG";
    private static HttpHeaders headers = new HttpHeaders();

    static {
        httpComponentsClientHttpRequestFactory.setConnectTimeout(80000);
        httpComponentsClientHttpRequestFactory.setReadTimeout(60000);

        byte[] plainCredsBytes = plainCreds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes);

        headers.add("Authorization", "Basic " + base64Creds);
        headers.setContentType(MediaType.APPLICATION_JSON);
    }
    private static final RestTemplate restTemplate = new RestTemplate(httpComponentsClientHttpRequestFactory);

    private HttpUtil() {

    }

    public static void sendHttpPostToJira(String url, String summary, String description) throws Exception {
        try {

            String postData = "{\"fields\":{\"project\":{\"id\":\"10203\"},\"issuetype\":{\"id\":\"1\"},\"priority\":{\"id\":\"1\"},\"versions\":[{\"id\":\"13400\"}],\"components\":[{\"id\":\"14700\"}],\"summary\":\"[Kylin build error]" + summary + "\",\"description\":\"" + StringEscapeUtils.escapeHtml(description) + "\"}}";

            HttpEntity<String> request = new HttpEntity<String>(postData, headers);

            String json = restTemplate.postForObject(url, request, String.class);

            if (!(json.contains("id") && json.contains("key"))) {
                LOGGER.warn("create Jira result is {} ", json);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("send http post url:%s failure! %s", url, e.getMessage()));
        }
    }

}
