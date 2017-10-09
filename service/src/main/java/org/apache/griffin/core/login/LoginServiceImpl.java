/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.login;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

@Service
public class LoginServiceImpl implements LoginService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginServiceImpl.class);

    @Autowired
    private Environment env;

    @Override
    public ResponseEntity<Map<String, Object>> login(Map<String, String> map) {
        String strategy = env.getProperty("login.strategy");
        switch (strategy) {
            case "ldap":
                return loginLDAP(map);
            case "default":
                return loginDefault(map);
            default: {
                LOGGER.error("Missing login strategy configuration");
                return new ResponseEntity<Map<String, Object>>(new HashMap<String, Object>(), HttpStatus.NOT_FOUND);
            }
        }
    }

    @Override
    public ResponseEntity<Map<String, Object>> loginDefault(Map<String, String> map) {
        String username = map.get("username");
        String password = map.get("password");
        if (username == null || password == null) {
            LOGGER.error("Missing default login input");
            return null;
        }
        String fullName = null;
        if (username.equals("user")) {
            if (password.equals("test")) {
                fullName = "Default";
            }
        }
        return getResponse(username, fullName);
    }

    @Override
    public ResponseEntity<Map<String, Object>> loginLDAP(Map<String, String> map) {
        String ntAccount = map.get("username");
        String password = map.get("password");
        if (ntAccount == null || password == null) {
            LOGGER.error("Missing ldap login input");
            return null;
        }
        String fullName = searchLDAP(ntAccount, password);
        return getResponse(ntAccount, fullName);
    }

    private String searchLDAP(String ntAccount, String password) {
        String domainComponent = env.getProperty("ldap.dc");
        Hashtable<String, String> ht = getLDAPEnvironmrnt(ntAccount, password);
        if (domainComponent == null || ht == null) {
            return null;
        }
        LdapContext ctx;
        try {
            String searchFilter = "(sAMAccountName=" + ntAccount + ")";
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            ctx = new InitialLdapContext(ht, null);
            NamingEnumeration<SearchResult> results = ctx.search(domainComponent, searchFilter, searchControls);
            String fullName = ntAccount;
            SearchResult searchResult = null;
            while (results.hasMoreElements()) {
                searchResult = results.nextElement();
                Attributes attrs = searchResult.getAttributes();
                if (attrs != null && attrs.get("cn") != null) {
                    String cnName = (String) attrs.get("cn").get();
                    if (cnName.indexOf("(") > 0) {
                        fullName = cnName.substring(0, cnName.indexOf("("));
                    }
                }
            }
            return fullName;
        } catch (NamingException e) {
            LOGGER.info("Failed to login with LDAP auth");
        }
        return null;
    }

    private Hashtable<String, String> getLDAPEnvironmrnt(String ntAccount, String password) {
        String ldapUrl = env.getProperty("ldap.url");
        String domain = env.getProperty("ldap.domain");
        String connectTimeout = env.getProperty("ldap.connect-timeout");
        String readTimeout = env.getProperty("ldap.read-timeout");
        if (ldapUrl == null || domain == null || connectTimeout == null || readTimeout == null) {
            LOGGER.error("Missing ldap properties");
            return null;
        }
        String ldapUser = ntAccount + "@" + domain;
        String ldapFactory = "com.sun.jndi.ldap.LdapCtxFactory";
        Hashtable<String, String> ht = new Hashtable<String, String>();
        ht.put(Context.INITIAL_CONTEXT_FACTORY, ldapFactory);
        ht.put("com.sun.jndi.ldap.connect.timeout", connectTimeout);
        ht.put("com.sun.jndi.ldap.read.timeout", readTimeout);
        ht.put(Context.PROVIDER_URL, ldapUrl);
        ht.put(Context.SECURITY_PRINCIPAL, ldapUser);
        ht.put(Context.SECURITY_CREDENTIALS, password);
        return ht;
    }

    private ResponseEntity<Map<String, Object>> getResponse(String ntAccount, String fullName) {
        Map<String, Object> message = new HashMap<String, Object>();
        if (fullName != null) {
            message.put("ntAccount", ntAccount);
            message.put("fullName", fullName);
            message.put("status", 0);
            return new ResponseEntity<Map<String, Object>>(message, HttpStatus.OK);
        } else {
            return new ResponseEntity<Map<String, Object>>(message, HttpStatus.NOT_FOUND);
        }
    }
}