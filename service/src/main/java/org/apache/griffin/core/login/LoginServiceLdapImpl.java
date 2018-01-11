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

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.util.Hashtable;

public class LoginServiceLdapImpl implements LoginService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginServiceLdapImpl.class);

    private static final String LDAP_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private String url;
    private String email;
    private String searchBase;
    private String searchPattern;
    private SearchControls searchControls;

    public LoginServiceLdapImpl(String url, String email, String searchBase, String searchPattern) {
        this.url = url;
        this.email = email;
        this.searchBase = searchBase;
        this.searchPattern = searchPattern;
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        this.searchControls = searchControls;
    }

    @Override
    public String login(String ntAccount, String password) {
        String searchFilter = searchPattern.replace("{0}", ntAccount);
        try {
            LdapContext ctx = getContextInstance(ntAccount, password);
            NamingEnumeration<SearchResult> results = ctx.search(searchBase, searchFilter, searchControls);
            return getFullName(results, ntAccount);
        } catch (NamingException e) {
            LOGGER.warn("User {} failed to login with LDAP auth. {}", ntAccount, e.getMessage());
        }
        return null;
    }

    private String getFullName(NamingEnumeration<SearchResult> results, String ntAccount) {
        String fullName = ntAccount;
        try {
            while (results.hasMoreElements()) {
                SearchResult searchResult = results.nextElement();
                Attributes attrs = searchResult.getAttributes();
                if (attrs != null && attrs.get("cn") != null) {
                    String cnName = (String) attrs.get("cn").get();
                    if (cnName.indexOf("(") > 0) {
                        fullName = cnName.substring(0, cnName.indexOf("("));
                    }
                }
            }
        } catch (NamingException e) {
            LOGGER.warn("User {} successfully login with LDAP auth, but failed to get full name.", ntAccount);
        }
        return fullName;
    }

    private LdapContext getContextInstance(String ntAccount, String password) throws NamingException {
        Hashtable<String, String> ht = new Hashtable<>();
        ht.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_FACTORY);
        ht.put(Context.PROVIDER_URL, url);
        ht.put(Context.SECURITY_PRINCIPAL, ntAccount + email);
        ht.put(Context.SECURITY_CREDENTIALS, password);
        return new InitialLdapContext(ht, null);
    }
}
