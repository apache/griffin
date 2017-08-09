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


package org.apache.griffin.core.measure.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;

@Entity
public class EvaluateRule  extends AuditableEntity {
    
    private static final long serialVersionUID = -3589222812544556642L;

    public int getSampleRatio() {
        return sampleRatio;
    }

    public void setSampleRatio(int sampleRatio) {
        this.sampleRatio = sampleRatio;
    }

    public String getRules() {
        return rules;
    }

    public void setRules(String rules) {
        this.rules = rules;
    }

    private int sampleRatio;

    @Lob
    @Column(length=1048576) //2^20=1048576
    private String rules;
    
    public EvaluateRule() {
    }

    public EvaluateRule(int sampleRatio, String rules) {
        this.sampleRatio = sampleRatio;
        this.rules = rules;
    }
}
