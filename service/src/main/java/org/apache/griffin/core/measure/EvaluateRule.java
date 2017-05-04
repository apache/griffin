
package org.apache.griffin.core.measure;

import javax.persistence.Entity;

@Entity
public class EvaluateRule  extends AuditableEntity {
    
    private static final long serialVersionUID = -3589222812544556642L;
    public int sampleRatio;
    public String rules;
    
    public EvaluateRule() {
    }

    public EvaluateRule(int sampleRatio, String rules) {
        this.sampleRatio = sampleRatio;
        this.rules = rules;
    }
}
