package org.apache.griffin.core.schedule;

/**
 * Created by xiangrchen on 6/1/17.
 */
public class JobHealth {
    private int health;
    private int invalid;
    private int jobCount;

    public int getHealth() {
        return health;
    }

    public void setHealth(int health) {
        this.health = health;
    }

    public int getInvalid() {
        return invalid;
    }

    public void setInvalid(int invalid) {
        this.invalid = invalid;
    }

    public int getJobCount() {
        return jobCount;
    }

    public void setJobCount(int jobCount) {
        this.jobCount = jobCount;
    }

    public JobHealth(int health, int invalid, int jobCount) {
        this.health = health;
        this.invalid = invalid;
        this.jobCount = jobCount;
    }
}
