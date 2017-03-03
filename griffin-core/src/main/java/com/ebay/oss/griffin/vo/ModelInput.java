/*
 * Copyright (c) 2016 eBay Software Foundation. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.ebay.oss.griffin.vo;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.ebay.oss.griffin.error.ErrorMessage;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelInput extends BaseObj {

    private ModelBasicInputNew basic; // basic info for the model
    private ModelExtraInputNew extra; // extra info for the model
    private List<MappingItemInput> mappings; // mappings for acc

    public ModelInput() {
        basic = new ModelBasicInputNew();
        extra = new ModelExtraInputNew();
        mappings = new ArrayList<MappingItemInput>();
    }

    public ModelInput(ModelBasicInputNew basic, ModelExtraInputNew extra,
                    List<MappingItemInput> mappings) {
        super();
        this.basic = basic;
        this.extra = extra;
        this.mappings = mappings;
    }

    public ModelBasicInputNew getBasic() {
        return basic;
    }

    public void setBasic(ModelBasicInputNew basic) {
        this.basic = basic;
    }

    public ModelExtraInputNew getExtra() {
        return extra;
    }

    public void setExtra(ModelExtraInputNew extra) {
        this.extra = extra;
    }

    public List<MappingItemInput> getMappings() {
        return mappings;
    }

    public void setMappings(List<MappingItemInput> mappings) {
        this.mappings = mappings;
    }

    public void parseFromString(String content)
    {
        String[] contents = content.split("\\|");
        extra.setSrcDb(contents[0]);
        extra.setSrcDataSet(contents[1]);
        extra.setTargetDb(contents[2]);
        extra.setTargetDataSet(contents[3]);


        String[] mappings = contents[4].split(";");
        for (int i = 0; i < mappings.length; i++) {
            String[] mappingcontents = mappings[i].split(",");

            boolean isPk = false;
            if (mappingcontents[2].equals("true"))
                isPk = true;
            System.out.println(mappingcontents[1]);
            String target = mappingcontents[1];
            System.out.println(target);
            String src = mappingcontents[0];
            this.mappings.add(new MappingItemInput(target, src, isPk, mappingcontents[3]));
        }

    }

    @Override
    public ErrorMessage validate() {
        if (this.basic == null) {
            return new ErrorMessage("No basic information provided!");
        } else {
            return this.basic.validate();
        }

    }

}
