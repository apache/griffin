package com.ebay.oss.griffin.service.modelcreator;

import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.ValidityType;
import com.ebay.oss.griffin.vo.ModelInput;

@Component("validityModelCreator")
public class ValidityModelCreator extends BaseModelCreator {

    @Override
    public boolean isSupport(ModelInput input) {
			return input.getBasic() != null && input.getBasic().getType() == ModelType.VALIDITY;
    }

    @Override
    protected void enhance(DqModel entity, ModelInput input) {
        if(input.getExtra().getVaType() == ValidityType.TOTAL_COUNT){
            entity.setStatus(input.getBasic().getStatus());
        }else{
            entity.setStatus(ModelStatus.TESTING);
        }

        newSampleJob4Model(entity);
    }

    protected String contentOf(ModelInput input) {
        return input.getExtra().getSrcDb() + "|"
                        + input.getExtra().getSrcDataSet() + "|"
                        + input.getExtra().getVaType() + "|"
                        + input.getExtra().getColumn();
    }
}
