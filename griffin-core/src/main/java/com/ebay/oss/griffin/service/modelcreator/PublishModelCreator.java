package com.ebay.oss.griffin.service.modelcreator;

import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.vo.ModelInput;

@Component("publishModelCreator")
public class PublishModelCreator extends BaseModelCreator {

    @Override
    public boolean isSupport(ModelInput input) {
        return input.getBasic() != null && input.getBasic().getType() == ModelType.PUBLISH;
    }

    @Override
    protected void enhance(DqModel entity, ModelInput input) {
        entity.setStatus(ModelStatus.DEPLOYED);
    }


    @Override
    protected String contentOf(ModelInput input) {
        return input.getExtra().getPublishUrl();
    }

}
