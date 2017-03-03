package com.ebay.oss.griffin.service.modelcreator;

import java.util.List;

import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.service.DqModelCreator;
import com.ebay.oss.griffin.vo.ModelInput;

@Component("modelCreatorChain")
public class DqModelCreatorChain implements DqModelCreator {
    
    private final List<DqModelCreator> list;
    
    public DqModelCreatorChain(List<DqModelCreator> list) {
        this.list = list;
    }

    @Override
    public DqModel newModel(ModelInput input) {
        for(DqModelCreator each : list) {
            if(each.isSupport(input)) {
                return each.newModel(input);
            }
        }
        throw new RuntimeException("Unsupported ModelInput" + input.getBasic().getType());
    }

    @Override
    public boolean isSupport(ModelInput input) {
        return true;
    }

}
