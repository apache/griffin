package org.apache.griffin.core.service;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class GriffinController {

    @RequestMapping("/version")
    public String greeting() {
        return "0.1.0";
    }

}

