package com.cmos.ngmc.service.impl.es;

import java.io.Serializable;

/**
 * @author GavinCook
 * @date 2017-02-21
 * @since 1.0.0
 */
public enum  ESConditionMode implements Serializable{
		MUST("must"), MUST_NOT("must_not"), SHOULD("should"), FILTER("filter");

    private String mode;

    ESConditionMode(String mode) {
        this.mode = mode;
    }

    public String mode() {
        return mode;
    }
}
