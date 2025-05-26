package com.dape.assembler.config.wrapper;

public class ArgsParameters {
    public String investigationClientIdt;

    public String getInvestigationClientIdt() {
        return investigationClientIdt;
    }

    public void setInvestigationClientIdt(String investigationClientIdt) {
        this.investigationClientIdt = investigationClientIdt;
    }

    @Override
    public String toString() {
        return "ArgsParameters{" +
               "investigationClientIdt='" + investigationClientIdt + '\'' +
               '}';
    }
}
