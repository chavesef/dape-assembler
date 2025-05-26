package com.dape.assembler.enums;

public enum TicketTypeEnum {
    SIMPLE(1, "Bilhete simples"),
    MULTIPLE(2, "Bilhete múltiplo"),;

    private final int codTicketType;
    private final String desTicketType;

    TicketTypeEnum(int codTicketType, String desTicketType) {
        this.codTicketType = codTicketType;
        this.desTicketType = desTicketType;
    }

    public int getCodTicketType() {
        return codTicketType;
    }

    public String getDesTicketType() {
        return desTicketType;
    }
}
