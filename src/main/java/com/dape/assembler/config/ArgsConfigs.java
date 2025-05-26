package com.dape.assembler.config;

import com.dape.assembler.config.wrapper.ArgsParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class ArgsConfigs {

    private static ArgsConfigs instance;
    private final ArgsParameters parameters;

    public ArgsConfigs(ArgsParameters parameters) {
        this.parameters = parameters;
    }

    public ArgsParameters getParameters() {
        return parameters;
    }

    public static ArgsConfigs buildArgsConfigs(String[] args) throws IOException {
        ArgsParameters argsParameters = new ArgsParameters();

        if (!(args[1].equals("{}")))
            argsParameters = new ObjectMapper().readerFor(ArgsParameters.class).readValue(args[1]);

        instance = new ArgsConfigs(argsParameters);
        return instance;
    }
}
