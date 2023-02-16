package org.apache.griffin.core.master.name;

import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

import java.net.URI;

public class ExecuteNodeNameResloverProvider extends NameResolverProvider {
    @Override
    protected boolean isAvailable() {
        return false;
    }

    @Override
    protected int priority() {
        return 0;
    }

    @Override
    public NameResolver newNameResolver(URI uri, NameResolver.Args args) {
        return null;
    }

    @Override
    public String getDefaultScheme() {
        return null;
    }
}
