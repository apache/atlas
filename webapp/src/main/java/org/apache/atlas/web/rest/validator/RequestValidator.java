package org.apache.atlas.web.rest.validator;

import org.apache.atlas.exception.AtlasBaseException;

public interface RequestValidator<T> {

    void validate(T t) throws AtlasBaseException;

}