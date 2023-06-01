package org.apache.atlas.web.rest.validator;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.LineageListRequest;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class LineageListRequestValidator implements RequestValidator<LineageListRequest> {

    private static final int MAX_DEPTH = Integer.MAX_VALUE / 2;

    @Override
    public void validate(LineageListRequest lineageListRequest) throws AtlasBaseException {
        if (StringUtils.isEmpty(lineageListRequest.getGuid()))
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "guid is required");

        validate(lineageListRequest.getSize() == null, "The 'size' parameter is missing or invalid");
        validate(lineageListRequest.getSize() < 0, "Invalid 'size' value, 'size' should be greater than or equal to zero");

        validate(lineageListRequest.getFrom() == null, "The 'from' parameter is missing or invalid");
        validate(lineageListRequest.getFrom() < 0, "Invalid 'from' value, 'from' should be greater than or equal to zero");

        validate(lineageListRequest.getDepth() == null, "The 'depth' parameter is missing or invalid");
        validate(lineageListRequest.getDepth() < 0 || lineageListRequest.getDepth() > MAX_DEPTH, "Invalid 'depth' parameter, constraint not satisfied: 0 <= depth <= 1073741823 (Integer.MAX_VALUE/2)");

        validate(lineageListRequest.getDirection() == null, "Invalid request, mandatory key 'direction' is required");

        // By default exclude extra metadata if flags not provided
        if (lineageListRequest.isExcludeMeanings() == null)
            lineageListRequest.setExcludeMeanings(Boolean.TRUE);
        if (lineageListRequest.isExcludeClassifications() == null)
            lineageListRequest.setExcludeClassifications(Boolean.TRUE);
    }

    private static void validate(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, errorMessage);
    }

}
