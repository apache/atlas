package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.atlas.repository.Constants.*;

/**
 * Validator for user and group based attributes on Atlas entities.
 * Handles validation of owner, admin, and viewer user/group attributes,
 * as well as announcement message validation for security concerns (SSI injection, etc.).
 */
public class UserGroupAttributeValidator {
    private static final Logger LOG = LoggerFactory.getLogger(UserGroupAttributeValidator.class);

    private static final Pattern SSI_TAG_PATTERN = Pattern.compile("<!--#\\s*\\w+.*-->", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final String TYPE_USER = "user";
    private static final String TYPE_GROUP = "group";

    private final UsersStore usersStore;

    public UserGroupAttributeValidator() {
        this(UsersStore.getInstance());
    }

    // Constructor for testing - allows injection of mock UsersStore
    UserGroupAttributeValidator(UsersStore usersStore) {
        this.usersStore = usersStore;
    }

    /**
     * Validates all user and group based attributes on an entity.
     * This includes owner/admin/viewer users and groups, as well as announcement messages.
     *
     * @param entity The entity to validate
     * @throws AtlasBaseException if validation fails
     */
    public void validate(AtlasEntity entity) throws AtlasBaseException {
        validateGroupAttributes(entity);
        validateUserAttributes(entity);
        validateAnnouncementMessage(entity);
    }

    private void validateGroupAttributes(AtlasEntity entity) throws AtlasBaseException {
        RangerUserStore userStore = usersStore.getUserStore();
        Set<String> validGroups = null;

        if (userStore != null) {
            Map<String, Map<String, String>> groupAttrMapping = userStore.getGroupAttrMapping();
            if (groupAttrMapping != null) {
                validGroups = groupAttrMapping.keySet();
            }

            if (validGroups != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Valid group count: {}", validGroups.size());
                }
            } else {
                LOG.warn("Group mapping is null");
            }
        } else {
            LOG.warn("RangerUserStore is null. Cannot validate groups.");
        }

        validateAttribute(entity, ATTR_OWNER_GROUPS, TYPE_GROUP, validGroups);
        validateAttribute(entity, ATTR_ADMIN_GROUPS, TYPE_GROUP, validGroups);
        validateAttribute(entity, ATTR_VIEWER_GROUPS, TYPE_GROUP, validGroups);
    }

    private void validateUserAttributes(AtlasEntity entity) throws AtlasBaseException {
        RangerUserStore userStore = usersStore.getUserStore();
        Set<String> validUsers = null;

        if (userStore != null) {
            Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
            if (userGroupMapping != null) {
                validUsers = userGroupMapping.keySet();
            }

            if (validUsers != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Valid user count: {}", validUsers.size());
                }
            } else {
                LOG.warn("User mapping is null");
            }
        } else {
            LOG.warn("RangerUserStore is null. Cannot validate users.");
        }

        validateAttribute(entity, OWNER_ATTRIBUTE, TYPE_USER, validUsers);
        validateAttribute(entity, ATTR_OWNER_USERS, TYPE_USER, validUsers);
        validateAttribute(entity, ATTR_ADMIN_USERS, TYPE_USER, validUsers);
        validateAttribute(entity, ATTR_VIEWER_USERS, TYPE_USER, validUsers);
    }

    private void validateAnnouncementMessage(AtlasEntity entity) throws AtlasBaseException {
        if (entity.hasAttribute(ATTR_ANNOUNCEMENT_MESSAGE)) {
            Object attributeValue = entity.getAttribute(ATTR_ANNOUNCEMENT_MESSAGE);
            if (attributeValue != null) {
                if (!(attributeValue instanceof String message)) {
                    LOG.warn("Invalid announcementMessage: must be string for asset: {}", getAssetIdentifier(entity));
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid announcementMessage: must be string");
                }
                if (StringUtils.isNotEmpty(message) && SSI_TAG_PATTERN.matcher(message).find()) {
                    LOG.warn("SSI tags detected in announcementMessage for asset: {}", getAssetIdentifier(entity));
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid announcementMessage: SSI tags are not allowed");
                }
            }
        }
    }

    private String getAssetIdentifier(AtlasEntity entity) {
        Object qualifiedName = entity.getAttribute(QUALIFIED_NAME);
        if (qualifiedName != null) {
            return sanitizeForLogging(qualifiedName.toString());
        }
        return entity.getGuid() != null ? entity.getGuid() : "unknown";
    }

    /**
     * Sanitizes a string for safe logging by replacing newline characters.
     * This prevents log forging attacks where an attacker could inject fake log entries.
     */
    private String sanitizeForLogging(String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("[\r\n]", "_");
    }

    private void validateAttribute(AtlasEntity entity, String attributeName, String type, Set<String> validNames) throws AtlasBaseException {
        Object attributeValue = entity.getAttribute(attributeName);
        if (attributeValue == null) {
            return;
        }

        if (attributeValue instanceof Collection<?> values) {
            for (Object itemObj : values) {
                validateAttribute(itemObj, type, validNames);
            }
        } else {
            validateAttribute(attributeValue, type, validNames);
        }
    }

    private boolean validateAttribute(Object attributeValue, String attributeType, Set<String> validNames) throws AtlasBaseException {
        if (!(attributeValue instanceof String)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Invalid " + attributeType + " attribute: must be string or collection of strings");
        }
        String value = ((String) attributeValue);
        if (!isValidAndExists(value, attributeType, validNames)) {
            // For backward compatibility, silently skip invalid/non-existent users/groups
            // instead of blocking the entire request. Security validations (SSI, special chars, URLs)
            // are still enforced in isValidAndExists() and will throw exceptions.
            LOG.warn("Skipping invalid/non-existent {} '{}' for backward compatibility", attributeType, sanitizeForLogging(value));
            return false;
        }
        return true;
    }

    private boolean isValidAndExists(String name, String type, Set<String> validNames) throws AtlasBaseException {
        if (StringUtils.isEmpty(name)) {
            return false;
        }

        // 1. Sanitization (Security) - Fail Fast
        if (SSI_TAG_PATTERN.matcher(name).find()) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: SSI tags are not allowed");
        }
        if (name.contains("<") || name.contains(">")) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: Special characters < > are not allowed");
        }
        if (name.toLowerCase().startsWith("http:") || name.toLowerCase().startsWith("https:")) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: URLs are not allowed");
        }

        // 2. Existence Check (Cleanup)
        // If we have a list of valid names, and the name is NOT in it, return false (filter it out).
        // Also skip validation if validNames is empty - this indicates a failed load from Heracles/auth service,
        // and we should not block requests due to transient API failures.
        if (validNames != null && !validNames.isEmpty() && !validNames.contains(name)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Invalid/Non-existent {} rejected.", type);
            }
            return false;
        }

        if (validNames == null || validNames.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("validNames is null or empty for {}. Skipping existence check.", type);
            }
        }

        return true;
    }
}

