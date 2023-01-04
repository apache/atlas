package org.apache.atlas.accesscontrol.persona.callable;

import org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_PREFIX_PERSONA;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_TYPE_PERSONA;

public class CleanRoleWorker implements Callable<RangerPolicy> {
    private static final Logger LOG = LoggerFactory.getLogger(CleanRoleWorker.class);

    private RangerPolicy rangerPolicy;
    private AtlasEntity persona;
    private List<String> removePolicyGuids;
    private AtlasRangerService atlasRangerService;

    public CleanRoleWorker(AtlasEntity persona, RangerPolicy rangerPolicy, List<String> removePolicyGuids, AtlasRangerService atlasRangerService) {
        this.rangerPolicy = rangerPolicy;
        this.persona = persona;
        this.removePolicyGuids = removePolicyGuids;
        this.atlasRangerService = atlasRangerService;
    }

    @Override
    public RangerPolicy call() {
        RangerPolicy ret = null;
        LOG.info("Starting CleanRoleWorker");

        try {
            boolean deletePolicy = false;
            String role = AtlasPersonaUtil.getRoleName(persona);

            if (rangerPolicy.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS) {
                deletePolicy = cleanRoleFromAccessPolicy(role, rangerPolicy);
            } else {
                deletePolicy = cleanRoleFromMaskingPolicy(role, rangerPolicy);
            }

            if (deletePolicy) {
                atlasRangerService.deleteRangerPolicy(rangerPolicy);
            } else {
                rangerPolicy.getPolicyLabels().remove(AtlasPersonaUtil.getPersonaLabel(persona.getGuid()));
                rangerPolicy.getPolicyLabels().removeAll(removePolicyGuids);

                long policyLabelCount = rangerPolicy.getPolicyLabels().stream().filter(x -> x.startsWith(LABEL_PREFIX_PERSONA)).count();
                if (policyLabelCount == 0) {
                    rangerPolicy.getPolicyLabels().remove(LABEL_TYPE_PERSONA);
                }

                atlasRangerService.updateRangerPolicy(rangerPolicy);
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to clean Ranger role from Ranger policies: {}", e.getMessage());
        } finally {
            LOG.info("End CleanRoleWorker");
        }

        return ret;
    }

    private boolean cleanRoleFromAccessPolicy(String role, RangerPolicy policy) {
        for (RangerPolicyItem policyItem : new ArrayList<>(policy.getPolicyItems())) {
            if (policyItem.getRoles().remove(role)) {
                if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                    policy.getPolicyItems().remove(policyItem);

                    if (CollectionUtils.isEmpty(policy.getDenyPolicyItems()) && CollectionUtils.isEmpty(policy.getPolicyItems())) {
                        return true;
                    }
                }
            }
        }

        for (RangerPolicyItem policyItem : new ArrayList<>(policy.getDenyPolicyItems())) {
            if (policyItem.getRoles().remove(role)) {
                if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                    policy.getDenyPolicyItems().remove(policyItem);

                    if (CollectionUtils.isEmpty(policy.getDenyPolicyItems()) && CollectionUtils.isEmpty(policy.getPolicyItems())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private boolean cleanRoleFromMaskingPolicy(String role, RangerPolicy policy) {
        List<RangerDataMaskPolicyItem> policyItemsToUpdate = policy.getDataMaskPolicyItems();

        for (RangerDataMaskPolicyItem policyItem : new ArrayList<>(policyItemsToUpdate)) {
            if (policyItem.getRoles().remove(role)) {
                if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                    policyItemsToUpdate.remove(policyItem);

                    if (CollectionUtils.isEmpty(policy.getDataMaskPolicyItems())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}