package org.apache.atlas.accesscontrol.persona.callable;

import org.apache.atlas.accesscontrol.AccessControlUtil;
import org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.atlas.AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE;
import static org.apache.atlas.AtlasConfiguration.RANGER_HEKA_SERVICE_TYPE;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_ACCESS;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_DATA_MASK;

public class UpdateRangerPolicyWorker implements Callable<RangerPolicy> {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateRangerPolicyWorker.class);

    private PersonaContext context;
    private RangerPolicy rangerPolicy;
    private RangerPolicy provisionalPolicy;
    private AtlasRangerService atlasRangerService;

    public UpdateRangerPolicyWorker(PersonaContext context, RangerPolicy rangerPolicy,
                                    RangerPolicy provisionalPolicy, AtlasRangerService atlasRangerService) {
        this.context = context;
        this.rangerPolicy = rangerPolicy;
        this.provisionalPolicy = provisionalPolicy;
        this.atlasRangerService = atlasRangerService;
    }

    @Override
    public RangerPolicy call() {
        RangerPolicy ret = null;
        LOG.info("Starting UpdateRangerPolicyWorker");

        try {
            boolean addNewPolicyItem = false;

            if (rangerPolicy == null) {
                RangerPolicy policy = AccessControlUtil.fetchRangerPolicyByResources(atlasRangerService,
                        context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                        context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                        provisionalPolicy);

                if (policy == null) {
                    atlasRangerService.createRangerPolicy(provisionalPolicy);
                } else {
                    rangerPolicy = policy;
                    addNewPolicyItem = true;
                }
            }

            if (rangerPolicy != null) {
                boolean update = false;

                if (context.isDataMaskPolicy()) {
                    List<RangerDataMaskPolicyItem> existingRangerPolicyItems = rangerPolicy.getDataMaskPolicyItems();
                    List<RangerDataMaskPolicyItem> provisionalPolicyItems = provisionalPolicy.getDataMaskPolicyItems();

                    update = updateMaskPolicyItem(context,
                            existingRangerPolicyItems,
                            provisionalPolicyItems,
                            addNewPolicyItem);

                } else {

                    List<RangerPolicyItem> existingRangerPolicyItems = context.isAllowPolicy() ?
                            rangerPolicy.getPolicyItems() :
                            rangerPolicy.getDenyPolicyItems();

                    List<RangerPolicyItem> provisionalPolicyItems = context.isAllowPolicy() ?
                            provisionalPolicy.getPolicyItems() :
                            provisionalPolicy.getDenyPolicyItems();

                    update = updatePolicyItem(context,
                            existingRangerPolicyItems,
                            provisionalPolicyItems,
                            addNewPolicyItem);
                }

                if (update) {
                    List<String> labels = rangerPolicy.getPolicyLabels();
                    labels.add(AtlasPersonaUtil.getPersonaLabel(context.getPersona().getGuid()));
                    labels.add(AtlasPersonaUtil.getPersonaPolicyLabel(context.getPersonaPolicy().getGuid()));
                    rangerPolicy.setPolicyLabels(labels);

                    atlasRangerService.updateRangerPolicy(rangerPolicy);
                }
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to update Ranger policies: {}", e.getMessage());
        } finally {
            LOG.info("End UpdateRangerPolicyWorker");
        }

        return ret;
    }

    private boolean updatePolicyItem(PersonaContext context,
                                     List<RangerPolicyItem> existingRangerPolicyItems,
                                     List<RangerPolicyItem> provisionalPolicyItems,
                                     boolean addNewPolicyItem) {


        if (addNewPolicyItem || CollectionUtils.isEmpty(existingRangerPolicyItems)) {
            //no condition present at all
            //add new condition & update existing Ranger policy
            existingRangerPolicyItems.add(provisionalPolicyItems.get(0));

        } else {
            String role = AccessControlUtil.getQualifiedName(context.getPersona());

            List<RangerPolicyItem> temp = new ArrayList<>(existingRangerPolicyItems);

            for (int i = 0; i < temp.size(); i++) {
                RangerPolicyItem policyItem = temp.get(i);

                if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                    List<RangerPolicyItemAccess> newAccesses = provisionalPolicyItems.get(0).getAccesses();

                    if (CollectionUtils.isEqualCollection(policyItem.getAccesses(), newAccesses)) {
                        //accesses are equal, do not update
                        return false;
                    }

                    if (CollectionUtils.isNotEmpty(policyItem.getGroups()) || CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                        //contaminated policyItem,
                        // remove role from policy Item
                        // Add another policy item specific for persona role
                        existingRangerPolicyItems.get(i).getRoles().remove(role);
                        existingRangerPolicyItems.add(provisionalPolicyItems.get(0));
                        continue;
                    }

                    existingRangerPolicyItems.get(i).setAccesses(provisionalPolicyItems.get(0).getAccesses());
                }
            }
        }

        return true;
    }

    private boolean updateMaskPolicyItem(PersonaContext context,
                                         List<RangerDataMaskPolicyItem> existingRangerPolicyItems,
                                         List<RangerDataMaskPolicyItem> provisionalPolicyItems,
                                         boolean addNewPolicyItem) {

        if (addNewPolicyItem || CollectionUtils.isEmpty(existingRangerPolicyItems)) {
            //no condition present at all
            //add new condition & update existing Ranger policy
            existingRangerPolicyItems.add(provisionalPolicyItems.get(0));

        } else {
            String role = AccessControlUtil.getQualifiedName(context.getPersona());

            List<RangerDataMaskPolicyItem> temp = new ArrayList<>(existingRangerPolicyItems);

            for (int i = 0; i < temp.size(); i++) {
                RangerDataMaskPolicyItem policyItem = temp.get(i);

                if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                    List<RangerPolicyItemAccess> newAccesses = provisionalPolicyItems.get(0).getAccesses();
                    RangerPolicyItemDataMaskInfo newMaskInfo = provisionalPolicyItems.get(0).getDataMaskInfo();

                    if (CollectionUtils.isEqualCollection(policyItem.getAccesses(), newAccesses) &&
                            policyItem.getDataMaskInfo().equals(newMaskInfo)) {
                        //accesses & mask info are equal, do not update
                        return false;
                    }

                    if (CollectionUtils.isNotEmpty(policyItem.getGroups()) || CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                        //contaminated policyItem,
                        // remove role from policy Item
                        // Add another policy item specific for persona role
                        existingRangerPolicyItems.get(i).getRoles().remove(role);
                        existingRangerPolicyItems.add(provisionalPolicyItems.get(0));
                        continue;
                    }

                    existingRangerPolicyItems.get(i).setAccesses(provisionalPolicyItems.get(0).getAccesses());
                    existingRangerPolicyItems.get(i).setDataMaskInfo(provisionalPolicyItems.get(0).getDataMaskInfo());
                }
            }
        }

        return true;
    }
}