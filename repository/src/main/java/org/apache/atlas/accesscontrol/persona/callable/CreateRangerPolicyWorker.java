package org.apache.atlas.accesscontrol.persona.callable;

import org.apache.atlas.accesscontrol.AccessControlUtil;
import org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.atlas.AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE;
import static org.apache.atlas.AtlasConfiguration.RANGER_HEKA_SERVICE_TYPE;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_ACCESS;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_DATA_MASK;

public class CreateRangerPolicyWorker implements Callable<RangerPolicy> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateRangerPolicyWorker.class);

    private PersonaContext context;
    private RangerPolicy provisionalPolicy;
    private AtlasRangerService atlasRangerService;

    public CreateRangerPolicyWorker(PersonaContext context, RangerPolicy provisionalPolicy,
                                    AtlasRangerService atlasRangerService) {
        this.context = context;
        this.provisionalPolicy = provisionalPolicy;
        this.atlasRangerService = atlasRangerService;
    }

    @Override
    public RangerPolicy call() {
        RangerPolicy ret = null;
        LOG.info("Starting CreateRangerPolicyWorker");

        RangerPolicy rangerPolicy = null;
        try {
            //check if there is existing Ranger policy of current provisional Ranger policy
            rangerPolicy = AccessControlUtil.fetchRangerPolicyByResources(atlasRangerService,
                    context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                    context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                    provisionalPolicy);

            if (rangerPolicy == null) {
                ret = atlasRangerService.createRangerPolicy(provisionalPolicy);
            } else {
                if (context.isDataMaskPolicy()) {
                    rangerPolicy.getDataMaskPolicyItems().add(provisionalPolicy.getDataMaskPolicyItems().get(0));
                } else if (context.isAllowPolicy()) {
                    rangerPolicy.getPolicyItems().add(provisionalPolicy.getPolicyItems().get(0));
                } else {
                    rangerPolicy.getDenyPolicyItems().add(provisionalPolicy.getDenyPolicyItems().get(0));
                }

                List<String> labels = rangerPolicy.getPolicyLabels();
                labels.addAll(AtlasPersonaUtil.getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid()));
                rangerPolicy.setPolicyLabels(labels);

                ret = atlasRangerService.updateRangerPolicy(rangerPolicy);
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to create Ranger policies: {}", e.getMessage());
        } finally {
            LOG.info("End CreateRangerPolicyWorker");
        }

        return ret;
    }
}