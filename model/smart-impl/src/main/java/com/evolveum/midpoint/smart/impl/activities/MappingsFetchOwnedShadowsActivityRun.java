package com.evolveum.midpoint.smart.impl.activities;

import com.evolveum.midpoint.repo.common.activity.ActivityInterruptedException;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunException;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunInstantiationContext;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunResult;
import com.evolveum.midpoint.repo.common.activity.run.LocalActivityRun;
import com.evolveum.midpoint.schema.result.OperationResult;
import com.evolveum.midpoint.smart.impl.SmartIntegrationBeans;
import com.evolveum.midpoint.util.exception.*;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;

import org.jetbrains.annotations.NotNull;


public class MappingsFetchOwnedShadowsActivityRun extends LocalActivityRun<
        MappingsSuggestionWorkDefinition,
        MappingsSuggestionActivityHandler,
        MappingsSuggestionWorkStateType> {

    private static final Trace LOGGER = TraceManager.getTrace(MappingsFetchOwnedShadowsActivityRun.class);
    private static final int ATTRIBUTE_MAPPING_EXAMPLES = 20;


    public MappingsFetchOwnedShadowsActivityRun(
            @NotNull ActivityRunInstantiationContext<MappingsSuggestionWorkDefinition, MappingsSuggestionActivityHandler> context) {
        super(context);
        setInstanceReady();
    }

    @Override
    protected @NotNull ActivityRunResult runLocally(OperationResult result)
            throws ActivityRunException, CommonException, ActivityInterruptedException {
        var parentState = Util.getParentState(this, result);

        var workDef = getWorkDefinition();
        LOGGER.debug("Fetching up to {} owned shadow refs for resource {} and type {}",
                ATTRIBUTE_MAPPING_EXAMPLES, workDef.getResourceOid(), workDef.getTypeIdentification());
        OwnedShadowsType ownedShadows = SmartIntegrationBeans.get().smartIntegrationService.sampleOwnedShadows(
                workDef.getResourceOid(),
                workDef.getTypeIdentification(),
                ATTRIBUTE_MAPPING_EXAMPLES,
                getRunningTask(),
                result);
        LOGGER.debug("Fetched {} owned shadow refs for resource {} and type {}",
                ownedShadows != null && ownedShadows.getOwnedShadow() != null ? ownedShadows.getOwnedShadow().size() : 0,
                workDef.getResourceOid(), workDef.getTypeIdentification());

        parentState.setWorkStateItemRealValues(
                MappingsSuggestionWorkStateType.F_OWNED_SHADOWS,
                ownedShadows);
        parentState.flushPendingTaskModificationsChecked(result);

        return ActivityRunResult.success();
    }
}
