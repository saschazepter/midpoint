/*
 * Copyright (c) 2026 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 *
 */

package com.evolveum.midpoint.smart.impl.activities.correlationSuggestion;

import com.evolveum.midpoint.prism.Referencable;
import com.evolveum.midpoint.repo.common.activity.ActivityInterruptedException;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunException;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunInstantiationContext;
import com.evolveum.midpoint.repo.common.activity.run.ActivityRunResult;
import com.evolveum.midpoint.repo.common.activity.run.LocalActivityRun;
import com.evolveum.midpoint.repo.common.activity.run.state.ActivityState;
import com.evolveum.midpoint.schema.result.OperationResult;
import com.evolveum.midpoint.schema.util.ShadowObjectTypeUtil;
import com.evolveum.midpoint.smart.impl.SmartIntegrationBeans;
import com.evolveum.midpoint.smart.impl.activities.Util;
import com.evolveum.midpoint.util.exception.CommonException;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;

import org.jetbrains.annotations.NotNull;

public class CorrelationSuggestionRemoteServiceCallActivityRun extends LocalActivityRun<
        CorrelationSuggestionWorkDefinition,
        CorrelationSuggestionActivityHandler,
        CorrelationSuggestionWorkStateType> {

    private static final Trace LOGGER = TraceManager.getTrace(CorrelationSuggestionRemoteServiceCallActivityRun.class);

    protected CorrelationSuggestionRemoteServiceCallActivityRun(@NotNull ActivityRunInstantiationContext<CorrelationSuggestionWorkDefinition, CorrelationSuggestionActivityHandler> context) {
        super(context);
        setInstanceReady();
    }

    @Override
    protected @NotNull ActivityRunResult runLocally(OperationResult result) throws ActivityRunException, CommonException, ActivityInterruptedException {
        var task = getRunningTask();
        var parentState = Util.getParentState(this, result);
        var resourceOid = getWorkDefinition().getResourceOid();
        var typeDef = getWorkDefinition().getTypeIdentification();
        var targetPathsToIgnore = getWorkDefinition().getTargetPathsToIgnore();

        var schemaMatch = loadSchemaMatch(parentState, result);

        var suggestedCorrelation = SmartIntegrationBeans.get().smartIntegrationService.suggestCorrelation(
                resourceOid, typeDef, schemaMatch, targetPathsToIgnore, null, task, result);

        parentState.setWorkStateItemRealValues(CorrelationSuggestionWorkStateType.F_RESULT, suggestedCorrelation);
        parentState.flushPendingTaskModifications(result);
        LOGGER.debug("Suggestions written to the work state:\n{}", suggestedCorrelation.debugDump(1));

        return ActivityRunResult.success();
    }

    private SchemaMatchResultType loadSchemaMatch(
            ActivityState parentState, OperationResult result) {
        try {
            var schemaMatchRef = parentState.getWorkStateItemRealValueClone(
                    CorrelationSuggestionWorkStateType.F_SCHEMA_MATCH_REF, ObjectReferenceType.class);
            if (schemaMatchRef == null) {
                return null;
            }
            var schemaMatchOid = Referencable.getOid(schemaMatchRef);
            if (schemaMatchOid == null) {
                return null;
            }
            var schemaMatchObject = SmartIntegrationBeans.get().repositoryService
                    .getObject(GenericObjectType.class, schemaMatchOid, null, result)
                    .asObjectable();
            return ShadowObjectTypeUtil.getObjectTypeSchemaMatchRequired(schemaMatchObject);
        } catch (Exception e) {
            LOGGER.warn("Failed to load schema match from work state, proceeding without it: {}", e.getMessage());
            return null;
        }
    }
}
