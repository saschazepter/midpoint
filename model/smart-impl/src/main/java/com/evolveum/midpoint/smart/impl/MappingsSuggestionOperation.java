/*
 * Copyright (C) 2010-2025 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */

package com.evolveum.midpoint.smart.impl;

import static com.evolveum.midpoint.smart.api.ServiceClient.Method.SUGGEST_MAPPING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import com.evolveum.midpoint.prism.PrismContext;
import com.evolveum.midpoint.repo.common.expression.ExpressionUtil;

import com.evolveum.midpoint.smart.impl.mappings.OwnedShadow;
import com.evolveum.midpoint.smart.impl.mappings.ValuesPair;
import com.evolveum.midpoint.smart.impl.scoring.MappingsQualityAssessor;
import com.evolveum.midpoint.util.MiscUtil;

import org.jetbrains.annotations.Nullable;

import com.evolveum.midpoint.prism.PrismPropertyDefinition;
import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.repo.common.activity.ActivityInterruptedException;
import com.evolveum.midpoint.repo.common.activity.run.state.CurrentActivityState;
import com.evolveum.midpoint.schema.processor.ResourceObjectTypeIdentification;
import com.evolveum.midpoint.schema.result.OperationResult;
import com.evolveum.midpoint.schema.util.AiUtil;
import com.evolveum.midpoint.smart.api.ServiceClient;
import com.evolveum.midpoint.task.api.Task;
import com.evolveum.midpoint.util.exception.*;
import com.evolveum.midpoint.util.logging.LoggingUtils;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;

/**
 * Implements "suggest mappings" operation.
 */
class MappingsSuggestionOperation {

    private static final Trace LOGGER = TraceManager.getTrace(MappingsSuggestionOperation.class);

    private static final String ID_MAPPINGS_SUGGESTION = "mappingsSuggestion";
    private final TypeOperationContext ctx;
    private final MappingsQualityAssessor qualityAssessor;

    private MappingsSuggestionOperation(TypeOperationContext ctx, MappingsQualityAssessor qualityAssessor) {
        this.ctx = ctx;
        this.qualityAssessor = qualityAssessor;
    }

    static MappingsSuggestionOperation init(
            ServiceClient serviceClient,
            String resourceOid,
            ResourceObjectTypeIdentification typeIdentification,
            @Nullable CurrentActivityState<?> activityState,
            MappingsQualityAssessor qualityAssessor,
            Task task,
            OperationResult result)
            throws SchemaException, ExpressionEvaluationException, SecurityViolationException, CommunicationException,
            ConfigurationException, ObjectNotFoundException {
        return new MappingsSuggestionOperation(
                TypeOperationContext.init(serviceClient, resourceOid, typeIdentification, activityState, task, result),
                qualityAssessor);
    }

    MappingsSuggestionType suggestMappings(
            OperationResult result,
            ShadowObjectClassStatisticsType statistics,
            SchemaMatchResultType schemaMatch,
            OwnedShadowsType ownedShadows)
            throws SchemaException, ObjectNotFoundException, ObjectAlreadyExistsException, ActivityInterruptedException {
        ctx.checkIfCanRun();

        if (schemaMatch.getSchemaMatchResult().isEmpty()) {
            LOGGER.warn("No schema match found for {}. Returning empty suggestion.", this);
            return new MappingsSuggestionType();
        }

        var mappingsSuggestionState = ctx.stateHolderFactory.create(ID_MAPPINGS_SUGGESTION, result);
        mappingsSuggestionState.setExpectedProgress(schemaMatch.getSchemaMatchResult().size());
        try {
            var suggestion = new MappingsSuggestionType();
            Collection<OwnedShadow> fetchedOwnedShadows;
            try {
                fetchedOwnedShadows = ValuesPair.preloadOwnedShadows(
                        ownedShadows,
                        ctx.b.modelService,
                        ctx.getFocusClass(),
                        ctx.task,
                        result);
            } catch (Throwable t) {
                LoggingUtils.logException(LOGGER, "Couldn't preload owned objects; proceeding without examples", t);
                fetchedOwnedShadows = java.util.List.of();
            }
            for (SchemaMatchOneResultType matchPair : schemaMatch.getSchemaMatchResult()) {
                var op = mappingsSuggestionState.recordProcessingStart(matchPair.getShadowAttribute().getName());
                mappingsSuggestionState.flush(result);
                var focusPropPath = PrismContext.get().itemPathParser().asItemPath(matchPair.getFocusPropertyPath());
                var shadowAttrPath = PrismContext.get().itemPathParser().asItemPath(matchPair.getShadowAttributePath());
                try {
                    Collection<ValuesPair> pairs = ValuesPair.buildFromPreloaded(
                            fetchedOwnedShadows,
                            shadowAttrPath,
                            focusPropPath);
                    var mapping = suggestMapping(matchPair, pairs, result);
                    suggestion.getAttributeMappings().add(mapping);
                    mappingsSuggestionState.recordProcessingEnd(op, ItemProcessingOutcomeType.SUCCESS);
                } catch (Throwable t) {
                    // TODO Shouldn't we create an unfinished mapping with just error info?
                    LoggingUtils.logException(LOGGER, "Couldn't suggest mapping for {}", t, matchPair.getShadowAttributePath());
                    mappingsSuggestionState.recordProcessingEnd(op, ItemProcessingOutcomeType.FAILURE);

                    // Normally, the activity framework makes sure that the activity result status is computed properly at the end.
                    // But this is a special case where we must do that ourselves.
                    // FIXME temporarily disabled, as GUI cannot deal with it anyway
                    //mappingsSuggestionState.setResultStatus(OperationResultStatus.PARTIAL_ERROR);
                }
                ctx.checkIfCanRun();
            }
            return suggestion;
        } catch (Throwable t) {
            mappingsSuggestionState.recordException(t);
            throw t;
        } finally {
            mappingsSuggestionState.close(result);
        }
    }

    private AttributeMappingsSuggestionType suggestMapping(
            SchemaMatchOneResultType matchPair,
            Collection<ValuesPair> valuesPairs,
            OperationResult parentResult)
            throws SchemaException {

        LOGGER.trace("Going to suggest mapping for {} -> {} based on {} values pairs",
                matchPair.getShadowAttributePath(), matchPair.getFocusPropertyPath(), valuesPairs.size());

        ItemPath focusPropPath = PrismContext.get().itemPathParser().asItemPath(matchPair.getFocusPropertyPath());
        ItemPath shadowAttrPath = PrismContext.get().itemPathParser().asItemPath(matchPair.getShadowAttributePath());
        var propertyDef = ctx.getFocusTypeDefinition().findPropertyDefinition(focusPropPath);
        ExpressionType expression;
        if (valuesPairs.isEmpty()) {
            LOGGER.trace(" -> no data pairs, so we'll use 'asIs' mapping (without calling LLM)");
            expression = null;
        } else if (valuesPairs.stream().allMatch(pair ->
                (pair.shadowValues() == null || pair.shadowValues().stream().allMatch(Objects::isNull))
                        || (pair.focusValues() == null || pair.focusValues().stream().allMatch(Objects::isNull)))) {
            LOGGER.trace(" -> all shadow or focus values are null, using 'asIs' mapping (without calling LLM)");
            expression = null;
        } else if (doesAsIsSuffice(valuesPairs, propertyDef)) {
            LOGGER.trace(" -> 'asIs' does suffice according to the data, so we'll use it (without calling LLM)");
            expression = null;
        } else if (isTargetDataMissing(valuesPairs)) {
            LOGGER.trace(" -> target data missing; we assume they are probably not there yet, so 'asIs' is fine (no LLM call)");
            expression = null;
        } else {
            LOGGER.trace(" -> going to ask LLM about mapping script");
            var transformationScript = askMicroservice(matchPair, valuesPairs);
            if (transformationScript == null || transformationScript.equals("input")) {
                LOGGER.trace(" -> LLM returned '{}', using 'asIs'", transformationScript);
                expression = null;
            } else {
                LOGGER.trace(" -> LLM returned a script, using it:\n{}", transformationScript);
                expression = new ExpressionType()
                        .expressionEvaluator(
                                new ObjectFactory().createScript(
                                        new ScriptExpressionEvaluatorType().code(transformationScript)));
            }
        }

        // TODO remove this ugly hack
        var serialized = PrismContext.get().itemPathSerializer().serializeStandalone(focusPropPath);
        var hackedSerialized = serialized.replace("ext:", "");
        var hackedReal = PrismContext.get().itemPathParser().asItemPath(hackedSerialized);
        var suggestion = new AttributeMappingsSuggestionType()
                .expectedQuality(this.qualityAssessor.assessMappingsQuality(valuesPairs, expression, this.ctx.task,
                        parentResult))
                .definition(new ResourceAttributeDefinitionType()
                        .ref(shadowAttrPath.rest().toBean()) // FIXME! what about activation, credentials, etc?
                        .inbound(new InboundMappingType()
                                .name(shadowAttrPath.lastName().getLocalPart()
                                        + "-to-" + focusPropPath) //TODO TBD
                                .target(new VariableBindingDefinitionType()
                                        .path(hackedReal.toBean()))
                                .expression(expression)));
        AiUtil.markAsAiProvided(suggestion); // everything is AI-provided now
        return suggestion;
    }

    private String askMicroservice(
            SchemaMatchOneResultType matchPair,
            Collection<ValuesPair> valuesPairs) throws SchemaException {
        var siRequest = new SiSuggestMappingRequestType()
                .applicationAttribute(matchPair.getShadowAttribute())
                .midPointAttribute(matchPair.getFocusProperty())
                .inbound(true);
        valuesPairs.forEach(pair ->
                siRequest.getExample().add(
                        pair.toSiExample(
                                matchPair.getShadowAttribute().getName(),
                                matchPair.getFocusProperty().getName())));
        return ctx.serviceClient
                .invoke(SUGGEST_MAPPING, siRequest, SiSuggestMappingResponseType.class)
                .getTransformationScript();
    }

    /** Returns {@code true} if a simple "asIs" mapping is sufficient. */
    private boolean doesAsIsSuffice(Collection<ValuesPair> valuesPairs, PrismPropertyDefinition<?> propertyDef) {
        for (var valuesPair : valuesPairs) {
            var shadowValues = valuesPair.shadowValues();
            var focusValues = valuesPair.focusValues();
            if (shadowValues.size() != focusValues.size()) {
                return false;
            }
            var expectedFocusValues = new ArrayList<>(focusValues.size());
            for (Object shadowValue : shadowValues) {
                Object converted;
                try {
                    converted = ExpressionUtil.convertValue(
                            propertyDef.getTypeClass(), null, shadowValue, ctx.b.protector);
                } catch (Exception e) {
                    // If the conversion is not possible e.g. because of different types, an exception is thrown
                    // We are OK with that (from performance point of view), because this is just a sample of values.
                    LOGGER.trace("Value conversion failed, assuming transformation is needed: {} (value: {})",
                            e.getMessage(), shadowValue); // no need to provide full stack trace here
                    return false;
                }
                if (converted != null) {
                    expectedFocusValues.add(converted);
                }
            }
            if (!MiscUtil.unorderedCollectionEquals(focusValues, expectedFocusValues)) {
                return false;
            }
        }
        return true;
    }

    /** Returns {@code true} if there are no target data altogether. */
    private boolean isTargetDataMissing(Collection<ValuesPair> valuesPairs) {
        return valuesPairs.stream().allMatch(pair -> pair.focusValues().isEmpty());
    }
}
