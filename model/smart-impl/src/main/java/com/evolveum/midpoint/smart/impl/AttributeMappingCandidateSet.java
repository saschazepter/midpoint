/*
 * Copyright (c) 2025 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 */

package com.evolveum.midpoint.smart.impl;

import java.util.*;

import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.schema.util.SmartMetadataUtil;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;
import com.evolveum.prism.xml.ns._public.types_3.ItemPathType;

import org.jetbrains.annotations.Nullable;

/**
 * Manages a collection of attribute mapping candidates, handling duplicate detection
 * and quality-based selection.
 * Multiple suggestions per target are allowed if they differ in source or script.
 * System-provided mappings (heuristics) are preferred over AI when quality is equal.
 */
class AttributeMappingCandidateSet {

    private static final float QUALITY_THRESHOLD = 0.4f;

    private final List<Candidate> candidates = new ArrayList<>();

    /** Target paths that already have mappings or are accepted as suggestions; proposals for these are skipped. */
    private final List<ItemPath> excludedMappingPaths;

    AttributeMappingCandidateSet(Collection<ItemPath> excludedMappingPaths) {
        this.excludedMappingPaths = excludedMappingPaths == null ? List.of() : List.copyOf(excludedMappingPaths);
    }

    /**
     * Proposes a new mapping candidate.
     * Deduplication logic:
     * - Among suggestions: Based on triple (source, target, script) where script is null for AS-IS mappings
     * - Against existing mappings: Based on target path only
     * Filtering logic:
     * - New data scenario (quality = null): Keep ALL mappings
     * - Existing data scenario (quality != null): Keep all above threshold (0.4)
     * When duplicates exist, the better quality candidate is kept.
     * System-provided mappings are preferred over AI when quality is equal.
     */
    void propose(AttributeMappingsSuggestionType suggestion) {
        var identity = MappingIdentity.extract(suggestion);
        if (identity.targetPath() == null) {
            throw new IllegalArgumentException("Target path must not be null for suggestion: " + suggestion);
        }

        // Deduplicate against existing mappings by target path only
        if (excludedMappingPaths.stream().anyMatch(identity.targetPath()::equivalent)) {
            return;
        }

        Float newQuality = suggestion.getExpectedQuality();
        boolean newIsSystemProvided = SmartMetadataUtil.isMarkedAsSystemProvided(suggestion.asPrismContainerValue());

        // Quality filtering: if quality is present, apply threshold
        if (newQuality != null && newQuality <= QUALITY_THRESHOLD) {
            return;
        }

        // Deduplicate among suggestions by (source, target, script) triple
        var iterator = candidates.iterator();
        while (iterator.hasNext()) {
            Candidate existing = iterator.next();

            // Check if it's a duplicate based on (source, target, script)
            if (identity.isDuplicateOf(existing.identity())) {
                Float existingQuality = existing.suggestion().getExpectedQuality();
                boolean existingIsSystemProvided = SmartMetadataUtil.isMarkedAsSystemProvided(
                        existing.suggestion().asPrismContainerValue());

                if (shouldReplaceWith(newQuality, newIsSystemProvided, existingQuality, existingIsSystemProvided)) {
                    iterator.remove();
                    break;
                } else {
                    return;
                }
            }
        }

        candidates.add(new Candidate(identity, suggestion));
    }

    /**
     * Returns an immutable list of the best mapping suggestions.
     * Multiple suggestions per target attribute are allowed if they differ in source or script.
     * Results are sorted by quality (descending), with system-provided preferred over AI when equal.
     */
    List<AttributeMappingsSuggestionType> best() {
        return candidates.stream()
                .sorted(this::compareByQualityAndOrigin)
                .map(Candidate::suggestion)
                .toList();
    }

    private int compareByQualityAndOrigin(Candidate a, Candidate b) {
        Float qualityA = a.suggestion().getExpectedQuality();
        Float qualityB = b.suggestion().getExpectedQuality();
        boolean aIsSystem = SmartMetadataUtil.isMarkedAsSystemProvided(a.suggestion().asPrismContainerValue());
        boolean bIsSystem = SmartMetadataUtil.isMarkedAsSystemProvided(b.suggestion().asPrismContainerValue());

        // Sort by quality descending (nulls last, meaning new data scenarios come last)
        if (qualityA != null && qualityB != null) {
            int qualityCompare = Float.compare(qualityB, qualityA);
            if (qualityCompare != 0) {
                return qualityCompare;
            }
        } else if (qualityA != null) {
            return -1; // a has quality, b doesn't - a comes first
        } else if (qualityB != null) {
            return 1; // b has quality, a doesn't - b comes first
        }

        // If quality is equal (or both null), prefer system-provided
        if (aIsSystem && !bIsSystem) {
            return -1;
        } else if (!aIsSystem && bIsSystem) {
            return 1;
        }

        return 0;
    }

    /**
     * Identity of a mapping used for deduplication.
     * Contains the triple (source, target, script) that uniquely identifies a mapping.
     */
    private record MappingIdentity(ItemPath targetPath, ItemPath sourcePath, @Nullable String script) {

        boolean isDuplicateOf(MappingIdentity other) {
            return this.targetPath.equivalent(other.targetPath) && this.sourcePath.equivalent(other.sourcePath)
                    && (this.script != null && this.script.equals(other.script) || this.script == null && other.script == null);
        }

        static MappingIdentity extract(AttributeMappingsSuggestionType suggestion) {
            var definition = suggestion.getDefinition();
            if (definition == null) {
                throw new IllegalArgumentException("No definition found for suggestion: " + suggestion);
            }

            var inbounds = definition.getInbound();
            if (inbounds != null && !inbounds.isEmpty()) {
                return extractFromInbound(definition, inbounds.get(0));
            }

            var outbound = definition.getOutbound();
            if (outbound != null) {
                return extractFromOutbound(definition, outbound);
            }

            throw new IllegalArgumentException("No inbound or outbound mapping found for suggestion: " + suggestion);
        }

        private static MappingIdentity extractFromInbound(
                ResourceAttributeDefinitionType definition, InboundMappingType inbound) {
            // Target: focus property from inbound target
            ItemPath target = null;
            if (inbound.getTarget() != null && inbound.getTarget().getPath() != null) {
                target = toItemPath(inbound.getTarget().getPath());
            }
            // Source: resource attribute from ref
            ItemPath source = null;
            if (definition.getRef() != null) {
                source = definition.getRef().getItemPath();
            }
            if (target == null || source == null) {
                throw new IllegalArgumentException("No target or source found for inbound mapping: " + inbound);
            }
            String script = extractScriptFromExpression(inbound.getExpression());
            return new MappingIdentity(target, source, script);
        }

        private static MappingIdentity extractFromOutbound(
                ResourceAttributeDefinitionType definition, MappingType outbound) {
            // Target: resource attribute from ref
            ItemPath target = null;
            if (definition.getRef() != null) {
                target = definition.getRef().getItemPath();
            }
            // Source: focus property from outbound source (first if multiple)
            ItemPath source = null;
            var sources = outbound.getSource();
            if (sources != null && !sources.isEmpty()) {
                var firstSource = sources.get(0);
                if (firstSource != null && firstSource.getPath() != null) {
                    source = toItemPath(firstSource.getPath());
                }
            }
            if (target == null || source == null) {
                throw new IllegalArgumentException("No target or source found for outbound mapping: " + outbound);
            }
            // Script: from outbound expression
            String script = extractScriptFromExpression(outbound.getExpression());
            return new MappingIdentity(target, source, script);
        }

        private static @Nullable String extractScriptFromExpression(@Nullable ExpressionType expression) {
            if (expression == null) {
                return null;
            }
            var evaluators = expression.getExpressionEvaluator();
            if (evaluators != null) {
                for (var evaluator : evaluators) {
                    if (evaluator.getValue() instanceof ScriptExpressionEvaluatorType scriptEval) {
                        return scriptEval.getCode();
                    }
                }
            }
            return null;
        }

        private static ItemPath toItemPath(Object path) {
            if (path instanceof ItemPathType itemPath) {
                return itemPath.getItemPath();
            }
            return null;
        }
    }

    private static boolean shouldReplaceWith(
            @Nullable Float newQuality, boolean newIsSystemProvided,
            @Nullable Float existingQuality, boolean existingIsSystemProvided) {
        // If both have quality values, compare them
        if (newQuality != null && existingQuality != null) {
            if (newQuality > existingQuality) {
                return true;
            }
            if (newQuality < existingQuality) {
                return false;
            }
            // Equal quality: prefer system-provided
            return newIsSystemProvided && !existingIsSystemProvided;
        }

        // If only new has quality, prefer new (existing data is more valuable than new data)
        if (newQuality != null && existingQuality == null) {
            return true;
        }

        // If only existing has quality, keep existing
        if (newQuality == null && existingQuality != null) {
            return false;
        }

        // Both null (new data scenario): prefer system-provided
        return newIsSystemProvided && !existingIsSystemProvided;
    }

    /**
     * Internal record holding a mapping candidate with its identity.
     * Deduplication among suggestions uses (source, target, script) triple.
     * Deduplication against existing mappings uses target path only.
     */
    private record Candidate(
            MappingIdentity identity,
            AttributeMappingsSuggestionType suggestion) {
    }
}
