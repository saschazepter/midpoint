package com.evolveum.midpoint.smart.impl.mappings;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;

import com.evolveum.midpoint.util.exception.*;

import org.jetbrains.annotations.NotNull;

import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.model.api.ModelService;
import com.evolveum.midpoint.xml.ns._public.common.common_3.FocusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ObjectType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ShadowType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;
import com.evolveum.midpoint.task.api.Task;
import com.evolveum.midpoint.schema.result.OperationResult;

public record ValuesPair(Collection<?> shadowValues, Collection<?> focusValues) {
    public SiSuggestMappingExampleType toSiExample(
            String applicationAttrDescriptivePath, String midPointPropertyDescriptivePath) {
        return new SiSuggestMappingExampleType()
                .application(toSiAttributeExample(applicationAttrDescriptivePath, shadowValues))
                .midPoint(toSiAttributeExample(midPointPropertyDescriptivePath, focusValues));
    }

    private @NotNull SiAttributeExampleType toSiAttributeExample(String path, Collection<?> values) {
        var example = new SiAttributeExampleType().name(path);
        example.getValue().addAll(stringify(values));
        return example;
    }

    private Collection<String> stringify(Collection<?> values) {
        return values.stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .toList();
    }

    /**
     * Preloads shadow and owner objects for given references to avoid repeated fetches per attribute.
     */
    public static <F extends FocusType> Collection<OwnedShadow> preloadOwnedShadows(
            OwnedShadowsType ownedShadows,
            ModelService modelService,
            Class<F> focusClass,
            Task task,
            OperationResult result) throws SchemaException, ExpressionEvaluationException, SecurityViolationException,
            CommunicationException, ConfigurationException, ObjectNotFoundException {
        if (ownedShadows == null) {
            return List.of();
        }
        var ownedShadowRefs = ownedShadows.getOwnedShadow();
        if (ownedShadowRefs == null || ownedShadowRefs.isEmpty()) {
            return List.of();
        }
        var loaded = new ArrayList<OwnedShadow>(ownedShadowRefs.size());
        for (var ref : ownedShadowRefs) {
            var shadowOid = ref.getShadowRef() != null ? ref.getShadowRef().getOid() : null;
            var ownerOid = ref.getOwnerRef() != null ? ref.getOwnerRef().getOid() : null;
            if (shadowOid == null || ownerOid == null) {
                continue;
            }
            var shadow = modelService.getObject(ShadowType.class, shadowOid, null, task, result).asObjectable();
            var owner = modelService.getObject(focusClass, ownerOid, null, task, result).asObjectable();
            loaded.add(new OwnedShadow(shadow, owner));
        }
        return loaded;
    }

    /** Builds {@link ValuesPair} list from preloaded objects for the provided paths. */
    public static Collection<ValuesPair> buildFromPreloaded(
            Collection<OwnedShadow> loaded,
            ItemPath shadowAttrPath,
            ItemPath focusPropPath) {
        if (loaded == null || loaded.isEmpty()) {
            return List.of();
        }
        var pairs = new ArrayList<ValuesPair>(loaded.size());
        for (var lo : loaded) {
            pairs.add(new ValuesPair(
                    getItemRealValues(lo.shadow(), shadowAttrPath),
                    getItemRealValues(lo.owner(), focusPropPath)));
        }
        return pairs;
    }

    /** Returns real values for the item at the given path or empty list if missing. */
    public static Collection<?> getItemRealValues(ObjectType objectable, ItemPath itemPath) {
        var item = objectable.asPrismObject().findItem(itemPath);
        return item != null ? item.getRealValues() : List.of();
    }
}
