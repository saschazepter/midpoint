/*
 * Copyright (c) 2026 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 */

package com.evolveum.midpoint.smart.impl.mappings;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.evolveum.midpoint.prism.PrismContext;
import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ActivationStatusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ActivationType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.LockoutStatusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.UserType;

/**
 * Registry of midPoint focus property paths that are categorical (enum-valued).
 *
 * Each entry maps a focus property path to its known set of allowed string values.
 * Initially focused on activation status attributes but designed to be generic.
 */
@Component
public class CategoricalAttributeRegistry {

    public record CategoricalMpAttribute(
            ItemPath focusPropertyPath,
            List<String> enumValues) {
    }

    private final List<CategoricalMpAttribute> categoricalAttributes;

    public CategoricalAttributeRegistry() {
        this.categoricalAttributes = List.of(
                new CategoricalMpAttribute(
                        ItemPath.create(UserType.F_ACTIVATION, ActivationType.F_ADMINISTRATIVE_STATUS),
                        Arrays.stream(ActivationStatusType.values()).map(ActivationStatusType::value).toList()),
                new CategoricalMpAttribute(
                        ItemPath.create(UserType.F_ACTIVATION, ActivationType.F_LOCKOUT_STATUS),
                        Arrays.stream(LockoutStatusType.values()).map(LockoutStatusType::value).toList())
        );
    }

    /**
     * Returns the categorical attribute definition for the given focus property path, if any.
     */
    public Optional<CategoricalMpAttribute> find(ItemPath focusPropertyPath) {
        var serialized = PrismContext.get().itemPathSerializer().serializeStandalone(focusPropertyPath);
        return categoricalAttributes.stream()
                .filter(cat -> {
                    var catSerialized = PrismContext.get().itemPathSerializer().serializeStandalone(cat.focusPropertyPath());
                    return catSerialized.equals(serialized);
                })
                .findFirst();
    }
}
