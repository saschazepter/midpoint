package com.evolveum.midpoint.smart.impl.mappings;

import com.evolveum.midpoint.xml.ns._public.common.common_3.FocusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ShadowType;

/** Holds preloaded shadow and owner objects to reuse across attributes. */
public record OwnedShadow(ShadowType shadow, FocusType owner) {
}
