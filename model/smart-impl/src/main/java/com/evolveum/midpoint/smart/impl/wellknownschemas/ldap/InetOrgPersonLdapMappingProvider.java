/*
 * Copyright (c) 2025 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 *
 */

package com.evolveum.midpoint.smart.impl.wellknownschemas.ldap;

import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.smart.impl.mappings.OwnedShadow;
import com.evolveum.midpoint.smart.impl.wellknownschemas.SystemMappingSuggestion;
import com.evolveum.midpoint.smart.impl.wellknownschemas.WellKnownSchemaProvider;
import com.evolveum.midpoint.smart.impl.wellknownschemas.WellKnownSchemaType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;

import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class InetOrgPersonLdapMappingProvider implements WellKnownSchemaProvider {

    @Override
    public WellKnownSchemaType getSupportedSchemaType() {
        return WellKnownSchemaType.LDAP_INETORGPERSON;
    }

    @Override
    public Map<ItemPath, ItemPath> suggestSchemaMatches() {
        Map<ItemPath, ItemPath> matches = new HashMap<>();
        matches.put(ItemPath.create("uid"), UserType.F_NAME);
        matches.put(ItemPath.create("cn"), UserType.F_FULL_NAME);
        matches.put(ItemPath.create("givenName"), UserType.F_GIVEN_NAME);
        matches.put(ItemPath.create("sn"), UserType.F_FAMILY_NAME);
        matches.put(ItemPath.create("mail"), UserType.F_EMAIL_ADDRESS);
        matches.put(ItemPath.create("telephoneNumber"), UserType.F_TELEPHONE_NUMBER);
        return matches;
    }

    @Override
    public List<SystemMappingSuggestion> suggestInboundMappings() {
        List<SystemMappingSuggestion> mappings = new ArrayList<>();
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("uid", UserType.F_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("cn", UserType.F_FULL_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("givenName", UserType.F_GIVEN_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("sn", UserType.F_FAMILY_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("mail", UserType.F_EMAIL_ADDRESS));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("telephoneNumber", UserType.F_TELEPHONE_NUMBER));
        return mappings;
    }

    @Override
    public List<SystemMappingSuggestion> suggestOutboundMappings(@Nullable List<OwnedShadow> sampleShadows) {
        List<SystemMappingSuggestion> mappings = new ArrayList<>();
        var dnMapping = createDnMapping(sampleShadows);
        if (dnMapping != null) {
            mappings.add(dnMapping);
        }
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("uid", UserType.F_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("cn", UserType.F_FULL_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("givenName", UserType.F_GIVEN_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("sn", UserType.F_FAMILY_NAME));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("mail", UserType.F_EMAIL_ADDRESS));
        mappings.add(SystemMappingSuggestion.createAsIsSuggestion("telephoneNumber", UserType.F_TELEPHONE_NUMBER));
        return mappings;
    }

    private SystemMappingSuggestion createDnMapping(@Nullable List<OwnedShadow> sampleShadows) {
        String ouSuffix = extractOuSuffixFromSamples(sampleShadows);
        if (ouSuffix == null) {
            return null;
        }
        String script = "basic.composeDnWithSuffix('uid', name, '" + ouSuffix + "')";
        return SystemMappingSuggestion.createScriptSuggestion(
                "dn",
                UserType.F_NAME,
                script,
                "Compose DN: uid=<name>," + ouSuffix);
    }

    private String extractOuSuffixFromSamples(List<OwnedShadow> sampleShadows) {
        if (sampleShadows == null || sampleShadows.isEmpty()) {
            return null;
        }

        for (OwnedShadow ownedShadow : sampleShadows) {
            try {
                ShadowType shadow = ownedShadow.shadow();
                if (shadow == null) {
                    continue;
                }

                var attributes = shadow.getAttributes();
                if (attributes == null) {
                    continue;
                }

                var dnItem = attributes.asPrismContainerValue().findItem(ItemPath.create("dn"));
                if (dnItem == null || dnItem.getRealValues().isEmpty()) {
                    continue;
                }

                String dnValue = String.valueOf(dnItem.getRealValues().iterator().next());
                if (dnValue == null || dnValue.isEmpty()) {
                    continue;
                }

                LdapName ldapName = new LdapName(dnValue);
                if (ldapName.size() < 2) {
                    continue;
                }

                for (int i = ldapName.size() ; i > 0 ; i--) {
                    if (ldapName.getPrefix(i).toString().startsWith("ou")) {
                        return ldapName.getPrefix(i).toString();
                    }
                }

                return null;
            } catch (InvalidNameException ignored) {
            }
        }

        return null;
    }
}
