/*
 * Copyright (C) 2026 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 */

package com.evolveum.midpoint.gui.impl.page.admin.resource.component.wizard.schemaHandling.objectType.smart.component;

import java.util.List;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.model.IModel;

import com.evolveum.midpoint.gui.api.page.PageBase;
import com.evolveum.midpoint.web.component.dialog.ConfirmationOption;
import com.evolveum.midpoint.web.component.dialog.ConfirmationWithOptionsDto;
import com.evolveum.midpoint.web.component.dialog.privacy.DataAccessPermission;
import com.evolveum.midpoint.web.component.input.ButtonWithConfirmationOptionsDialog;
import com.evolveum.midpoint.web.component.util.Describable;

public class SmartSuggestButtonWithConfirmation<T extends Describable> extends ButtonWithConfirmationOptionsDialog<T> {

    public SmartSuggestButtonWithConfirmation(String id, IModel<ButtonConfig<T>> buttonConfig,
            IModel<ButtonHandlers<T>> clickHandlers) {
        super(id, buttonConfig, clickHandlers);
    }

    public static SmartSuggestButtonWithConfirmation<DataAccessPermission> create(String id, IModel<String> title,
            IModel<String> icon, List<ConfirmationOption<DataAccessPermission>> options,
            IModel<ButtonHandlers<DataAccessPermission>> clickHandlers, PageBase pageBase) {
        final ConfirmationWithOptionsDto<DataAccessPermission> confirmationDialogConfig =
                ConfirmationWithOptionsDto.<DataAccessPermission>builder()
                        .confirmationTitle(pageBase.createStringResource("SmartSuggestConfirmationPanel.title"))
                        .confirmationSubtitle(pageBase.createStringResource("SmartSuggestConfirmationPanel.subtitle"))
                        .confirmationOptionsTitle(pageBase.createStringResource(
                                "SmartSuggestConfirmationPanel.request.component.title"))
                        .confirmationInfoMessage(pageBase.createStringResource(
                                "SmartSuggestConfirmationPanel.infoMessage"))
                        .confirmationOptions(options)
                        .build();
        final ButtonConfig<DataAccessPermission> buttonConfig = new ButtonConfig<>(
                icon, title, () -> confirmationDialogConfig, () -> pageBase);

        final SmartSuggestButtonWithConfirmation<DataAccessPermission> button =
                new SmartSuggestButtonWithConfirmation<>(id, () -> buttonConfig, clickHandlers);
        button.add(AttributeModifier.append("class", "mx-2 btn rounded bg-purple"));
        return button;
    }
}
