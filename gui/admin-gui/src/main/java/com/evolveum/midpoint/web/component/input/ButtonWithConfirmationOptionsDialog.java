/*
 * Copyright (C) 2026 Evolveum and contributors
 *
 * Licensed under the EUPL-1.2 or later.
 *
 */

package com.evolveum.midpoint.web.component.input;

import java.io.Serializable;
import java.util.List;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.model.IModel;

import com.evolveum.midpoint.gui.api.page.PageBase;
import com.evolveum.midpoint.web.component.AjaxIconButton;
import com.evolveum.midpoint.web.component.dialog.ConfirmationOption;
import com.evolveum.midpoint.web.component.dialog.ConfirmationWithOptionsDto;
import com.evolveum.midpoint.web.component.dialog.ConfirmationWithOptionsPanel;
import com.evolveum.midpoint.web.component.util.Describable;
import com.evolveum.midpoint.web.component.util.SerializableBiConsumer;
import com.evolveum.midpoint.web.component.util.SerializableConsumer;

public class ButtonWithConfirmationOptionsDialog<T extends Describable> extends AjaxIconButton {

    private final IModel<ButtonConfig<T>> config;
    private final IModel<ButtonHandlers<T>> handlers;

    public ButtonWithConfirmationOptionsDialog(String id, IModel<ButtonConfig<T>> buttonConfig,
            IModel<ButtonHandlers<T>> clickHandlers) {
        super(id, buttonConfig.getObject().icon(), buttonConfig.getObject().title());
        this.config = buttonConfig;
        this.handlers = clickHandlers;
    }

    @Override
    public void onInitialize() {
        super.onInitialize();
    }

    @Override
    public void onClick(AjaxRequestTarget target) {
        final ButtonConfig<T> cfg = this.config.getObject();
        final ButtonHandlers<T> onClickHandlers = this.handlers.getObject();
        final PageBase pageBase = cfg.pageBase().getObject();
        final ConfirmationWithOptionsPanel<T> dialog = new ConfirmationWithOptionsPanel<>(
                pageBase.getMainPopupBodyId(),
                cfg.confirmationDialogConfig()) {

            @Override
            public void confirmationPerformed(AjaxRequestTarget target,
                    IModel<List<ConfirmationOption<T>>> confirmedOptions) {
                if (onClickHandlers.confirmHandler() != null) {
                    onClickHandlers.confirmHandler().accept(target, confirmedOptions);
                }
            }

            @Override
            public void cancelPerformed(AjaxRequestTarget target) {
                if (onClickHandlers.cancelHandler() != null) {
                    onClickHandlers.cancelHandler().accept(target);
                }
            }
        };
        pageBase.showMainPopup(dialog, target);
    }

    @Override
    public void onDetach() {
        this.config.detach();
        this.handlers.detach();
        super.onDetach();
    }

    public record ButtonHandlers<T extends Describable>(
            SerializableConsumer<AjaxRequestTarget> cancelHandler,
            SerializableBiConsumer<AjaxRequestTarget, IModel<List<ConfirmationOption<T>>>> confirmHandler) implements
            Serializable {

    }

    public record ButtonConfig<T extends Describable>(
            IModel<String> icon,
            IModel<String> title,
            IModel<ConfirmationWithOptionsDto<T>> confirmationDialogConfig,
            IModel<PageBase> pageBase) implements Serializable{}

}
