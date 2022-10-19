import { App as Application, createApp } from 'vue';
import UiButton from '@/components/uikit/UiButton.vue';
import UiIcon from '@/components/uikit/UiIcon.vue';
import UiSpinner from '@/components/uikit/UiSpinner.vue';
import UiDropdown from '@/components/uikit/UiDropdown.vue';
import UiToggleGroup from '@/components/uikit/UiToggleGroup.vue';
import UiTabs from '@/components/uikit/UiTabs.vue';
import UiTable from '@/components/uikit/UiTable/UiTable.vue';
import UiPopupWindow, { Props as UiPopupWindowType } from '@/components/uikit/UiPopupWindow.vue'

const componentMap: any = {
    UiButton,
    UiIcon,
    UiSpinner,
    UiDropdown,
    UiToggleGroup,
    UiTabs,
    UiTable,
};

declare module '@vue/runtime-core' {
    export interface GlobalComponents {
        UiButton: typeof UiButton,
        UiToggleGroup: typeof UiToggleGroup,
        UiTabs: typeof UiTabs,
        UiTable: typeof UiTable,
    }
}

export default {
    install(app: Application) {
        for (const name in componentMap) {
            app.component(name, componentMap[name])
        }

        app.config.globalProperties.$confirm = function (text: string, params: UiPopupWindowType = {}) {
            return new Promise((resolve, reject) => {
                const confirm = createApp(UiPopupWindow, {
                    content: text,
                    applyButton: params.applyButton ?? 'apply',
                    cancelButton: params.cancelButton ?? 'cancel',
                    ...params,
                    onApply: () => {
                        confirm.unmount();
                        resolve(true);
                    },
                    onCancel: (e: string) => {
                        confirm.unmount();
                        reject(e === 'cancel-button' ? false : null);
                    }
                })

                const wrapper = document.createElement('div');
                confirm.mount(wrapper);
                document.body.appendChild(wrapper);
            })
        }
        app.provide('$confirm', app.config.globalProperties.$confirm)
    },
}