import '@patternfly/patternfly/patternfly.scss';
import '@patternfly/patternfly/patternfly-addons.scss';
import 'floating-vue/dist/style.css';
import '@/assets/styles/main.scss';
import lang from '@/lang';
import {createApp} from 'vue';
import {createPinia} from 'pinia';
import App from '@/App.vue';
import makeServer from '@/server';
import FloatingVue from 'floating-vue';
import {router} from '@/router';
import uikitPlugin from '@/plugins/uikit';
import i18nPlugin from '@/plugins/i18n';
import GridLayout from 'vue3-drr-grid-layout';
import 'vue3-drr-grid-layout/dist/style.css';

if (import.meta.env.VITE_MOCK_API) {
    makeServer();
}
const app = createApp(App);

app.use(createPinia());
FloatingVue.options.disposeTimeout = 300;
app.use(FloatingVue);
app.use(GridLayout);
app.use(uikitPlugin);
app.use(i18nPlugin);
app.use(router);

app.directive('click-outside', {
    mounted(el, binding, vnode) {
        el.clickOutsideEvent = function(event: Event) {
            if (!(el === event.target || el.contains(event.target))) {
                binding.value(event, el)
            }
        }
        document.body.addEventListener('mousedown', el.clickOutsideEvent)
    },
    unmounted(el) {
        document.body.removeEventListener('mousedown', el.clickOutsideEvent)
    }
})

app.config.globalProperties.loadDictionary(lang.en)
app.mount('#app');

app.config.errorHandler = (err, vm, info) => {
    console.log(err, vm, info);
    // err: error trace
    // vm: component in which error occured
    // info: Vue specific error information such as lifecycle hooks, events etc.

    // TODO: Perform any custom logic or log to server
};
