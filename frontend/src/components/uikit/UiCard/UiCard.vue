<template>
    <UiCardContainer>
        <UiCardTitle>
            <router-link
                v-if="link"
                :to="link"
            >
                {{ title }}
            </router-link>
            <span v-else>
                {{ title }}
            </span>
            <template
                v-if="$slots.rightTitle"
                #right
            >
                <slot name="rightTitle" />
            </template>
        </UiCardTitle>

        <UiCardBody>
            <slot />
        </UiCardBody>
    </UiCardContainer>
</template>

<script setup lang="ts">
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue';
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue';

defineProps({
    title: {
        type: String,
        default: '',
    },
    link: [Object, String]
})
</script>
