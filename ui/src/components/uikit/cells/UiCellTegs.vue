<template>
    <div class="ui-tags-cell">
        <span
            v-for="item in props.value"
            :key="item"
            class="pf-c-label pf-m-blue pf-u-mr-sm"
        >
            <span class="pf-c-label__content">{{ item }}</span>
        </span>
    </div>
</template>

<script lang="ts" setup>
interface Props {
    value?: string[]
}

const props = defineProps<Props>()
</script>