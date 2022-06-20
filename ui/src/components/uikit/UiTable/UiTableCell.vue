<template>
    <div
        :class="{
            'pf-u-text-nowrap': props.nowrap,
        }"
    >
        {{ props.title }}
    </div>
</template>

<script lang="ts" setup>

type Props = {
    title: string | number
    value?: string | number
    nowrap?: boolean
}

const props = defineProps<Props>()
</script>