<template>
    <div>
        {{ props.title }}
    </div>
</template>

<script lang="ts" setup>

type Props = {
    title: string | number
}

const props = defineProps<Props>()
</script>

<style lang="scss" scoped></style>