<template>
    <div class="ui-tags-cell">
        <UiTags :value="props.value" />
    </div>
</template>

<script lang="ts" setup>
import UiTags from '../UiTags.vue'

interface Props {
    value?: string[]
}

const props = defineProps<Props>()
</script>