<template>
    <div>
        {{ identifier }}
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';

const props = defineProps<{
  index: number;
}>();

const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');
const identifier = computed((): string => alphabet[props.index]);
</script>
