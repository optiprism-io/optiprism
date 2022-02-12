<template>
    <div class="alphabet-identifier">
        {{ identifier }}
    </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
    index: number;
}>();

const alphabet = "abcdefghijklmnopqrstuvwxyz".split("");
const identifier = computed((): string => alphabet[props.index]);
</script>

<style lang="scss" scoped>
.alphabet-identifier {
    width: 20px;
    text-transform: uppercase;
    text-align: center;
    color: var(--pf-global--main-color--100, black);
}
</style>