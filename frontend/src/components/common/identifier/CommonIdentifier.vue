<template>
    <NumericIdentifier
        v-if="type === 'numeric'"
        :index="index"
        class="common-identifier"
    />
    <AlphabetIdentifier
        v-else
        :index="index"
        class="common-identifier"
    />
</template>

<script setup lang="ts">
import {PropType} from 'vue';
import NumericIdentifier from '@/components/common/identifier/NumericIdentifier.vue';
import AlphabetIdentifier from '@/components/common/identifier/AlphabetIdentifier.vue';

defineProps({
    type: {
        type: String as PropType<'numeric' | 'alphabet'>,
        default: 'alphabet',
    },
    index: {
        type: Number,
        required: true,
    },
})
</script>

<style scoped lang="scss">
.common-identifier {
  width: 20px;
  text-transform: uppercase;
  text-align: center;
  color: var(--pf-global--main-color--100, black);
}
</style>
