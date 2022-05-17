<template>
  <div>
    {{ `${index + 1}` }}
  </div>
</template>

<script setup lang="ts">
defineProps<{
  index: number;
}>();
</script>
