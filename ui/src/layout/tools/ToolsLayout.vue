<script lang="ts">
import GridContainer from '@/components/grid/GridContainer.vue';
import GridItem from '@/components/grid/GridItem.vue';
import { defineComponent, h, VNode } from 'vue';

export default defineComponent({
  components: {
    GridContainer,
    GridItem,
  },
  setup(props, { slots }) {
    const children = slots.default?.() ?? [];
    const renderGridItem = (child: VNode) => h(GridItem, {}, () => child);

    return () => [
      h('div', { class: 'pf-u-font-size-2xl pf-u-mb-md' }, slots.title?.() ?? ''),
      h(GridContainer, {}, () => [
        ...children.map(renderGridItem),
        h(GridItem, { col: 12, colLg: 12 }, slots.main)
      ])
    ];
  }
});
</script>
