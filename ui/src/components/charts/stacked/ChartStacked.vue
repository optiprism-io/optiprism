<template>
    <div class="pf-l-flex pf-m-column">
        <div
            ref="container"
            class="pf-l-flex__item"
        />
        <div class="pf-u-font-size-lg pf-u-font-weight-bold pf-l-flex__item pf-u-px-lg">
            <slot />
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, PropType, ref, watch} from 'vue';
import {Chart, getEngine} from '@antv/g2';
import { lighten } from '@/helpers/colorHelper';

const G = getEngine('svg')
const container = ref<HTMLDivElement | null>(null)
const chart = ref<Chart | null>(null)

const props = defineProps({
    data: {
        type: Array as PropType<Record<string, string | number>[]>,
        default: () => []
    },
    xKey: {
        type: String,
        required: true
    },
    yKeys: {
        type: Array as PropType<string[][]>,
        required: true
    },
    mainKeys: {
        type: Array as PropType<string[]>,
        default: () => []
    },
    labels: {
        type: Object as PropType<Record<string, string>>,
        default: () => ({})
    },
    colors: {
        type: Array as PropType<string[]>,
        default: () => ['#ee5253', '#2e86de', '#ff9f43', '#5f27cd', '#10ac84', '#f368e0', '#0abde3']
    },
    width: {
        type: Number,
        default: 400
    },
})

const primaryKeys = computed(() => props.yKeys.map(keys => keys[0]))
const secondaryKeys = computed(() => props.yKeys.map(keys => keys[1]))

const dataView = computed(() => {
    const colors = props.colors.slice(0, props.data.length)

    return props.data
        .map((item, i) => {
            return Array.from({ length: primaryKeys.value.length }).map((_, j) => {
                const iterator = primaryKeys.value.length - 1 - j

                const primaryKey = primaryKeys.value[iterator]
                const secondaryKey = secondaryKeys.value[iterator]

                return {
                    [props.xKey]: item[props.xKey],
                    primaryKey,
                    secondaryKey,
                    primaryValue: item[primaryKey],
                    secondaryValue: item[secondaryKey],
                    color: lighten(colors[i], iterator * 80),
                }
            })
        })
        .flat()
})

watch(() => [container.value, dataView.value], () => {
    if (!container.value) {
        return
    }

    if (chart.value) {
        chart.value.destroy()
    }

    chart.value = new Chart({
        container: container.value,
        height: 500,
        width: props.width,
        autoFit: false,
        padding: [40, 20],
        renderer: 'svg'
    });

    chart.value
        .animate(false)
        .legend(false)
        .tooltip({
            customItems: originalItems => {
                const [item] = originalItems
                const { primaryKey, secondaryKey, primaryValue, secondaryValue } = item.data as Record<string, string>

                const secondaryBlock = secondaryKey && secondaryValue
                    ? [{
                        ...item,
                        name: props.labels[secondaryKey] ?? secondaryKey,
                        value: secondaryValue
                    }]
                    : []

                return [
                    {
                        ...item,
                        name: props.labels[primaryKey] ?? primaryKey,
                        value: primaryValue,
                    },
                    ...secondaryBlock
                ]
            },
            showMarkers: false
        })
        .data(dataView.value)
        .axis('dimension', false)
        .axis('primaryValue', false)
        .interval({ intervalPadding: 20 })
        .adjust('stack')
        .position('dimension*primaryValue')
        .color('color', color => color)
        .label(
            'color',
            {
                position: 'top',
                offsetY: 3,
                offsetX: 0,
                content: (data) => {
                    const {
                        primaryKey: primaryKeyUnknown,
                        secondaryKey: secondaryKeyUnknown,
                        primaryValue: primaryValueUnknown,
                        secondaryValue: secondaryValueUnknown
                    } = data as Record<string, string | number>

                    const primaryKey = String(primaryKeyUnknown)
                    const secondaryKey = String(secondaryKeyUnknown)
                    const primaryValue = Number(primaryValueUnknown)
                    const secondaryValue = Number(secondaryValueUnknown)

                    const getSecondaryValue = (): string => {
                        const postfix = secondaryKey?.indexOf('Ratio') !== -1 ? '%' : ''
                        return secondaryValue + postfix
                    }

                    const getPrimaryValue = (): string => {
                        /* Human-readable number */
                        if (primaryValue > 1e6) {
                            return (primaryValue / 1e6).toFixed(1) + 'M'
                        }

                        const [integer, fractional] = String(primaryValue).split('.')
                        const thousands = integer.length > 3 ? integer.slice(0, integer.length - 3) : null
                        const smallValue = String(primaryValue - Number(thousands) * 1e3).padStart(3, '0')
                        return `${thousands ? thousands + ',' : ''}${smallValue}${fractional ? '.' + fractional : ''}`
                    }

                    if (!props.mainKeys.includes(primaryKey) || !props.mainKeys.includes(secondaryKey)) {
                        return ''
                    }

                    const group = new G.Group({})

                    const rect = new G.Shape.Rect({
                        attrs: {
                            x: 0,
                            y: 0,
                            width: 60,
                            height: 40,
                            fill: '#ffffff',
                            stroke: '#cccccc',
                            strokeWidth: 0.5,
                            radius: 4,
                        }
                    })

                    const secondaryText = new G.Shape.Text({
                        attrs: {
                            x: 30,
                            y: 3,
                            text: getSecondaryValue(),
                            textAlign: 'center',
                            fontSize: 14,
                            fontWeight: 400,
                            textBaseline: 'top',
                            fill: '#000000',
                        }
                    })

                    const primaryText = new G.Shape.Text({
                        attrs: {
                            x: 30,
                            y: 34,
                            text: getPrimaryValue(),
                            textAlign: 'center',
                            fontSize: 8,
                            fontFamily: 'sans-serif',
                            fontWeight: 300,
                            textBaseline: 'bottom',
                            fill: '#ababab',
                        }
                    })

                    group.addShape(rect)
                    group.addShape(secondaryText)
                    group.addShape(primaryText)

                    group.moveTo(0, 9)

                    return group
                }
                // content: (data) => {
                //     const { primaryKey: primaryKeyUnknown, secondaryKey: secondaryKeyUnknown, primaryValue: primaryValueUnknown, secondaryValue: secondaryValueUnknown } = data as Record<string, string | number>
                //     const primaryKey = String(primaryKeyUnknown)
                //     const secondaryKey = String(secondaryKeyUnknown)
                //     const primaryValue = Number(primaryValueUnknown)
                //     const secondaryValue = Number(secondaryValueUnknown)
                //
                //     if (props.mainKeys.includes(primaryKey) && props.mainKeys.includes(secondaryKey)) {
                //         return [primaryValue, secondaryValue].filter(value => value > 0).join('\n')
                //     } else {
                //         return ''
                //     }
                // },
            }
        )

    chart.value.render();
})

watch(() => props.width, (width) => {
    if (chart.value) {
        chart.value.changeSize(width, 500)
    }
}, {immediate: true})
</script>
