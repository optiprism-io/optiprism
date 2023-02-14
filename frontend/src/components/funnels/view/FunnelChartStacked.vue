<template>
    <div class="pf-l-flex pf-m-column">
        <div
            ref="container"
            class="pf-l-flex__item"
        />
        <div
            class="pf-u-font-weight-bold pf-l-flex__item pf-u-px-lg"
            :class="{
                'pf-u-font-size-lg': !liteChart,
            }"
        >
            <slot />
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, inject, PropType, ref, watch} from 'vue';
import {Chart, getEngine} from '@antv/g2';
import {lighten} from '@/helpers/colorHelper';
import {I18N} from '@/utils/i18n';
import iconArrow from '@/assets/img/arrow-down.png';

const {$t} = inject('i18n') as I18N
const G = getEngine('canvas')
const container = ref<HTMLDivElement | null>(null)
const chart = ref<Chart | null>(null)

const props = defineProps({
    data: {
        type: Array as PropType<Record<string, string | number>[]>,
        default: () => []
    },
    width: {
        type: Number,
        default: 400
    },
    height: {
        type: Number,
        default: 500
    },
    colors: {
        type: Array as PropType<string[]>,
        default: () => []
    },
    liteChart: Boolean
})

const xKey = 'dimension'
const primaryKeys = ['conversionCount', 'dropOffCount']
const secondaryKeys = ['conversionRatio', 'dropOffRatio']
const labelKey = 'dropOffCount'

const dataView = computed(() => {
    return props.data
        .map((item, i) => {
            return Array.from({ length: primaryKeys.length }).map((_, j) => {
                const iterator = primaryKeys.length - 1 - j

                const primaryKey = primaryKeys[iterator]
                const secondaryKey = secondaryKeys[iterator]

                return {
                    index: i,
                    [xKey]: item[xKey],
                    primaryKey,
                    secondaryKey,
                    primaryValue: item[primaryKey],
                    secondaryValue: item[secondaryKey],
                    color: lighten(props.colors[i], iterator * 80),
                }
            })
        })
        .flat()
})

/* Human-readable number */
const humanReadable = (number: number | string): string => {
    number = Number(number)

    if (number > 1e6) {
        return (number / 1e6).toFixed(1) + 'M'
    }
    const [integer, fractional] = String(number).split('.')
    const thousands = integer.length > 3 ? integer.slice(0, integer.length - 3) : null
    const smallValue = String(number - Number(thousands) * 1e3).padStart(3, '0')
    return `${thousands ? thousands + ',' : ''}${smallValue}${fractional ? '.' + fractional : ''}`
}

const update = () => {
    if (!container.value) {
        return
    }

    if (chart.value) {
        chart.value.destroy()
    }

    chart.value = new Chart({
        container: container.value,
        height: props.height,
        width: props.width,
        autoFit: true,
        padding: props.liteChart ? [50, 5, 0, 5] : [80, 50, 30, 50],
        renderer: 'canvas'
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
                        name: $t(`funnels.view.${secondaryKey}`),
                        value: secondaryValue
                    }]
                    : []

                return [
                    {
                        ...item,
                        name: $t(`funnels.view.${primaryKey}`),
                        value: primaryValue,
                    },
                    ...secondaryBlock
                ]
            },
            showMarkers: false
        })
        .data(dataView.value)
        .axis(xKey, false)
        .axis('primaryValue', false)
        .interval({ intervalPadding: 20 })
        .adjust('stack')
        .color('color', color => color)
        .position(`${xKey}*primaryValue`)
        .label('primaryValue', () => {
            return {
                position: 'top',
                offset: 0,
                content: (data) => {
                    const width = props.liteChart ? 39 : 64
                    const size = props.liteChart ? 10 : 14

                    const commonProps = {
                        textAlign: 'left',
                        fontSize: size,
                        fontFamily: 'sans-serif',
                        textBaseline: 'top'
                    }

                    const numberProps = {
                        ...commonProps,
                        fontWeight: 400,
                        fill: '#000000'
                    }

                    const percentageProps = {
                        ...commonProps,
                        fontWeight: 600,
                    }

                    const { index: indexUnknown, primaryKey: primaryKeyUnknown } = data as Record<string, string | number>
                    const index = Number(indexUnknown)
                    const primaryKey = String(primaryKeyUnknown)
                    const dataItem = props.data[index]

                    if (primaryKey !== labelKey || !dataItem) {
                        return ''
                    }

                    const { dropOffCount, dropOffRatio, conversionCount, conversionRatio } = dataItem;
                    const conversionCountText = humanReadable(conversionCount)
                    const conversionRatioText = `${conversionRatio}%`
                    const dropOffCountText = humanReadable(dropOffCount)
                    const dropOffRatioText = `${dropOffRatio}%`

                    const hasDropOff = dropOffCount > 0 && dropOffRatio > 0

                    const group = new G.Group({})

                    if (conversionCount > 0) {
                        group.addShape({
                            type: 'text',
                            attrs: {
                                x: 0,
                                y: size,
                                text: conversionCountText,
                                ...numberProps
                            }
                        })
                    }

                    if (conversionRatio > 0) {
                        group.addShape({
                            type: 'text',
                            attrs: {
                                x: 0,
                                y: 0,
                                text: conversionRatioText,
                                fill: '#000000',
                                ...percentageProps
                            }
                        })
                    }

                    if (dropOffCount > 0) {
                        group.addShape({
                            type: 'text',
                            attrs: {
                                x: dropOffRatio > 0 ? 0 : width / 2,
                                y: 4 * size,
                                text: dropOffCountText,
                                ...numberProps
                            }
                        })
                    }

                    if (dropOffRatio > 0) {
                        group.addShape({
                            type: 'text',
                            attrs: {
                                x: 0,
                                y: 3 * size,
                                text: dropOffRatioText,
                                fill: '#ff0000',
                                ...percentageProps
                            }
                        })

                        group.addShape({
                            type: 'image',
                            attrs: {
                                x: width - size,
                                y: 3 * size,
                                width: size,
                                height: size,
                                img: iconArrow,
                            }
                        })
                    }

                    group.moveTo(0, -(hasDropOff ? 3 : 2) * size)
                    return group
                }
            }
        })

    chart.value.render();
}

watch(() => [container.value, dataView.value], () => {
    update()
})

watch(() => props.height, (height) => {
    if (chart.value) {
        chart.value.changeSize(props.width, height)
    }
}, { immediate: true })

watch(() => props.width, (width) => {
    if (chart.value) {
        chart.value.changeSize(width, props.height)
    }
}, { immediate: true })
</script>
