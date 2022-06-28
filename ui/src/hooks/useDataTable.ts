import { DataTableResponse, DataTableResponseColumns } from '@/api'
import { getStringDateByFormat } from '@/helpers/getStringDates'
import { Column, Row } from '@/components/uikit/UiTable/UiTable'

const FIXED_COLUMNS_TYPES = ['dimension', 'metric']

type ColumnMap = {
    [key: string]: Column;
}

export type ResponseUseDataTable = {
    hasData: boolean
    tableColumns: ColumnMap
    tableData: Row[]
    tableColumnsValues: Column[]
    lineChart: any[]
    pieChart: any[]
}

export default function useDataTable(payload: DataTableResponse): ResponseUseDataTable {
    const hasData = Boolean(payload?.columns && payload?.columns.length)
    const dimensionColumns = payload?.columns ? payload.columns.filter(column => column.type === 'dimension') : []
    const metricColumns = payload?.columns ? payload?.columns.filter(column => column.type === 'metric') : []
    const metricValueColumns = payload?.columns ? payload?.columns.filter(column => column.type === 'metricValue') : []
    const totalColumn = metricValueColumns.find(item => item.name === 'total')
    const fixedColumnLength = dimensionColumns.length + metricColumns.length - 1

    let tableColumns = {}
    let tableData: Row[] = []
    let lineChart = []
    let pieChart: any[] = []


    if (payload?.columns) {
        tableColumns = {
            ...payload?.columns.reduce((acc: any, column: DataTableResponseColumns, i: number) => {
                if (column.name && column.type) {
                    if (FIXED_COLUMNS_TYPES.includes(column.type)) {
                        acc[column.name] = {
                            pinned: true,
                            value: column.name,
                            title: column.name,
                            truncate: true,
                            lastPinned: fixedColumnLength === i,
                            style: {
                                left: i ? `${i * 170}px` : '',
                                width: 'auto',
                                minWidth: i === 0 ? `${170}x` : '',
                            },
                        }
                    } else {
                        acc[column.name] = {
                            value: column.name,
                            title: getStringDateByFormat(column.name, '%d %b, %Y'),
                        }
                    }
                }

                return acc
            }, {}),
        }

        tableData = payload?.columns.reduce((tableRows: Row[], column: DataTableResponseColumns, indexColumn: number) => {
            const left = indexColumn ? `${indexColumn * 170}px` : ''
            const minWidth = indexColumn === 0 ? `${170}px` : ''

            if (column.values) {
                column.values.forEach((item, i) => {

                    if (!tableRows[i]) {
                        tableRows[i] = []
                    }

                    if (column?.type && FIXED_COLUMNS_TYPES.includes(column.type)) {
                        tableRows[i].push({
                            value: item || '',
                            title: item || '-',
                            pinned: true,
                            lastPinned: indexColumn === fixedColumnLength,
                            style: {
                                left,
                                width: 'auto',
                                minWidth,
                            },
                        })
                    } else {
                        tableRows[i].push({
                            value: item,
                            title: item || '-',
                        })
                    }
                })
            }

            return tableRows
        }, [])
    }

    const tableColumnsValues: Column[] = Object.values(tableColumns)

    if (hasData) {
        lineChart = metricValueColumns.reduce((acc: any[], item: DataTableResponseColumns) => {
            if (item.values && item.name !== 'total') {
                item.values.forEach((value, indexValue: number) => {

                    acc.push({
                        date: item.name ? new Date(item.name) : '',
                        value,
                        category: dimensionColumns.map((column: DataTableResponseColumns) => {
                            return column.values ? column.values[indexValue] : ''
                        }).filter(item => Boolean(item)).join(', '),
                    });
                });
            }
            return acc
        }, [])
    }


    if (hasData && totalColumn?.values) {
        pieChart = totalColumn.values.map((item, index: number) => {
            return {
                type: dimensionColumns.map((column: DataTableResponseColumns) => {
                    return column.values ? column.values[index] : ''
                }).filter(item => Boolean(item)).join(', '),
                value: item,
            }
        })
    }

    return {
        hasData,
        tableColumns,
        tableData,
        tableColumnsValues,
        lineChart,
        pieChart,
    }
}
