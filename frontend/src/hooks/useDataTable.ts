import { DataTableResponse, DataTableResponseColumnsInner } from '@/api'
import {getStringDateByFormat} from '@/helpers/getStringDates'
import {Cell, Column, Row} from '@/components/uikit/UiTable/UiTable'

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
            ...payload?.columns.reduce((acc: any, column: DataTableResponseColumnsInner, i: number) => {
                if (column.name && column.type) {
                    if (FIXED_COLUMNS_TYPES.includes(column.type)) {
                        acc[column.name] = {
                            value: column.name,
                            title: column.name,
                            truncate: true,
                            lastFixed: fixedColumnLength === i,
                            fixed: true,
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

        tableData = payload?.columns.reduce((tableRows: Row[], column: DataTableResponseColumnsInner, indexColumn: number) => {
            if (column.data) {
                column.data.forEach((item, i) => {
                    if (!tableRows[i]) {
                        tableRows[i] = []
                    }

                    if (column?.type) {
                        const cell: Cell = {
                            key: column.type,
                            value: item,
                            title: item || '-',
                        }

                        if (FIXED_COLUMNS_TYPES.includes(column.type)) {
                            cell.lastFixed = indexColumn === fixedColumnLength
                            cell.fixed = true
                        }

                        tableRows[i].push(cell)
                    }
                })
            }

            return tableRows
        }, [])
    }

    const tableColumnsValues: Column[] = Object.values(tableColumns)

    if (hasData) {
        lineChart = metricValueColumns.reduce((acc: any[], item: DataTableResponseColumnsInner) => {
            if (item.data && item.name !== 'total') {
                item.data.forEach((value, indexValue: number) => {

                    acc.push({
                        date: item.name ? new Date(item.name) : '',
                        value,
                        category: dimensionColumns.map((column: DataTableResponseColumnsInner) => {
                            return column.data ? column.data[indexValue] : ''
                        }).filter(item => Boolean(item)).join(', '),
                    });
                });
            }
            return acc
        }, [])
    }

    if (hasData && totalColumn?.data) {
        pieChart = totalColumn.data.map((item, index: number) => {
            return {
                type: dimensionColumns.map((column: DataTableResponseColumnsInner) => {
                    return column.data ? column.data[index] : ''
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
