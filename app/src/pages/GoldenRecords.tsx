import { useEffect, useMemo, useState } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  flexRender,
  createColumnHelper,
  type SortingState,
} from '@tanstack/react-table'
import { ChevronDown, ChevronRight, Search, Loader2, AlertCircle } from 'lucide-react'
import { querySnowflake } from '../api'

interface GoldenRecord {
  MASTER_PAYOR_ID: string
  GOLDEN_PAYOR_NAME: string
  GOLDEN_TAX_ID: string
  GOLDEN_NPI: string
  GOLDEN_STATE_CODE: string
  GOLDEN_PAYOR_TYPE: string
  GOLDEN_STATUS: string
  SOURCE_COUNT: number
}

const columnHelper = createColumnHelper<GoldenRecord>()

export default function GoldenRecords() {
  const [data, setData] = useState<GoldenRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sorting, setSorting] = useState<SortingState>([])
  const [globalFilter, setGlobalFilter] = useState('')
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [expandedData, setExpandedData] = useState<{ sources: any[]; hierarchy: any[] } | null>(null)
  const [expandLoading, setExpandLoading] = useState(false)

  useEffect(() => {
    loadData()
  }, [])

  async function loadData() {
    setLoading(true)
    setError(null)
    try {
      const rows = await querySnowflake(`
        SELECT g.*,
          (SELECT COUNT(*) FROM MDM.MASTER.XREF x WHERE x.MASTER_PAYOR_ID = g.MASTER_PAYOR_ID) as SOURCE_COUNT
        FROM MDM.MASTER.GOLDEN_PAYORS g
        ORDER BY g.GOLDEN_PAYOR_NAME
      `)
      setData(rows)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  async function toggleExpand(id: string) {
    if (expandedId === id) {
      setExpandedId(null)
      setExpandedData(null)
      return
    }
    setExpandedId(id)
    setExpandLoading(true)
    try {
      const [sources, hierarchy] = await Promise.all([
        querySnowflake(`SELECT x.*, s.* FROM MDM.MASTER.XREF x JOIN MDM.STAGING.STG_PAYORS_UNIONED s ON x.SOURCE_RECORD_ID = s.RECORD_ID WHERE x.MASTER_PAYOR_ID = '${id}'`),
        querySnowflake(`SELECT * FROM MDM.MASTER.PAYOR_HIERARCHY WHERE CHILD_PAYOR_ID = '${id}' OR PARENT_PAYOR_ID = '${id}'`),
      ])
      setExpandedData({ sources, hierarchy })
    } catch {
      setExpandedData({ sources: [], hierarchy: [] })
    } finally {
      setExpandLoading(false)
    }
  }

  const columns = useMemo(() => [
    columnHelper.display({
      id: 'expand',
      cell: ({ row }) => (
        <button onClick={() => toggleExpand(row.original.MASTER_PAYOR_ID)} className="text-gray-400 hover:text-gray-100">
          {expandedId === row.original.MASTER_PAYOR_ID ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        </button>
      ),
      size: 40,
    }),
    columnHelper.accessor('GOLDEN_PAYOR_NAME', { header: 'Name', size: 250 }),
    columnHelper.accessor('GOLDEN_TAX_ID', { header: 'Tax ID', size: 120 }),
    columnHelper.accessor('GOLDEN_NPI', { header: 'NPI', size: 120 }),
    columnHelper.accessor('GOLDEN_STATE_CODE', { header: 'State', size: 80 }),
    columnHelper.accessor('GOLDEN_PAYOR_TYPE', { header: 'Type', size: 100 }),
    columnHelper.accessor('GOLDEN_STATUS', {
      header: 'Status',
      size: 100,
      cell: ({ getValue }) => {
        const val = getValue()
        const color = val === 'active' ? 'text-green-400 bg-green-900/30' : 'text-gray-400 bg-gray-800'
        return <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>{val}</span>
      },
    }),
    columnHelper.accessor('SOURCE_COUNT', {
      header: 'Sources',
      size: 80,
      cell: ({ getValue }) => (
        <span className="px-2 py-0.5 rounded bg-blue-900/30 text-blue-400 text-xs font-medium">
          {getValue()}
        </span>
      ),
    }),
    columnHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: () => (
        <button disabled className="px-3 py-1 bg-gray-800 text-gray-500 rounded text-xs cursor-not-allowed">
          Override Field
        </button>
      ),
      size: 120,
    }),
  ], [expandedId])

  const table = useReactTable({
    data,
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 20 } },
  })

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 size={32} className="animate-spin text-blue-500" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-96 text-gray-400">
        <AlertCircle size={48} className="mb-4 text-red-500" />
        <p className="mb-2">{error}</p>
        <button onClick={loadData} className="px-4 py-2 bg-blue-600 rounded-lg text-sm">Retry</button>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Golden Records</h2>
        <div className="relative">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            value={globalFilter}
            onChange={(e) => setGlobalFilter(e.target.value)}
            placeholder="Search records..."
            className="bg-gray-900 border border-gray-800 rounded-lg pl-9 pr-4 py-2 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:border-gray-700 w-72"
          />
        </div>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="border-b border-gray-800">
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    className="px-4 py-3 text-left text-gray-400 font-medium cursor-pointer hover:text-gray-200 select-none"
                    style={{ width: header.getSize() }}
                  >
                    <div className="flex items-center gap-1">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {header.column.getIsSorted() === 'asc' ? ' ↑' : header.column.getIsSorted() === 'desc' ? ' ↓' : ''}
                    </div>
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row, i) => (
              <>
                <tr key={row.id} className={`border-b border-gray-800 ${i % 2 === 0 ? 'bg-gray-900' : 'bg-gray-950'}`}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-4 py-2">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
                {expandedId === row.original.MASTER_PAYOR_ID && (
                  <tr key={`${row.id}-detail`} className="bg-gray-950">
                    <td colSpan={columns.length} className="px-6 py-4">
                      {expandLoading ? (
                        <Loader2 size={16} className="animate-spin text-gray-500" />
                      ) : expandedData ? (
                        <div className="space-y-4">
                          <div>
                            <h4 className="text-xs font-medium text-gray-400 mb-2">Contributing Source Records</h4>
                            {expandedData.sources.length === 0 ? (
                              <p className="text-xs text-gray-600">No source records found</p>
                            ) : (
                              <div className="space-y-1">
                                {expandedData.sources.map((s: any, j: number) => (
                                  <div key={j} className="flex gap-4 text-xs bg-gray-900 rounded px-3 py-2">
                                    <span className="text-blue-400 font-medium">{s.SOURCE_SYSTEM}</span>
                                    <span className="text-gray-300">{s.PAYOR_NAME}</span>
                                    <span className="text-gray-500">{s.TAX_ID}</span>
                                    <span className="text-gray-500">{s.STATE_CODE}</span>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                          <div>
                            <h4 className="text-xs font-medium text-gray-400 mb-2">Hierarchy</h4>
                            {expandedData.hierarchy.length === 0 ? (
                              <p className="text-xs text-gray-600">No hierarchy links</p>
                            ) : (
                              <div className="space-y-1">
                                {expandedData.hierarchy.map((h: any, j: number) => (
                                  <div key={j} className="text-xs bg-gray-900 rounded px-3 py-2 text-gray-300">
                                    {h.PARENT_PAYOR_ID} → {h.CHILD_PAYOR_ID} ({h.RELATIONSHIP_TYPE})
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        </div>
                      ) : null}
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-gray-400">
        <span>
          Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()} ({data.length} records)
        </span>
        <div className="flex gap-2">
          <button
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
            className="px-3 py-1 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded text-sm"
          >
            Previous
          </button>
          <button
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
            className="px-3 py-1 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded text-sm"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  )
}
