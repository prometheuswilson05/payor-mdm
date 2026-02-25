import { useEffect, useMemo, useState } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { ChevronDown, ChevronRight, Loader2, AlertCircle } from 'lucide-react'
import { querySnowflake } from '../api'

interface AuditRow {
  LOG_ID: string
  ENTITY_TYPE: string
  ENTITY_ID: string
  ACTION: string
  CHANGED_BY: string
  CHANGED_AT: string
  CHANGE_DETAILS: string
}

const PAGE_SIZE = 20

const columnHelper = createColumnHelper<AuditRow>()

export default function AuditTrail() {
  const [data, setData] = useState<AuditRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [hasMore, setHasMore] = useState(true)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [entityFilter, setEntityFilter] = useState('')
  const [actionFilter, setActionFilter] = useState('')

  useEffect(() => {
    loadData()
  }, [page])

  async function loadData() {
    setLoading(true)
    setError(null)
    try {
      let sql = `SELECT * FROM MDM.AUDIT.MDM_CHANGE_LOG`
      const conditions: string[] = []
      if (entityFilter) conditions.push(`ENTITY_TYPE = '${entityFilter}'`)
      if (actionFilter) conditions.push(`ACTION = '${actionFilter}'`)
      if (conditions.length > 0) sql += ` WHERE ${conditions.join(' AND ')}`
      sql += ` ORDER BY CHANGED_AT DESC LIMIT ${PAGE_SIZE + 1} OFFSET ${page * PAGE_SIZE}`

      const rows = await querySnowflake(sql)
      setHasMore(rows.length > PAGE_SIZE)
      setData(rows.slice(0, PAGE_SIZE))
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  function applyFilters() {
    setPage(0)
    loadData()
  }

  const columns = useMemo(() => [
    columnHelper.display({
      id: 'expand',
      cell: ({ row }) => (
        <button
          onClick={() => setExpandedId(expandedId === row.original.LOG_ID ? null : row.original.LOG_ID)}
          className="text-gray-400 hover:text-gray-100"
        >
          {expandedId === row.original.LOG_ID ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>
      ),
      size: 40,
    }),
    columnHelper.accessor('ENTITY_TYPE', {
      header: 'Entity Type',
      cell: ({ getValue }) => (
        <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-800 text-gray-300">
          {getValue()}
        </span>
      ),
    }),
    columnHelper.accessor('ENTITY_ID', { header: 'Entity ID' }),
    columnHelper.accessor('ACTION', {
      header: 'Action',
      cell: ({ getValue }) => {
        const val = getValue()
        const color =
          val === 'match_confirmed' ? 'bg-green-900/30 text-green-400' :
          val === 'match_rejected' ? 'bg-red-900/30 text-red-400' :
          'bg-blue-900/30 text-blue-400'
        return <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>{val}</span>
      },
    }),
    columnHelper.accessor('CHANGED_BY', { header: 'Changed By' }),
    columnHelper.accessor('CHANGED_AT', {
      header: 'Time',
      cell: ({ getValue }) => {
        const val = getValue()
        return val ? new Date(val).toLocaleString() : ''
      },
    }),
  ], [expandedId])

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

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
      <h2 className="text-2xl font-bold">Audit Trail</h2>

      {/* Filters */}
      <div className="flex gap-4">
        <div>
          <label className="block text-xs text-gray-400 mb-1">Entity Type</label>
          <select
            value={entityFilter}
            onChange={(e) => setEntityFilter(e.target.value)}
            className="bg-gray-900 border border-gray-800 rounded px-3 py-2 text-sm text-gray-100"
          >
            <option value="">All</option>
            <option value="match_candidate">Match Candidate</option>
            <option value="golden_record">Golden Record</option>
            <option value="hierarchy">Hierarchy</option>
          </select>
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Action</label>
          <select
            value={actionFilter}
            onChange={(e) => setActionFilter(e.target.value)}
            className="bg-gray-900 border border-gray-800 rounded px-3 py-2 text-sm text-gray-100"
          >
            <option value="">All</option>
            <option value="match_confirmed">Confirmed</option>
            <option value="match_rejected">Rejected</option>
            <option value="override">Override</option>
            <option value="hierarchy_add">Hierarchy Add</option>
          </select>
        </div>
        <div className="flex items-end">
          <button
            onClick={applyFilters}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-sm font-medium"
          >
            Apply
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        {loading ? (
          <div className="flex justify-center py-12">
            <Loader2 size={24} className="animate-spin text-blue-500" />
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b border-gray-800">
                  {hg.headers.map((header) => (
                    <th key={header.id} className="px-4 py-3 text-left text-gray-400 font-medium">
                      {flexRender(header.column.columnDef.header, header.getContext())}
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
                  {expandedId === row.original.LOG_ID && (
                    <tr key={`${row.id}-detail`} className="bg-gray-950">
                      <td colSpan={columns.length} className="px-6 py-4">
                        <h4 className="text-xs font-medium text-gray-400 mb-2">Change Details</h4>
                        <pre className="text-xs text-gray-300 bg-gray-900 rounded p-3 whitespace-pre-wrap font-mono">
                          {formatDetails(row.original.CHANGE_DETAILS)}
                        </pre>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-gray-400">
        <span>Page {page + 1}</span>
        <div className="flex gap-2">
          <button
            onClick={() => setPage(Math.max(0, page - 1))}
            disabled={page === 0}
            className="px-3 py-1 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded text-sm"
          >
            Previous
          </button>
          <button
            onClick={() => setPage(page + 1)}
            disabled={!hasMore}
            className="px-3 py-1 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded text-sm"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  )
}

function formatDetails(details: string | null): string {
  if (!details) return 'No details'
  try {
    const parsed = JSON.parse(details)
    return JSON.stringify(parsed, null, 2)
  } catch {
    return details
  }
}
