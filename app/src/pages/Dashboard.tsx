import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell,
} from 'recharts'
import { RefreshCw, Crown, Database, AlertCircle, Network, Copy } from 'lucide-react'
import { querySnowflake, triggerDbtRun } from '../api'

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316']

interface KPIs {
  goldenCount: number
  sourceCount: number
  pendingReview: number
  hierarchyCount: number
  duplicatePairs: number
}

export default function Dashboard() {
  const [kpis, setKpis] = useState<KPIs | null>(null)
  const [histogram, setHistogram] = useState<any[]>([])
  const [sourcePie, setSourcePie] = useState<any[]>([])
  const [activity, setActivity] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [dbtRunning, setDbtRunning] = useState(false)
  const [dbtOutput, setDbtOutput] = useState<string | null>(null)

  useEffect(() => {
    loadData()
  }, [])

  async function loadData() {
    setLoading(true)
    setError(null)
    try {
      const [golden, source, pending, hierarchy, dupes, hist, sources, recent] =
        await Promise.all([
          querySnowflake("SELECT COUNT(*) as cnt FROM MDM.MASTER.GOLDEN_PAYORS"),
          querySnowflake("SELECT COUNT(*) as cnt FROM MDM.STAGING.STG_PAYORS_UNIONED"),
          querySnowflake("SELECT COUNT(*) as cnt FROM MDM.MATCH.MATCH_CANDIDATES WHERE FINAL_DECISION = 'review'"),
          querySnowflake("SELECT COUNT(*) as cnt FROM MDM.MASTER.PAYOR_HIERARCHY"),
          querySnowflake("SELECT COUNT(*) as cnt FROM MDM.MATCH.MATCH_CANDIDATES WHERE FINAL_DECISION = 'review' AND SOURCE_A_ID = SOURCE_B_ID"),
          querySnowflake("SELECT WIDTH_BUCKET(COMPOSITE_SCORE, 0, 1, 10) as bucket, COUNT(*) as cnt FROM MDM.MATCH.MATCH_CANDIDATES GROUP BY bucket ORDER BY bucket"),
          querySnowflake("SELECT SOURCE_SYSTEM, COUNT(*) as cnt FROM MDM.STAGING.STG_PAYORS_UNIONED GROUP BY SOURCE_SYSTEM"),
          querySnowflake("SELECT * FROM MDM.AUDIT.MDM_CHANGE_LOG ORDER BY CHANGED_AT DESC LIMIT 10"),
        ])

      setKpis({
        goldenCount: golden[0]?.CNT ?? 0,
        sourceCount: source[0]?.CNT ?? 0,
        pendingReview: pending[0]?.CNT ?? 0,
        hierarchyCount: hierarchy[0]?.CNT ?? 0,
        duplicatePairs: dupes[0]?.CNT ?? 0,
      })

      setHistogram(
        hist.map((r: any) => ({
          bucket: `${((r.BUCKET - 1) * 10)}–${r.BUCKET * 10}%`,
          count: r.CNT,
        }))
      )

      setSourcePie(
        sources.map((r: any) => ({ name: r.SOURCE_SYSTEM, value: r.CNT }))
      )

      setActivity(recent)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  async function handleDbtRun() {
    setDbtRunning(true)
    setDbtOutput(null)
    try {
      const output = await triggerDbtRun()
      setDbtOutput(output)
      loadData()
    } catch (e: any) {
      setDbtOutput(`Error: ${e.message}`)
    } finally {
      setDbtRunning(false)
    }
  }

  if (loading) return <LoadingSkeleton />
  if (error) return <ErrorState message={error} onRetry={loadData} />

  const kpiCards = [
    { label: 'Golden Records', value: kpis!.goldenCount, icon: Crown, color: 'text-yellow-400' },
    { label: 'Source Records', value: kpis!.sourceCount, icon: Database, color: 'text-blue-400' },
    { label: 'Pending Review', value: kpis!.pendingReview, icon: AlertCircle, color: 'text-orange-400' },
    { label: 'Hierarchy Links', value: kpis!.hierarchyCount, icon: Network, color: 'text-purple-400' },
    { label: 'Duplicate Pairs', value: kpis!.duplicatePairs, icon: Copy, color: 'text-red-400' },
  ]

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Dashboard</h2>
        <button
          onClick={handleDbtRun}
          disabled={dbtRunning}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
        >
          <RefreshCw size={16} className={dbtRunning ? 'animate-spin' : ''} />
          {dbtRunning ? 'Rebuilding...' : 'Rebuild Golden Records'}
        </button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4">
        {kpiCards.map(({ label, value, icon: Icon, color }) => (
          <div key={label} className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              <Icon size={16} className={color} />
              <span className="text-xs text-gray-400">{label}</span>
            </div>
            <p className="text-2xl font-bold">{value.toLocaleString()}</p>
          </div>
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-medium text-gray-400 mb-4">Match Score Distribution</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={histogram}>
              <XAxis dataKey="bucket" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-medium text-gray-400 mb-4">Records by Source</h3>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie
                data={sourcePie}
                cx="50%"
                cy="50%"
                outerRadius={90}
                dataKey="value"
                label={(props: any) => `${props.name} (${((props.percent ?? 0) * 100).toFixed(0)}%)`}
              >
                {sourcePie.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* dbt output */}
      {dbtOutput && (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-medium text-gray-400 mb-2">dbt Run Output</h3>
          <pre className="text-xs text-gray-300 whitespace-pre-wrap max-h-60 overflow-auto font-mono">
            {dbtOutput}
          </pre>
        </div>
      )}

      {/* Activity Feed */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Recent Activity</h3>
        {activity.length === 0 ? (
          <p className="text-sm text-gray-500">No recent activity</p>
        ) : (
          <div className="space-y-2">
            {activity.map((row: any, i: number) => (
              <div key={i} className="flex items-center justify-between text-sm py-2 border-b border-gray-800 last:border-0">
                <div className="flex items-center gap-3">
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-800 text-gray-300">
                    {row.ACTION}
                  </span>
                  <span className="text-gray-300">
                    {row.ENTITY_TYPE} — {row.ENTITY_ID}
                  </span>
                </div>
                <div className="flex items-center gap-4 text-gray-500 text-xs">
                  <span>{row.CHANGED_BY}</span>
                  <span>{row.CHANGED_AT ? new Date(row.CHANGED_AT).toLocaleString() : ''}</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function LoadingSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gray-800 rounded w-40" />
      <div className="grid grid-cols-5 gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="h-24 bg-gray-900 border border-gray-800 rounded-lg" />
        ))}
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div className="h-72 bg-gray-900 border border-gray-800 rounded-lg" />
        <div className="h-72 bg-gray-900 border border-gray-800 rounded-lg" />
      </div>
    </div>
  )
}

function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center h-96 text-gray-400">
      <AlertCircle size={48} className="mb-4 text-red-500" />
      <p className="text-lg mb-2">Failed to load dashboard</p>
      <p className="text-sm text-gray-500 mb-4">{message}</p>
      <button onClick={onRetry} className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-sm font-medium">
        Retry
      </button>
    </div>
  )
}
