import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { Loader2, AlertCircle } from 'lucide-react'
import { querySnowflake } from '../api'

const FIELD_COLORS: Record<string, string> = {
  name_pct: '#3b82f6',
  tax_pct: '#10b981',
  npi_pct: '#f59e0b',
  addr_pct: '#8b5cf6',
  phone_pct: '#ec4899',
}

export default function DataQuality() {
  const [completeness, setCompleteness] = useState<any[]>([])
  const [matchRate, setMatchRate] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadData()
  }, [])

  async function loadData() {
    setLoading(true)
    setError(null)
    try {
      const [comp, matchR] = await Promise.all([
        querySnowflake(`
          SELECT SOURCE_SYSTEM,
            COUNT(*) as total,
            SUM(CASE WHEN PAYOR_NAME IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as NAME_PCT,
            SUM(CASE WHEN TAX_ID IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as TAX_PCT,
            SUM(CASE WHEN NPI IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as NPI_PCT,
            SUM(CASE WHEN ADDRESS_LINE1 IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ADDR_PCT,
            SUM(CASE WHEN PHONE IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as PHONE_PCT
          FROM MDM.STAGING.STG_PAYORS_UNIONED
          GROUP BY SOURCE_SYSTEM
        `),
        querySnowflake(`
          SELECT SOURCE_A_SYSTEM, SOURCE_B_SYSTEM, COUNT(*) as PAIRS,
            SUM(CASE WHEN FINAL_DECISION IN ('auto_match','match_confirmed') THEN 1 ELSE 0 END) as MATCHES
          FROM MDM.MATCH.MATCH_CANDIDATES
          GROUP BY SOURCE_A_SYSTEM, SOURCE_B_SYSTEM
        `),
      ])

      setCompleteness(
        comp.map((r: any) => ({
          source: r.SOURCE_SYSTEM,
          total: r.TOTAL,
          name_pct: parseFloat(r.NAME_PCT).toFixed(1),
          tax_pct: parseFloat(r.TAX_PCT).toFixed(1),
          npi_pct: parseFloat(r.NPI_PCT).toFixed(1),
          addr_pct: parseFloat(r.ADDR_PCT).toFixed(1),
          phone_pct: parseFloat(r.PHONE_PCT).toFixed(1),
        }))
      )

      setMatchRate(
        matchR.map((r: any) => ({
          pair: `${r.SOURCE_A_SYSTEM} / ${r.SOURCE_B_SYSTEM}`,
          pairs: r.PAIRS,
          matches: r.MATCHES,
          rate: r.PAIRS > 0 ? ((r.MATCHES / r.PAIRS) * 100).toFixed(1) : '0',
        }))
      )
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

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
    <div className="space-y-6">
      <h2 className="text-2xl font-bold">Data Quality</h2>

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <p className="text-xs text-gray-400 mb-1">Source Systems</p>
          <p className="text-2xl font-bold">{completeness.length}</p>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <p className="text-xs text-gray-400 mb-1">Total Source Records</p>
          <p className="text-2xl font-bold">
            {completeness.reduce((sum, r) => sum + (r.total || 0), 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <p className="text-xs text-gray-400 mb-1">Source Pairs Compared</p>
          <p className="text-2xl font-bold">{matchRate.length}</p>
        </div>
      </div>

      {/* Completeness chart */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Field Completeness by Source (%)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={completeness}>
            <XAxis dataKey="source" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} domain={[0, 100]} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f3f4f6' }}
            />
            <Legend />
            <Bar dataKey="name_pct" name="Name" fill={FIELD_COLORS.name_pct} radius={[2, 2, 0, 0]} />
            <Bar dataKey="tax_pct" name="Tax ID" fill={FIELD_COLORS.tax_pct} radius={[2, 2, 0, 0]} />
            <Bar dataKey="npi_pct" name="NPI" fill={FIELD_COLORS.npi_pct} radius={[2, 2, 0, 0]} />
            <Bar dataKey="addr_pct" name="Address" fill={FIELD_COLORS.addr_pct} radius={[2, 2, 0, 0]} />
            <Bar dataKey="phone_pct" name="Phone" fill={FIELD_COLORS.phone_pct} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Completeness detail table */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Source</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Records</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Name</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Tax ID</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">NPI</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Address</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Phone</th>
            </tr>
          </thead>
          <tbody>
            {completeness.map((r, i) => (
              <tr key={r.source} className={`border-b border-gray-800 ${i % 2 === 0 ? 'bg-gray-900' : 'bg-gray-950'}`}>
                <td className="px-4 py-2 font-medium">{r.source}</td>
                <td className="px-4 py-2 text-gray-300">{r.total?.toLocaleString()}</td>
                <td className="px-4 py-2"><PctBadge value={r.name_pct} /></td>
                <td className="px-4 py-2"><PctBadge value={r.tax_pct} /></td>
                <td className="px-4 py-2"><PctBadge value={r.npi_pct} /></td>
                <td className="px-4 py-2"><PctBadge value={r.addr_pct} /></td>
                <td className="px-4 py-2"><PctBadge value={r.phone_pct} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Match rate chart */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Match Rate by Source Pair</h3>
        {matchRate.length === 0 ? (
          <p className="text-sm text-gray-500">No match pairs found</p>
        ) : (
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={matchRate}>
              <XAxis dataKey="pair" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Bar dataKey="pairs" name="Total Pairs" fill="#6b7280" radius={[2, 2, 0, 0]} />
              <Bar dataKey="matches" name="Matches" fill="#10b981" radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Match rate table */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Source Pair</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Total Pairs</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Matches</th>
              <th className="px-4 py-3 text-left text-gray-400 font-medium">Match Rate</th>
            </tr>
          </thead>
          <tbody>
            {matchRate.map((r, i) => (
              <tr key={r.pair} className={`border-b border-gray-800 ${i % 2 === 0 ? 'bg-gray-900' : 'bg-gray-950'}`}>
                <td className="px-4 py-2 font-medium">{r.pair}</td>
                <td className="px-4 py-2 text-gray-300">{r.pairs}</td>
                <td className="px-4 py-2 text-gray-300">{r.matches}</td>
                <td className="px-4 py-2"><PctBadge value={r.rate} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function PctBadge({ value }: { value: string | number }) {
  const num = parseFloat(String(value))
  const color =
    num >= 90 ? 'bg-green-900/30 text-green-400' :
    num >= 70 ? 'bg-yellow-900/30 text-yellow-400' :
    'bg-red-900/30 text-red-400'
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {num.toFixed(1)}%
    </span>
  )
}
