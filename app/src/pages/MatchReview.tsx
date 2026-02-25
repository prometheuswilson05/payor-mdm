import { useEffect, useState, useCallback } from 'react'
import { Check, X, SkipForward, AlertCircle, Loader2 } from 'lucide-react'
import { querySnowflake, writeSnowflake } from '../api'

const COMPARE_FIELDS = [
  'PAYOR_NAME', 'TAX_ID', 'NPI', 'ADDRESS_LINE1', 'CITY',
  'STATE_CODE', 'ZIP_CODE', 'PHONE', 'PAYOR_TYPE', 'STATUS',
]

const SCORE_FIELDS = [
  'NAME_SCORE', 'TAX_ID_SCORE', 'NPI_SCORE', 'ADDRESS_SCORE', 'PHONE_SCORE', 'COMPOSITE_SCORE',
]

function fieldMatchClass(valA: string | null, valB: string | null): string {
  if (!valA && !valB) return 'bg-gray-800/50'
  if (!valA || !valB) return 'bg-red-900/30'
  if (valA === valB) return 'bg-green-900/30'
  if (valA.substring(0, 3).toUpperCase() === valB.substring(0, 3).toUpperCase()) return 'bg-yellow-900/30'
  return 'bg-red-900/30'
}

export default function MatchReview() {
  const [candidates, setCandidates] = useState<any[]>([])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [sourceA, setSourceA] = useState<any>(null)
  const [sourceB, setSourceB] = useState<any>(null)
  const [notes, setNotes] = useState('')
  const [loading, setLoading] = useState(true)
  const [loadingDetail, setLoadingDetail] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    loadCandidates()
  }, [])

  async function loadCandidates() {
    setLoading(true)
    setError(null)
    try {
      const rows = await querySnowflake(
        "SELECT * FROM MDM.MATCH.MATCH_CANDIDATES WHERE FINAL_DECISION = 'review' ORDER BY COMPOSITE_SCORE DESC"
      )
      setCandidates(rows)
      setCurrentIndex(0)
      if (rows.length > 0) {
        await loadSourceRecords(rows[0])
      }
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  async function loadSourceRecords(candidate: any) {
    setLoadingDetail(true)
    try {
      const [a, b] = await Promise.all([
        querySnowflake(`SELECT * FROM MDM.STAGING.STG_PAYORS_UNIONED WHERE RECORD_ID = '${candidate.SOURCE_A_ID}'`),
        querySnowflake(`SELECT * FROM MDM.STAGING.STG_PAYORS_UNIONED WHERE RECORD_ID = '${candidate.SOURCE_B_ID}'`),
      ])
      setSourceA(a[0] ?? null)
      setSourceB(b[0] ?? null)
    } catch {
      setSourceA(null)
      setSourceB(null)
    } finally {
      setLoadingDetail(false)
    }
  }

  const handleDecision = useCallback(async (decision: 'match_confirmed' | 'match_rejected') => {
    if (saving || candidates.length === 0) return
    const candidate = candidates[currentIndex]
    if (!candidate) return

    setSaving(true)
    try {
      const escapedNotes = notes.replace(/'/g, "''")
      await writeSnowflake([
        `UPDATE MDM.MATCH.MATCH_CANDIDATES SET STEWARD_DECISION = '${decision}', FINAL_DECISION = '${decision}', REVIEWED_BY = 'steward', REVIEWED_AT = CURRENT_TIMESTAMP() WHERE CANDIDATE_ID = '${candidate.CANDIDATE_ID}'`,
        `INSERT INTO MDM.AUDIT.MDM_CHANGE_LOG (ENTITY_TYPE, ENTITY_ID, ACTION, CHANGED_BY, CHANGE_DETAILS) VALUES ('match_candidate', '${candidate.CANDIDATE_ID}', '${decision}', 'steward', '${escapedNotes}')`,
      ])

      const updated = candidates.filter((_, i) => i !== currentIndex)
      setCandidates(updated)
      setNotes('')

      const nextIndex = Math.min(currentIndex, updated.length - 1)
      setCurrentIndex(nextIndex)
      if (updated.length > 0 && updated[nextIndex]) {
        await loadSourceRecords(updated[nextIndex])
      }
    } catch (e: any) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }, [saving, candidates, currentIndex, notes])

  const handleSkip = useCallback(() => {
    if (candidates.length === 0) return
    const next = (currentIndex + 1) % candidates.length
    setCurrentIndex(next)
    setNotes('')
    loadSourceRecords(candidates[next])
  }, [candidates, currentIndex])

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.target instanceof HTMLTextAreaElement || e.target instanceof HTMLInputElement) return
      if (e.key === 'y' || e.key === 'Y') handleDecision('match_confirmed')
      else if (e.key === 'n' || e.key === 'N') handleDecision('match_rejected')
      else if (e.key === 's' || e.key === 'S') handleSkip()
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [handleDecision, handleSkip])

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
        <button onClick={loadCandidates} className="px-4 py-2 bg-blue-600 rounded-lg text-sm">Retry</button>
      </div>
    )
  }

  if (candidates.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-96 text-gray-400">
        <Check size={48} className="mb-4 text-green-500" />
        <p className="text-lg">All caught up!</p>
        <p className="text-sm text-gray-500">No pending candidates to review.</p>
      </div>
    )
  }

  const candidate = candidates[currentIndex]
  const totalOriginal = candidates.length
  const progressPct = totalOriginal > 0 ? ((currentIndex + 1) / totalOriginal) * 100 : 0

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Match Review</h2>
        <span className="text-sm text-gray-400">
          Reviewing {currentIndex + 1} of {totalOriginal} pending candidates
        </span>
      </div>

      {/* Progress bar */}
      <div className="w-full bg-gray-800 rounded-full h-2">
        <div className="bg-blue-600 h-2 rounded-full transition-all" style={{ width: `${progressPct}%` }} />
      </div>

      {/* Score badges */}
      <div className="flex gap-3 flex-wrap">
        {SCORE_FIELDS.map((field) => {
          const val = candidate[field]
          if (val == null) return null
          const pct = (parseFloat(val) * 100).toFixed(0)
          const colorClass =
            parseFloat(val) >= 0.8 ? 'bg-green-900/50 text-green-400' :
            parseFloat(val) >= 0.5 ? 'bg-yellow-900/50 text-yellow-400' :
            'bg-red-900/50 text-red-400'
          return (
            <span key={field} className={`px-3 py-1 rounded-full text-xs font-medium ${colorClass}`}>
              {field.replace('_SCORE', '')}: {pct}%
            </span>
          )
        })}
      </div>

      {/* Side-by-side comparison */}
      {loadingDetail ? (
        <div className="flex justify-center py-12">
          <Loader2 size={24} className="animate-spin text-gray-500" />
        </div>
      ) : (
        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800">
                <th className="px-4 py-3 text-left text-gray-400 font-medium w-36">Field</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">
                  Source A
                  {sourceA && <span className="ml-2 text-xs text-gray-500">({sourceA.SOURCE_SYSTEM})</span>}
                </th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">
                  Source B
                  {sourceB && <span className="ml-2 text-xs text-gray-500">({sourceB.SOURCE_SYSTEM})</span>}
                </th>
              </tr>
            </thead>
            <tbody>
              {COMPARE_FIELDS.map((field) => {
                const valA = sourceA?.[field] ?? null
                const valB = sourceB?.[field] ?? null
                const bgClass = fieldMatchClass(
                  valA ? String(valA) : null,
                  valB ? String(valB) : null,
                )
                return (
                  <tr key={field} className={`border-b border-gray-800 ${bgClass}`}>
                    <td className="px-4 py-2 font-medium text-gray-400">{field}</td>
                    <td className="px-4 py-2 text-gray-100">{valA ?? <span className="text-gray-600 italic">null</span>}</td>
                    <td className="px-4 py-2 text-gray-100">{valB ?? <span className="text-gray-600 italic">null</span>}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Notes */}
      <textarea
        value={notes}
        onChange={(e) => setNotes(e.target.value)}
        placeholder="Optional notes about this decision..."
        className="w-full bg-gray-900 border border-gray-800 rounded-lg px-4 py-3 text-sm text-gray-100 placeholder-gray-600 resize-none focus:outline-none focus:border-gray-700"
        rows={2}
      />

      {/* Actions */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => handleDecision('match_confirmed')}
          disabled={saving}
          className="flex items-center gap-2 px-5 py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
        >
          <Check size={16} />
          Confirm Match
          <kbd className="ml-2 px-1.5 py-0.5 bg-green-800 rounded text-xs">Y</kbd>
        </button>
        <button
          onClick={() => handleDecision('match_rejected')}
          disabled={saving}
          className="flex items-center gap-2 px-5 py-2.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
        >
          <X size={16} />
          Not a Match
          <kbd className="ml-2 px-1.5 py-0.5 bg-red-800 rounded text-xs">N</kbd>
        </button>
        <button
          onClick={handleSkip}
          disabled={saving}
          className="flex items-center gap-2 px-5 py-2.5 bg-gray-700 hover:bg-gray-600 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
        >
          <SkipForward size={16} />
          Skip
          <kbd className="ml-2 px-1.5 py-0.5 bg-gray-800 rounded text-xs">S</kbd>
        </button>
        {saving && <Loader2 size={16} className="animate-spin text-blue-500 ml-2" />}
      </div>
    </div>
  )
}
