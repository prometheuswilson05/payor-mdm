import { useEffect, useState } from 'react'
import { ChevronDown, ChevronRight, Plus, Loader2, AlertCircle } from 'lucide-react'
import { querySnowflake, writeSnowflake } from '../api'

interface HierarchyRow {
  PARENT_PAYOR_ID: string
  CHILD_PAYOR_ID: string
  RELATIONSHIP_TYPE: string
  STEWARD_CONFIRMED: boolean
  PARENT_NAME: string
  CHILD_NAME: string
}

interface TreeNode {
  id: string
  name: string
  relationshipType?: string
  confirmed?: boolean
  children: TreeNode[]
}

interface GoldenOption {
  MASTER_PAYOR_ID: string
  GOLDEN_PAYOR_NAME: string
}

export default function HierarchyManager() {
  const [rows, setRows] = useState<HierarchyRow[]>([])
  const [unassigned, setUnassigned] = useState<GoldenOption[]>([])
  const [allGolden, setAllGolden] = useState<GoldenOption[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showForm, setShowForm] = useState(false)
  const [formParent, setFormParent] = useState('')
  const [formChild, setFormChild] = useState('')
  const [formType, setFormType] = useState('subsidiary')
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    loadData()
  }, [])

  async function loadData() {
    setLoading(true)
    setError(null)
    try {
      const [hier, unass, golden] = await Promise.all([
        querySnowflake(`
          SELECT h.*, p.GOLDEN_PAYOR_NAME as PARENT_NAME, c.GOLDEN_PAYOR_NAME as CHILD_NAME
          FROM MDM.MASTER.PAYOR_HIERARCHY h
          JOIN MDM.MASTER.GOLDEN_PAYORS p ON h.PARENT_PAYOR_ID = p.MASTER_PAYOR_ID
          JOIN MDM.MASTER.GOLDEN_PAYORS c ON h.CHILD_PAYOR_ID = c.MASTER_PAYOR_ID
        `),
        querySnowflake(`
          SELECT * FROM MDM.MASTER.GOLDEN_PAYORS
          WHERE MASTER_PAYOR_ID NOT IN (SELECT CHILD_PAYOR_ID FROM MDM.MASTER.PAYOR_HIERARCHY)
          AND MASTER_PAYOR_ID NOT IN (SELECT PARENT_PAYOR_ID FROM MDM.MASTER.PAYOR_HIERARCHY)
        `),
        querySnowflake(`SELECT MASTER_PAYOR_ID, GOLDEN_PAYOR_NAME FROM MDM.MASTER.GOLDEN_PAYORS ORDER BY GOLDEN_PAYOR_NAME`),
      ])
      setRows(hier)
      setUnassigned(unass)
      setAllGolden(golden)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  function buildTree(rows: HierarchyRow[]): TreeNode[] {
    const childIds = new Set(rows.map((r) => r.CHILD_PAYOR_ID))
    const rootIds = new Set<string>()
    rows.forEach((r) => {
      if (!childIds.has(r.PARENT_PAYOR_ID)) rootIds.add(r.PARENT_PAYOR_ID)
    })

    const childrenMap = new Map<string, HierarchyRow[]>()
    rows.forEach((r) => {
      const existing = childrenMap.get(r.PARENT_PAYOR_ID) || []
      existing.push(r)
      childrenMap.set(r.PARENT_PAYOR_ID, existing)
    })

    function buildNode(id: string, name: string, relType?: string, confirmed?: boolean): TreeNode {
      const kids = childrenMap.get(id) || []
      return {
        id,
        name,
        relationshipType: relType,
        confirmed,
        children: kids.map((k) =>
          buildNode(k.CHILD_PAYOR_ID, k.CHILD_NAME, k.RELATIONSHIP_TYPE, k.STEWARD_CONFIRMED)
        ),
      }
    }

    const nameMap = new Map<string, string>()
    rows.forEach((r) => {
      nameMap.set(r.PARENT_PAYOR_ID, r.PARENT_NAME)
      nameMap.set(r.CHILD_PAYOR_ID, r.CHILD_NAME)
    })

    return Array.from(rootIds).map((id) => buildNode(id, nameMap.get(id) || id))
  }

  async function handleAddRelationship() {
    if (!formParent || !formChild || formParent === formChild) return
    setSaving(true)
    try {
      await writeSnowflake([
        `INSERT INTO MDM.MASTER.PAYOR_HIERARCHY (PARENT_PAYOR_ID, CHILD_PAYOR_ID, RELATIONSHIP_TYPE, STEWARD_CONFIRMED, CONFIRMED_BY, CONFIRMED_AT) VALUES ('${formParent}', '${formChild}', '${formType}', TRUE, 'steward', CURRENT_TIMESTAMP())`,
      ])
      setShowForm(false)
      setFormParent('')
      setFormChild('')
      setFormType('subsidiary')
      await loadData()
    } catch (e: any) {
      setError(e.message)
    } finally {
      setSaving(false)
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

  const tree = buildTree(rows)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Hierarchy Manager</h2>
        <button
          onClick={() => setShowForm(!showForm)}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-sm font-medium"
        >
          <Plus size={16} />
          Add Relationship
        </button>
      </div>

      {/* Add form */}
      {showForm && (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 space-y-3">
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-gray-400 mb-1">Parent</label>
              <select
                value={formParent}
                onChange={(e) => setFormParent(e.target.value)}
                className="w-full bg-gray-950 border border-gray-800 rounded px-3 py-2 text-sm text-gray-100"
              >
                <option value="">Select parent...</option>
                {allGolden.map((g) => (
                  <option key={g.MASTER_PAYOR_ID} value={g.MASTER_PAYOR_ID}>
                    {g.GOLDEN_PAYOR_NAME}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Child</label>
              <select
                value={formChild}
                onChange={(e) => setFormChild(e.target.value)}
                className="w-full bg-gray-950 border border-gray-800 rounded px-3 py-2 text-sm text-gray-100"
              >
                <option value="">Select child...</option>
                {allGolden.map((g) => (
                  <option key={g.MASTER_PAYOR_ID} value={g.MASTER_PAYOR_ID}>
                    {g.GOLDEN_PAYOR_NAME}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Relationship Type</label>
              <select
                value={formType}
                onChange={(e) => setFormType(e.target.value)}
                className="w-full bg-gray-950 border border-gray-800 rounded px-3 py-2 text-sm text-gray-100"
              >
                <option value="subsidiary">Subsidiary</option>
                <option value="division">Division</option>
                <option value="brand">Brand</option>
                <option value="affiliate">Affiliate</option>
              </select>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleAddRelationship}
              disabled={saving || !formParent || !formChild}
              className="px-4 py-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-sm font-medium"
            >
              {saving ? 'Saving...' : 'Save'}
            </button>
            <button
              onClick={() => setShowForm(false)}
              className="px-4 py-2 bg-gray-800 hover:bg-gray-700 rounded-lg text-sm"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Tree view */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Hierarchy Tree</h3>
        {tree.length === 0 ? (
          <p className="text-sm text-gray-500">No hierarchy relationships found</p>
        ) : (
          <div className="space-y-1">
            {tree.map((node) => (
              <TreeNodeView key={node.id} node={node} depth={0} />
            ))}
          </div>
        )}
      </div>

      {/* Unassigned */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-400 mb-4">
          Unassigned Records ({unassigned.length})
        </h3>
        {unassigned.length === 0 ? (
          <p className="text-sm text-gray-500">All records have hierarchy assignments</p>
        ) : (
          <div className="grid grid-cols-3 gap-2">
            {unassigned.map((r: any) => (
              <div key={r.MASTER_PAYOR_ID} className="text-sm bg-gray-950 rounded px-3 py-2 text-gray-300">
                {r.GOLDEN_PAYOR_NAME}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function TreeNodeView({ node, depth }: { node: TreeNode; depth: number }) {
  const [expanded, setExpanded] = useState(true)
  const hasChildren = node.children.length > 0

  return (
    <div>
      <div
        className="flex items-center gap-2 py-1.5 px-2 rounded hover:bg-gray-800 cursor-pointer"
        style={{ paddingLeft: `${depth * 24 + 8}px` }}
        onClick={() => setExpanded(!expanded)}
      >
        {hasChildren ? (
          expanded ? <ChevronDown size={14} className="text-gray-500" /> : <ChevronRight size={14} className="text-gray-500" />
        ) : (
          <span className="w-3.5" />
        )}
        <span
          className={`w-2 h-2 rounded-full ${node.confirmed === false ? 'border border-dashed border-gray-500' : 'bg-blue-500'}`}
        />
        <span className="text-sm text-gray-100">{node.name}</span>
        {node.relationshipType && (
          <span className="px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-400">
            {node.relationshipType}
          </span>
        )}
        {node.confirmed === false && (
          <span className="text-xs text-yellow-500">unconfirmed</span>
        )}
      </div>
      {expanded && hasChildren && (
        <div className={depth === 0 ? 'border-l border-gray-800 ml-5' : 'border-l border-gray-800 ml-5'}>
          {node.children.map((child) => (
            <TreeNodeView key={child.id} node={child} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  )
}
