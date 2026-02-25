import { useEffect, useState } from 'react'
import { Routes, Route, NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  GitCompareArrows,
  Crown,
  Network,
  ScrollText,
  ShieldCheck,
} from 'lucide-react'
import { checkConnection } from './api'
import Dashboard from './pages/Dashboard'
import MatchReview from './pages/MatchReview'
import GoldenRecords from './pages/GoldenRecords'
import HierarchyManager from './pages/HierarchyManager'
import AuditTrail from './pages/AuditTrail'
import DataQuality from './pages/DataQuality'

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/match-review', label: 'Match Review', icon: GitCompareArrows },
  { to: '/golden-records', label: 'Golden Records', icon: Crown },
  { to: '/hierarchy', label: 'Hierarchy', icon: Network },
  { to: '/audit', label: 'Audit Trail', icon: ScrollText },
  { to: '/data-quality', label: 'Data Quality', icon: ShieldCheck },
]

export default function App() {
  const [connected, setConnected] = useState<boolean | null>(null)

  useEffect(() => {
    checkConnection().then(setConnected).catch(() => setConnected(false))
    const interval = setInterval(() => {
      checkConnection().then(setConnected).catch(() => setConnected(false))
    }, 30000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="flex h-screen bg-gray-950 text-gray-100">
      {/* Sidebar */}
      <aside className="w-60 flex flex-col border-r border-gray-800 bg-gray-900">
        <div className="px-5 py-5 border-b border-gray-800">
          <h1 className="text-xl font-bold tracking-tight">Payor MDM</h1>
          <p className="text-xs text-gray-500 mt-0.5">Stewardship Console</p>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {navItems.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-blue-600/20 text-blue-400'
                    : 'text-gray-400 hover:text-gray-100 hover:bg-gray-800'
                }`
              }
            >
              <Icon size={18} />
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="px-5 py-4 border-t border-gray-800">
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <span
              className={`w-2 h-2 rounded-full ${
                connected === null
                  ? 'bg-yellow-500'
                  : connected
                    ? 'bg-green-500'
                    : 'bg-red-500'
              }`}
            />
            {connected === null
              ? 'Connecting...'
              : connected
                ? 'Snowflake connected'
                : 'Disconnected'}
          </div>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/match-review" element={<MatchReview />} />
          <Route path="/golden-records" element={<GoldenRecords />} />
          <Route path="/hierarchy" element={<HierarchyManager />} />
          <Route path="/audit" element={<AuditTrail />} />
          <Route path="/data-quality" element={<DataQuality />} />
        </Routes>
      </main>
    </div>
  )
}
