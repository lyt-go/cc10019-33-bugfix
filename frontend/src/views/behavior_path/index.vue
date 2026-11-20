<template>
  <section class="page" data-module="behavior_path">
    <header class="page-head">
      <div><h2>行为路径</h2><p class="page-desc">基于会话切割产物，按时间窗口切分有效事件范围构建路径 Sankey；窗口外晚到目标不计入转化。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记路径模型</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <div class="stat-row" v-if="result">
      <article class="stat-card" data-testid="kpi-card"><span class="stat-label">有效会话</span><strong class="stat-value">{{ result.total_sessions }}</strong></article>
      <article class="stat-card" data-testid="kpi-card"><span class="stat-label">转化会话</span><strong class="stat-value">{{ result.total_conversions }}</strong></article>
      <article class="stat-card" data-testid="kpi-card"><span class="stat-label">未转化会话</span><strong class="stat-value">{{ result.unconverted_sessions }}</strong></article>
      <article class="stat-card" data-testid="kpi-card"><span class="stat-label">晚到目标排除</span><strong class="stat-value">{{ result.late_target_excluded }}</strong></article>
    </div>

    <div v-if="result?.stale" class="error-text" data-testid="stale-banner">
      当前图为过期产物：{{ result.stale_reason }}。会话切割未产出可确认的新图，请勿按此图决策。
    </div>

    <div class="filter-bar">
      <label class="filter-item"><span>时间窗口（秒）</span>
        <input data-testid="time-window-input" type="number" min="1" v-model.number="timeWindow" />
      </label>
      <button class="btn" type="button" data-testid="apply-window" @click="applyTimeWindow">应用窗口并失效旧图</button>
      <button class="btn primary" type="button" data-testid="build-path" @click="buildGraph">重新构图</button>
      <button class="btn" type="button" data-testid="load-common" @click="loadCommonPaths">查询常见路径</button>
      <button class="btn ghost" type="button" data-testid="load-details" @click="loadDetails">路径明细</button>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </div>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to+(edge.model_id||'')">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in dagNodes" :key="node.kind + String(node.id)" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="node.kind==='model' && selectNode(node)" @mousedown="node.kind==='model' && startDrag($event, node)">
          <rect :x="nodePos[node.posKey]?.x - 60" :y="nodePos[node.posKey]?.y - 20" width="120" height="40" rx="6" :fill="node.kind==='event' ? '#e0f2fe' : nodeColor(node.status)"/>
          <text :x="nodePos[node.posKey]?.x" :y="nodePos[node.posKey]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>

    <div v-if="commonPaths.length" class="analysis-bar" data-testid="common-paths-panel">
      <h3>常见路径（含未转化片段，min_count={{ minCount }}）</h3>
      <table class="data-table">
        <thead><tr><th>路径</th><th>次数</th><th>转化</th><th>转化会话</th><th>未转化会话</th></tr></thead>
        <tbody>
          <tr v-for="p in commonPaths" :key="p.path.join('>')" :data-converted="String(p.converted)">
            <td>{{ p.path.join(' → ') }}</td><td>{{ p.count }}</td>
            <td>{{ p.converted ? '是' : '否（未转化片段）' }}</td>
            <td>{{ p.converted_sessions }}</td><td>{{ p.unconverted_sessions }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="pathDetails.length" data-testid="path-details-panel">
      <h3>路径明细（每条目标边追溯到窗口内一笔事件；晚到排除 {{ lateSummary }}）</h3>
      <table class="data-table">
        <thead><tr><th>会话</th><th>用户</th><th>边</th><th>事件时间</th><th>窗口范围</th><th>窗口内</th><th>转化</th><th>排除原因</th></tr></thead>
        <tbody>
          <tr v-for="(d, i) in pathDetails.slice(0, 50)" :key="i" :data-in-window="String(d.in_window)">
            <td>{{ d.session_id }}</td><td>{{ d.user_id }}</td>
            <td>{{ d.source_event }} → {{ d.target_event }}</td>
            <td>{{ d.target_timestamp }}</td>
            <td>[{{ d.window_start }}, {{ d.window_end }})</td>
            <td>{{ d.in_window ? '是' : '否' }}</td>
            <td>{{ d.converted ? '是' : '否' }}</td>
            <td>{{ d.excluded_reason || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记路径模型</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ dagNodes.length }} 个节点 · {{ graph.edges.length }} 条路径边</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/behavior-path'
const columns = ["模型编号", "起始事件", "目标事件", "时间窗口", "路径深度", "会话标识", "路径过滤", "分析区间"]
const actions = ["提交校验", "标记生效", "停用模型"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已生效':'#d1fae5','已启用':'#d1fae5','已停用':'#fee2e2','已完成':'#dbeafe' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number|string,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const timeWindow = ref<number>(3600)
const result = ref<any>(null)
const commonPaths = ref<any[]>([])
const pathDetails = ref<any[]>([])
const minCount = 2
const lateSummary = computed(() => result.value?.late_target_excluded ?? 0)
// DAG 展示节点：路径模型节点 + 构图产物中的事件节点（来自 Sankey 边）
const dagNodes = computed(() => {
  const models = graph.nodes.map((n: any) => ({ ...n, kind: 'model', posKey: n.id }))
  const eventNames = Array.from(new Set(graph.edges.flatMap((e: any) => [e.from, e.to])))
  const events = eventNames.map((name: string) => ({
    id: `event:${name}`, name, label: name, kind: 'event', posKey: name, status: '事件',
  }))
  return [...models, ...events]
})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)

function layout(nodes: any[]) {
  const eventNames = Array.from(new Set(graph.edges.flatMap((e: any) => [e.from, e.to])))
  const w = 800
  const h = Math.max(400, (nodes.length + eventNames.length) * 70)
  svgW.value = w; svgH.value = h
  const pos: Record<number|string,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 90 } })
  // 事件节点（来自 Sankey 边）单独布局
  eventNames.forEach((name: string, i: number) => {
    pos[name] = { x: 100 + (i % 6) * 120, y: h - 80 - Math.floor(i / 6) * 70 }
  })
  nodePos.value = pos
}
function selectNode(node: any) {
  if (selectedNode.value?.id === node.id) {
    selectedNode.value = null
    return
  }
  selectedNode.value = node
  if (node.kind === 'model' && node.model_id) {
    void loadResult(node.model_id)
  }
}
function startDrag(e: MouseEvent, node: any) {
  dragNode = node
  const onMove = (ev: MouseEvent) => { if (!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }
  const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }
  document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp)
}
async function reload() {
  errorMessage.value = ''
  try {
    const res = await request(ENDPOINT); const data = await res.json()
    graph.nodes = data.nodes || []; graph.edges = data.edges || []
    layout(graph.nodes)
  } catch (e: any) { errorMessage.value = e.message || '加载失败' }
}
async function buildGraph() {
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${selectedNode.value?.model_id || 'BP-001'}/build`, { method: 'POST' })
    if (!res.ok) {
      const detail = (await res.json().catch(() => ({})))?.detail || '构图失败'
      throw new Error(detail)
    }
    const payload = await res.json()
    result.value = payload.entry
    await reload()
  } catch (e: any) { errorMessage.value = e.message || '构图失败' }
}
async function applyTimeWindow() {
  errorMessage.value = ''
  const modelId = selectedNode.value?.model_id || 'BP-001'
  try {
    const res = await request(`${ENDPOINT}/${modelId}/time-window`, {
      method: 'POST', body: JSON.stringify({ time_window: timeWindow.value }),
    })
    if (!res.ok) throw new Error((await res.json().catch(() => ({})))?.detail || '窗口更新失败')
    await reload()
  } catch (e: any) { errorMessage.value = e.message }
}
async function loadResult(modelId = 'BP-001') {
  const res = await request(`${ENDPOINT}/${modelId}/result`)
  if (!res.ok) return
  result.value = await res.json()
  timeWindow.value = result.value.time_window
}
async function loadCommonPaths() {
  errorMessage.value = ''
  const modelId = selectedNode.value?.model_id || 'BP-001'
  try {
    const res = await request(`${ENDPOINT}/${modelId}/common-paths?min_count=${minCount}`)
    commonPaths.value = res.ok ? await res.json() : []
  } catch (e: any) { errorMessage.value = e.message }
}
async function loadDetails() {
  errorMessage.value = ''
  const modelId = selectedNode.value?.model_id || 'BP-001'
  try {
    const res = await request(`${ENDPOINT}/${modelId}/path-details`)
    const data = res.ok ? await res.json() : { items: [] }
    pathDetails.value = data.items || []
    if (data.stale) errorMessage.value = `当前明细来自过期产物：${data.stale_reason}`
  } catch (e: any) { errorMessage.value = e.message }
}
async function runAction(action: string, node: any) {
  try {
    await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    await reload()
  } catch (e: any) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    if (!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload()
  } catch (e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(async () => {
  await reload()
  await loadResult()
})
</script>
