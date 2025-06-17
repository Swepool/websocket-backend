<script lang="ts">
import { chains } from "$lib/stores/chains.svelte"
import { Option } from "effect"
import { onMount } from "svelte"
import { initializeCanvasWithCleanup } from "$lib/utils/canvasInit"
import Card from "./ui/Card.svelte"
import type { TransferListItem } from "@unionlabs/sdk/schema"

type EnhancedTransferListItem = TransferListItem & {
  isTestnetTransfer?: boolean
  formattedTimestamp?: string
  routeKey?: string
  senderDisplay?: string
  receiverDisplay?: string
}

interface FlowStream {
  id: string
  sourceX: number
  sourceY: number
  targetX: number
  targetY: number
  width: number
  color: string
  progress: number
  fromChain: string
  toChain: string
  transferCount: number
  opacity: number
}

interface FlowNode {
  id: string
  x: number
  y: number
  width: number
  height: number
  color: string
  displayName: string
  totalInflow: number
  totalOutflow: number
  transferCount: number
  isSource: boolean
  isTarget: boolean
  activity: number
  rank: number
}

interface FlowConnection {
  source: string
  target: string
  value: number
  transferCount: number
  color: string
  width: number
}

let {
  transfers = [],
  onChainSelection = () => {},
}: {
  transfers: EnhancedTransferListItem[]
  onChainSelection?: (fromChain: string | null, toChain: string | null) => void
} = $props()

// Color configuration
const COLORS = {
  background: "#050505",
  node: {
    source: "#3b82f6",
    target: "#10b981",
    hub: "#f59e0b",
    default: "#6b7280"
  },
  flow: {
    mainnet: "#8b5cf6",
    testnet: "#06d6a0",
    selected: "#f59e0b",
    gradient: ["#8b5cf6", "#06d6a0", "#f59e0b"]
  },
  text: "#f8fafc",
  textSecondary: "#cbd5e1",
  border: "#374151",
  highlight: "#fbbf24"
} as const

// Layout constants
const LAYOUT = {
  nodeWidth: 15,
  nodeMinHeight: 40,
  nodeMaxHeight: 200,
  levelSpacing: 200,
  nodeSpacing: 80,
  flowMinWidth: 2,
  flowMaxWidth: 30,
  streamResolution: 50
}

// Performance constants
const PARTICLE_SPEED = 0.015
const TARGET_FPS = 60
const FRAME_INTERVAL = 1000 / TARGET_FPS
const MAX_STREAMS = 100

// State variables
let canvas: HTMLCanvasElement
let ctx: CanvasRenderingContext2D
let animationFrame: number
let lastFrameTime = 0
let canvasWidth = $state(800)
let canvasHeight = $state(600)
let containerElement: HTMLElement
let canvasReady = $state(false)

// Mouse state
let mouseX = 0
let mouseY = 0
let hoveredNode: string | null = null

// Selection state
let selectedFromChain: string | null = $state(null)
let selectedToChain: string | null = $state(null)

// Flow state
let nodes = new Map<string, FlowNode>()
let connections = new Map<string, FlowConnection>()
let streams: FlowStream[] = []
let levels: string[][] = []

// Reactive derived values
let transfersLength = $derived(transfers.length)
let processedTransferCount = $state(0)

function updateCanvasSize() {
  if (!containerElement || !canvas || !ctx) return

  const rect = containerElement.getBoundingClientRect()
  const dpr = window.devicePixelRatio || 1
  
  canvasWidth = rect.width
  canvasHeight = rect.height

  canvas.width = canvasWidth * dpr
  canvas.height = canvasHeight * dpr
  canvas.style.width = `${canvasWidth}px`
  canvas.style.height = `${canvasHeight}px`

  if (ctx) {
    ctx.scale(dpr, dpr)
  }

  setupFlowLayout()
}

function setupFlowLayout() {
  if (!Option.isSome(chains.data)) return

  const chainData = chains.data.value
  if (!chainData?.length) return

  nodes.clear()
  connections.clear()
  levels = []

  // Initialize nodes
  chainData.forEach((chain, index) => {
    nodes.set(chain.universal_chain_id, {
      id: chain.universal_chain_id,
      x: 0,
      y: 0,
      width: LAYOUT.nodeWidth,
      height: LAYOUT.nodeMinHeight,
      color: COLORS.node.default,
      displayName: chain.display_name || chain.chain_id,
      totalInflow: 0,
      totalOutflow: 0,
      transferCount: 0,
      isSource: false,
      isTarget: false,
      activity: 0,
      rank: 0
    })
  })

  calculateNodeRanking()
  arrangeNodesInLevels()
  positionNodes()
}

function calculateNodeRanking() {
  // Simple ranking based on connectivity - more connected nodes get central positions
  const connectivityMap = new Map<string, number>()
  
  for (const connection of connections.values()) {
    connectivityMap.set(connection.source, (connectivityMap.get(connection.source) || 0) + 1)
    connectivityMap.set(connection.target, (connectivityMap.get(connection.target) || 0) + 1)
  }

  // Assign ranks (0 = leftmost, higher = rightmost)
  const nodeArray = Array.from(nodes.values())
  nodeArray.sort((a, b) => (connectivityMap.get(b.id) || 0) - (connectivityMap.get(a.id) || 0))
  
  nodeArray.forEach((node, index) => {
    node.rank = Math.floor(index / Math.ceil(nodeArray.length / 3)) // 3 levels
  })
}

function arrangeNodesInLevels() {
  const maxRank = Math.max(...Array.from(nodes.values()).map(n => n.rank))
  levels = Array(maxRank + 1).fill(null).map(() => [])
  
  for (const node of nodes.values()) {
    levels[node.rank].push(node.id)
  }
}

function positionNodes() {
  const levelWidth = canvasWidth / (levels.length + 1)
  
  levels.forEach((level, levelIndex) => {
    const x = levelWidth * (levelIndex + 1)
    const totalHeight = level.length * LAYOUT.nodeSpacing
    const startY = (canvasHeight - totalHeight) / 2 + LAYOUT.nodeSpacing / 2
    
    level.forEach((nodeId, nodeIndex) => {
      const node = nodes.get(nodeId)!
      node.x = x
      node.y = startY + nodeIndex * LAYOUT.nodeSpacing
      
      // Update node height based on activity
      node.height = Math.max(
        LAYOUT.nodeMinHeight,
        Math.min(LAYOUT.nodeMaxHeight, LAYOUT.nodeMinHeight + node.transferCount * 3)
      )
    })
  })
}

function createStreamFromTransfer(transfer: EnhancedTransferListItem) {
  if (streams.length >= MAX_STREAMS) {
    streams.splice(0, Math.floor(MAX_STREAMS * 0.2))
  }

  const sourceId = transfer.source_chain.universal_chain_id
  const targetId = transfer.destination_chain.universal_chain_id

  if (!nodes.has(sourceId) || !nodes.has(targetId)) return

  const sourceNode = nodes.get(sourceId)!
  const targetNode = nodes.get(targetId)!

  // Update connection
  const connectionKey = `${sourceId}-${targetId}`
  if (connections.has(connectionKey)) {
    const conn = connections.get(connectionKey)!
    conn.value += 1
    conn.transferCount += 1
    conn.width = Math.min(LAYOUT.flowMaxWidth, LAYOUT.flowMinWidth + conn.transferCount * 0.5)
  } else {
    connections.set(connectionKey, {
      source: sourceId,
      target: targetId,
      value: 1,
      transferCount: 1,
      color: transfer.isTestnetTransfer ? COLORS.flow.testnet : COLORS.flow.mainnet,
      width: LAYOUT.flowMinWidth
    })
  }

  // Update node properties
  sourceNode.totalOutflow += 1
  targetNode.totalInflow += 1
  sourceNode.transferCount += 1
  targetNode.transferCount += 1
  sourceNode.activity = Math.min(sourceNode.activity + 1, 10)
  targetNode.activity = Math.min(targetNode.activity + 1, 10)

  // Determine node types
  sourceNode.isSource = sourceNode.totalOutflow > sourceNode.totalInflow
  targetNode.isTarget = targetNode.totalInflow > targetNode.totalOutflow

  // Create animated stream
  const stream: FlowStream = {
    id: transfer.packet_hash,
    sourceX: sourceNode.x + sourceNode.width,
    sourceY: sourceNode.y + sourceNode.height / 2,
    targetX: targetNode.x,
    targetY: targetNode.y + targetNode.height / 2,
    width: Math.max(2, Math.min(8, Math.sqrt(connections.get(connectionKey)!.transferCount))),
    color: transfer.isTestnetTransfer ? COLORS.flow.testnet : COLORS.flow.mainnet,
    progress: 0,
    fromChain: sourceId,
    toChain: targetId,
    transferCount: 1,
    opacity: 0.8
  }

  streams.push(stream)
  
  // Reposition nodes after each transfer
  positionNodes()
}

function drawBackground() {
  ctx.fillStyle = COLORS.background
  ctx.fillRect(0, 0, canvasWidth, canvasHeight)
  
  // Draw subtle vertical guides
  ctx.strokeStyle = COLORS.border
  ctx.lineWidth = 1
  ctx.globalAlpha = 0.1
  
  levels.forEach((_, levelIndex) => {
    const x = (canvasWidth / (levels.length + 1)) * (levelIndex + 1)
    ctx.beginPath()
    ctx.moveTo(x, 0)
    ctx.lineTo(x, canvasHeight)
    ctx.stroke()
  })
  
  ctx.globalAlpha = 1
}

function drawConnections() {
  // Draw static connection flows
  for (const connection of connections.values()) {
    const sourceNode = nodes.get(connection.source)
    const targetNode = nodes.get(connection.target)
    
    if (!sourceNode || !targetNode) continue
    
    const isSelected = (selectedFromChain === connection.source && selectedToChain === connection.target) ||
                      (selectedFromChain === connection.target && selectedToChain === connection.source)
    
    const gradient = ctx.createLinearGradient(
      sourceNode.x + sourceNode.width, sourceNode.y,
      targetNode.x, targetNode.y
    )
    gradient.addColorStop(0, connection.color + '40')
    gradient.addColorStop(1, connection.color + '20')
    
    ctx.strokeStyle = isSelected ? COLORS.flow.selected : gradient
    ctx.lineWidth = isSelected ? connection.width + 2 : connection.width
    ctx.globalAlpha = isSelected ? 0.9 : 0.4
    
    // Draw curved flow line
    const sourceX = sourceNode.x + sourceNode.width
    const sourceY = sourceNode.y + sourceNode.height / 2
    const targetX = targetNode.x
    const targetY = targetNode.y + targetNode.height / 2
    
    const midX = (sourceX + targetX) / 2
    
    ctx.beginPath()
    ctx.moveTo(sourceX, sourceY)
    ctx.bezierCurveTo(
      midX, sourceY,
      midX, targetY,
      targetX, targetY
    )
    ctx.stroke()
  }
  
  ctx.globalAlpha = 1
}

function drawStreams() {
  for (const stream of streams) {
    const t = stream.progress
    
    // Calculate position along bezier curve
    const sourceX = stream.sourceX
    const sourceY = stream.sourceY
    const targetX = stream.targetX
    const targetY = stream.targetY
    const midX = (sourceX + targetX) / 2
    
    // Bezier curve calculation
    const x = Math.pow(1 - t, 2) * sourceX + 
              2 * (1 - t) * t * midX + 
              Math.pow(t, 2) * targetX
    const y = Math.pow(1 - t, 2) * sourceY + 
              2 * (1 - t) * t * sourceY + 
              Math.pow(t, 2) * targetY
    
    // Draw animated particle
    ctx.fillStyle = stream.color
    ctx.shadowColor = stream.color
    ctx.shadowBlur = 15
    ctx.globalAlpha = stream.opacity
    
    ctx.beginPath()
    ctx.arc(x, y, stream.width / 2, 0, 2 * Math.PI)
    ctx.fill()
    
    ctx.shadowBlur = 0
  }
  
  ctx.globalAlpha = 1
}

function drawNodes() {
  for (const [nodeId, node] of nodes) {
    const isSelected = nodeId === selectedFromChain || nodeId === selectedToChain
    const isHovered = nodeId === hoveredNode
    
    // Determine node color based on role and activity
    let nodeColor = COLORS.node.default
    if (node.isSource && node.isTarget) nodeColor = COLORS.node.hub
    else if (node.isSource) nodeColor = COLORS.node.source
    else if (node.isTarget) nodeColor = COLORS.node.target
    
    if (isSelected) {
      ctx.shadowColor = COLORS.highlight
      ctx.shadowBlur = 20
    }
    
    // Node background with activity glow
    if (node.activity > 0) {
      ctx.fillStyle = nodeColor + Math.floor(node.activity * 10).toString(16).padStart(2, '0')
      ctx.fillRect(
        node.x - 5,
        node.y - 5,
        node.width + 10,
        node.height + 10
      )
    }
    
    // Main node
    ctx.fillStyle = isSelected ? COLORS.highlight : nodeColor
    ctx.fillRect(node.x, node.y, node.width, node.height)
    
    // Node border
    ctx.strokeStyle = isSelected ? COLORS.highlight : COLORS.border
    ctx.lineWidth = isSelected ? 2 : 1
    ctx.strokeRect(node.x, node.y, node.width, node.height)
    
    ctx.shadowBlur = 0
    
    // Transfer count display
    if (node.transferCount > 0) {
      ctx.fillStyle = COLORS.text
      ctx.font = "10px monospace"
      ctx.textAlign = "center"
      ctx.fillText(
        node.transferCount.toString(),
        node.x + node.width / 2,
        node.y - 8
      )
    }
    
    // Node label
    ctx.fillStyle = isHovered ? COLORS.text : COLORS.textSecondary
    ctx.font = isHovered ? "11px sans-serif" : "10px sans-serif"
    ctx.textAlign = "center"
    ctx.fillText(
      node.displayName,
      node.x + node.width / 2,
      node.y + node.height + 18
    )
    
    // Flow indicators
    if (node.totalInflow > 0 || node.totalOutflow > 0) {
      ctx.fillStyle = COLORS.textSecondary
      ctx.font = "8px monospace"
      ctx.textAlign = "center"
      ctx.fillText(
        `↓${node.totalInflow} ↑${node.totalOutflow}`,
        node.x + node.width / 2,
        node.y + node.height + 30
      )
    }
    
    // Decay activity
    node.activity = Math.max(node.activity - 0.05, 0)
  }
}

function checkHover() {
  hoveredNode = null
  for (const [nodeId, node] of nodes) {
    if (mouseX >= node.x && mouseX <= node.x + node.width &&
        mouseY >= node.y && mouseY <= node.y + node.height) {
      hoveredNode = nodeId
      break
    }
  }
  
  if (canvas) {
    canvas.style.cursor = hoveredNode ? "pointer" : "default"
  }
}

function handleChainClick() {
  if (!hoveredNode) {
    selectedFromChain = null
    selectedToChain = null
    onChainSelection(null, null)
    return
  }

  if (!selectedFromChain) {
    selectedFromChain = hoveredNode
    selectedToChain = null
    onChainSelection(hoveredNode, null)
  } else if (!selectedToChain && hoveredNode !== selectedFromChain) {
    selectedToChain = hoveredNode
    onChainSelection(selectedFromChain, hoveredNode)
  } else {
    selectedFromChain = hoveredNode
    selectedToChain = null
    onChainSelection(hoveredNode, null)
  }
}

function animate(currentTime = 0) {
  if (!ctx) {
    animationFrame = requestAnimationFrame(animate)
    return
  }

  if (currentTime - lastFrameTime < FRAME_INTERVAL) {
    animationFrame = requestAnimationFrame(animate)
    return
  }
  lastFrameTime = currentTime

  checkHover()

  // Clear and draw background
  drawBackground()
  
  // Draw static elements
  drawConnections()
  
  // Update and draw streams
  const activeStreams: FlowStream[] = []
  for (const stream of streams) {
    stream.progress += PARTICLE_SPEED
    
    if (stream.progress >= 1) {
      const targetNode = nodes.get(stream.toChain)
      if (targetNode) {
        targetNode.activity = Math.min(targetNode.activity + 1, 10)
      }
    } else {
      // Fade out stream as it progresses
      stream.opacity = Math.max(0.2, 1 - stream.progress)
      activeStreams.push(stream)
    }
  }
  streams = activeStreams
  
  drawStreams()
  drawNodes()

  animationFrame = requestAnimationFrame(animate)
}

// Event handlers
const handleMouseMove = (e: MouseEvent) => {
  const rect = canvas.getBoundingClientRect()
  mouseX = e.clientX - rect.left
  mouseY = e.clientY - rect.top
}

const handleMouseLeave = () => {
  hoveredNode = null
  canvas.style.cursor = "default"
}

// Process new transfers
$effect(() => {
  if (canvasReady && transfersLength > processedTransferCount) {
    const newTransfers = transfers.slice(processedTransferCount)
    newTransfers.forEach(transfer => {
      createStreamFromTransfer(transfer)
    })
    processedTransferCount = transfers.length
  }
})

// Setup chains when data is available
$effect(() => {
  if (Option.isSome(chains.data)) {
    setupFlowLayout()
  }
})

onMount(() => {
  if (canvas && containerElement) {
    ctx = canvas.getContext("2d")!

    const cleanup = initializeCanvasWithCleanup({
      canvas,
      container: containerElement,
      onInitialized: () => {
        updateCanvasSize()
        canvasReady = true
        animate()
      },
      onResize: () => {
        updateCanvasSize()
      },
      eventListeners: [
        { element: canvas, event: "mousemove", handler: handleMouseMove as (event: Event) => void },
        { element: canvas, event: "mouseleave", handler: handleMouseLeave as (event: Event) => void },
        { element: canvas, event: "click", handler: handleChainClick as (event: Event) => void }
      ]
    })

    return () => {
      cancelAnimationFrame(animationFrame)
      cleanup()
    }
  }

  return () => {
    cancelAnimationFrame(animationFrame)
  }
})
</script>

<Card class="h-full p-3">
  <div class="relative w-full h-full" bind:this={containerElement}>
    {#if selectedFromChain || selectedToChain}
      <div class="absolute top-0 left-0 z-10 bg-black/95 rounded-lg p-3 text-white min-w-56">
        <div class="text-xs mb-1 text-purple-400">Sankey Flow Diagram</div>
        {#if selectedFromChain && selectedToChain}
          <div class="text-sm font-medium mb-2">
            {nodes.get(selectedFromChain)?.displayName} → 
            {nodes.get(selectedToChain)?.displayName}
          </div>
          <div class="text-xs mb-1">
            Flow Volume: {connections.get(`${selectedFromChain}-${selectedToChain}`)?.transferCount || 
                          connections.get(`${selectedToChain}-${selectedFromChain}`)?.transferCount || 0}
          </div>
          <div class="text-xs">
            Flow Width: {connections.get(`${selectedFromChain}-${selectedToChain}`)?.width.toFixed(1) || 
                         connections.get(`${selectedToChain}-${selectedFromChain}`)?.width.toFixed(1) || '0.0'}px
          </div>
        {:else if selectedFromChain}
          <div class="text-sm font-medium mb-2">
            {nodes.get(selectedFromChain)?.displayName}
          </div>
          <div class="text-xs mb-1">
            Inflow: {nodes.get(selectedFromChain)?.totalInflow || 0}
          </div>
          <div class="text-xs mb-1">
            Outflow: {nodes.get(selectedFromChain)?.totalOutflow || 0}
          </div>
          <div class="text-xs">
            Total: {nodes.get(selectedFromChain)?.transferCount || 0}
          </div>
        {/if}
        <div class="text-xs mt-2 text-gray-400">
          Click nodes to analyze flows
        </div>
      </div>
    {/if}
    <canvas
      bind:this={canvas}
      class="w-full h-full"
      style="background: transparent;"
    />
  </div>
</Card> 