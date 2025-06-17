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

interface GridParticle {
  id: string
  x: number
  y: number
  startX: number
  startY: number
  targetX: number
  targetY: number
  fromChain: string
  toChain: string
  progress: number
  color: string
  size: number
  pathType: 'straight' | 'curved'
}

interface GridChainNode {
  x: number
  y: number
  gridX: number
  gridY: number
  size: number
  color: string
  activity: number
  displayName: string
  glowIntensity: number
  transferCount: number
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
  background: "#0a0a0a",
  gridLines: "#1a1a1a",
  chainDefault: "#374151",
  chainActive: "#60a5fa",
  chainSelected: "#3b82f6",
  chainHot: "#ef4444",
  particle: "#fbbf24",
  particleTestnet: "#06d6a0",
  connectionFlow: "#4f46e5",
  text: "#ffffff",
  textSecondary: "#9ca3af",
  heatmapLow: "#1e293b",
  heatmapHigh: "#dc2626"
} as const

// Performance constants
const PARTICLE_SPEED = 0.02
const TARGET_FPS = 60
const FRAME_INTERVAL = 1000 / TARGET_FPS
const MAX_PARTICLES = 150
const GRID_SPACING = 80
const NODE_SIZE = 15

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
let hoveredChain: string | null = null

// Selection state
let selectedFromChain: string | null = $state(null)
let selectedToChain: string | null = $state(null)

// Animation state
let particles: GridParticle[] = []
let chainNodes = new Map<string, GridChainNode>()
let gridCols = 0
let gridRows = 0
let transferCounts = new Map<string, number>()

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

  setupGridLayout()
}

function setupGridLayout() {
  if (!Option.isSome(chains.data)) return

  const chainData = chains.data.value
  if (!chainData?.length) return

  chainNodes.clear()
  
  // Calculate optimal grid dimensions
  const chainCount = chainData.length
  gridCols = Math.ceil(Math.sqrt(chainCount))
  gridRows = Math.ceil(chainCount / gridCols)
  
  // Calculate grid positioning
  const totalWidth = (gridCols - 1) * GRID_SPACING
  const totalHeight = (gridRows - 1) * GRID_SPACING
  const startX = (canvasWidth - totalWidth) / 2
  const startY = (canvasHeight - totalHeight) / 2

  chainData.forEach((chain, index) => {
    const col = index % gridCols
    const row = Math.floor(index / gridCols)
    const x = startX + col * GRID_SPACING
    const y = startY + row * GRID_SPACING

    chainNodes.set(chain.universal_chain_id, {
      x,
      y,
      gridX: col,
      gridY: row,
      size: NODE_SIZE,
      color: COLORS.chainDefault,
      activity: 0,
      displayName: chain.display_name || chain.chain_id,
      glowIntensity: 0,
      transferCount: 0
    })
  })
}

function createParticleFromTransfer(transfer: EnhancedTransferListItem) {
  if (particles.length >= MAX_PARTICLES) {
    particles.splice(0, Math.floor(MAX_PARTICLES * 0.2))
  }

  const sourceId = transfer.source_chain.universal_chain_id
  const destId = transfer.destination_chain.universal_chain_id

  if (!chainNodes.has(sourceId) || !chainNodes.has(destId)) return

  const fromNode = chainNodes.get(sourceId)!
  const toNode = chainNodes.get(destId)!

  // Update transfer counts for heatmap
  const routeKey = `${sourceId}-${destId}`
  transferCounts.set(routeKey, (transferCounts.get(routeKey) || 0) + 1)

  // Increase activity
  fromNode.activity = Math.min(fromNode.activity + 1, 5)
  toNode.activity = Math.min(toNode.activity + 1, 5)
  fromNode.transferCount += 1
  toNode.transferCount += 1

  // Determine path type based on grid position
  const pathType = Math.abs(fromNode.gridX - toNode.gridX) + Math.abs(fromNode.gridY - toNode.gridY) > 2 
    ? 'curved' : 'straight'

  const particle: GridParticle = {
    id: transfer.packet_hash,
    x: fromNode.x,
    y: fromNode.y,
    startX: fromNode.x,
    startY: fromNode.y,
    targetX: toNode.x,
    targetY: toNode.y,
    fromChain: sourceId,
    toChain: destId,
    progress: 0,
    color: transfer.isTestnetTransfer ? COLORS.particleTestnet : COLORS.particle,
    size: 3,
    pathType
  }

  particles.push(particle)
}

function drawBackground() {
  ctx.fillStyle = COLORS.background
  ctx.fillRect(0, 0, canvasWidth, canvasHeight)
  
  // Draw grid lines
  ctx.strokeStyle = COLORS.gridLines
  ctx.lineWidth = 0.5
  ctx.globalAlpha = 0.3
  
  // Vertical lines
  for (let col = 0; col < gridCols; col++) {
    const x = (canvasWidth - (gridCols - 1) * GRID_SPACING) / 2 + col * GRID_SPACING
    ctx.beginPath()
    ctx.moveTo(x, 0)
    ctx.lineTo(x, canvasHeight)
    ctx.stroke()
  }
  
  // Horizontal lines
  for (let row = 0; row < gridRows; row++) {
    const y = (canvasHeight - (gridRows - 1) * GRID_SPACING) / 2 + row * GRID_SPACING
    ctx.beginPath()
    ctx.moveTo(0, y)
    ctx.lineTo(canvasWidth, y)
    ctx.stroke()
  }
  
  ctx.globalAlpha = 1
}

function drawHeatmapConnections() {
  const maxTransfers = Math.max(...transferCounts.values(), 1)
  
  ctx.lineWidth = 2
  ctx.globalAlpha = 0.6
  
  for (const [routeKey, count] of transferCounts) {
    const [fromId, toId] = routeKey.split('-')
    const fromNode = chainNodes.get(fromId)
    const toNode = chainNodes.get(toId)
    
    if (!fromNode || !toNode) continue
    
    const intensity = count / maxTransfers
    const hue = 240 - (intensity * 180) // Blue to red
    ctx.strokeStyle = `hsl(${hue}, 80%, ${50 + intensity * 30}%)`
    
    ctx.beginPath()
    ctx.moveTo(fromNode.x, fromNode.y)
    ctx.lineTo(toNode.x, toNode.y)
    ctx.stroke()
  }
  
  ctx.globalAlpha = 1
}

function drawParticles() {
  for (const particle of particles) {
    const t = particle.progress
    
    if (particle.pathType === 'curved') {
      // Bezier curve path
      const midX = (particle.startX + particle.targetX) / 2
      const midY = (particle.startY + particle.targetY) / 2 - 30
      
      const bezierT = t
      const x = Math.pow(1 - bezierT, 2) * particle.startX + 
                2 * (1 - bezierT) * bezierT * midX + 
                Math.pow(bezierT, 2) * particle.targetX
      const y = Math.pow(1 - bezierT, 2) * particle.startY + 
                2 * (1 - bezierT) * bezierT * midY + 
                Math.pow(bezierT, 2) * particle.targetY
      
      particle.x = x
      particle.y = y
    } else {
      // Straight line path
      particle.x = particle.startX + (particle.targetX - particle.startX) * t
      particle.y = particle.startY + (particle.targetY - particle.startY) * t
    }

    // Draw particle with trail effect
    ctx.fillStyle = particle.color
    ctx.shadowColor = particle.color
    ctx.shadowBlur = 8
    ctx.beginPath()
    ctx.arc(particle.x, particle.y, particle.size, 0, 2 * Math.PI)
    ctx.fill()
    ctx.shadowBlur = 0
  }
}

function drawNodes() {
  for (const [chainId, node] of chainNodes) {
    const isSelected = chainId === selectedFromChain || chainId === selectedToChain
    const isHovered = chainId === hoveredChain
    
    // Node background based on activity
    const activityIntensity = Math.min(node.activity / 5, 1)
    const activityColor = `rgba(239, 68, 68, ${activityIntensity * 0.3})`
    
    if (activityIntensity > 0) {
      ctx.fillStyle = activityColor
      ctx.beginPath()
      ctx.arc(node.x, node.y, node.size + 10, 0, 2 * Math.PI)
      ctx.fill()
    }
    
    // Main node
    ctx.fillStyle = isSelected ? COLORS.chainSelected : 
                   isHovered ? COLORS.chainActive : 
                   node.activity > 2 ? COLORS.chainHot : COLORS.chainDefault
    
    ctx.beginPath()
    ctx.arc(node.x, node.y, node.size, 0, 2 * Math.PI)
    ctx.fill()
    
    // Selection border
    if (isSelected) {
      ctx.strokeStyle = COLORS.chainSelected
      ctx.lineWidth = 3
      ctx.stroke()
    }
    
    // Transfer count display
    if (node.transferCount > 0) {
      ctx.fillStyle = COLORS.text
      ctx.font = "10px monospace"
      ctx.textAlign = "center"
      ctx.fillText(node.transferCount.toString(), node.x, node.y - node.size - 15)
    }
    
    // Chain name on hover
    if (isHovered) {
      ctx.fillStyle = COLORS.text
      ctx.font = "12px sans-serif"
      ctx.textAlign = "center"
      ctx.fillText(node.displayName, node.x, node.y + node.size + 20)
    }
    
    // Decay activity
    node.activity = Math.max(node.activity - 0.05, 0)
  }
}

function checkHover() {
  hoveredChain = null
  for (const [chainId, node] of chainNodes) {
    const distance = Math.sqrt(
      (mouseX - node.x) ** 2 + (mouseY - node.y) ** 2
    )
    if (distance <= node.size + 5) {
      hoveredChain = chainId
      break
    }
  }
  
  if (canvas) {
    canvas.style.cursor = hoveredChain ? "pointer" : "default"
  }
}

function handleChainClick() {
  if (!hoveredChain) {
    selectedFromChain = null
    selectedToChain = null
    onChainSelection(null, null)
    return
  }

  if (!selectedFromChain) {
    selectedFromChain = hoveredChain
    selectedToChain = null
    onChainSelection(hoveredChain, null)
  } else if (!selectedToChain && hoveredChain !== selectedFromChain) {
    selectedToChain = hoveredChain
    onChainSelection(selectedFromChain, hoveredChain)
  } else {
    selectedFromChain = hoveredChain
    selectedToChain = null
    onChainSelection(hoveredChain, null)
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
  
  // Draw heatmap connections
  if (transferCounts.size > 0) {
    drawHeatmapConnections()
  }
  
  // Update particles
  const activeParticles: GridParticle[] = []
  for (const particle of particles) {
    particle.progress += PARTICLE_SPEED
    
    if (particle.progress >= 1) {
      const toNode = chainNodes.get(particle.toChain)
      if (toNode) {
        toNode.glowIntensity = 1.0
      }
    } else {
      activeParticles.push(particle)
    }
  }
  particles = activeParticles
  
  // Draw components
  drawParticles()
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
  hoveredChain = null
  canvas.style.cursor = "default"
}

// Process new transfers
$effect(() => {
  if (canvasReady && transfersLength > processedTransferCount) {
    const newTransfers = transfers.slice(processedTransferCount)
    newTransfers.forEach(transfer => {
      createParticleFromTransfer(transfer)
    })
    processedTransferCount = transfers.length
  }
})

// Setup chains when data is available
$effect(() => {
  if (Option.isSome(chains.data)) {
    setupGridLayout()
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
      <div class="absolute top-0 left-0 z-10 bg-black/80 rounded-lg p-3 text-white min-w-48">
        <div class="text-xs mb-1 text-gray-400">Grid Network Analysis</div>
        {#if selectedFromChain && selectedToChain}
          <div class="text-sm font-medium mb-2">
            {chainNodes.get(selectedFromChain)?.displayName} → 
            {chainNodes.get(selectedToChain)?.displayName}
          </div>
          <div class="text-xs">
            Transfers: {transferCounts.get(`${selectedFromChain}-${selectedToChain}`) || 0}
          </div>
        {:else if selectedFromChain}
          <div class="text-sm font-medium mb-2">
            {chainNodes.get(selectedFromChain)?.displayName}
          </div>
          <div class="text-xs">
            Total Transfers: {chainNodes.get(selectedFromChain)?.transferCount || 0}
          </div>
        {/if}
        <div class="text-xs mt-2 text-gray-500">
          Click to select route • Click elsewhere to clear
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