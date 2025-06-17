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

interface ForceParticle {
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
  velocity: { x: number; y: number }
}

interface ForceNode {
  id: string
  x: number
  y: number
  vx: number
  vy: number
  size: number
  mass: number
  color: string
  displayName: string
  activity: number
  connections: number
  transferCount: number
  charge: number
  fixed: boolean
}

interface ForceEdge {
  source: string
  target: string
  strength: number
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
  background: "#0f0f23",
  node: {
    default: "#64748b",
    active: "#06b6d4",
    selected: "#0ea5e9",
    hot: "#f59e0b",
    hub: "#ef4444"
  },
  edge: {
    default: "#334155",
    active: "#0ea5e9",
    flow: "#06d6a0"
  },
  particle: {
    default: "#f59e0b",
    testnet: "#06d6a0",
    trail: "#fbbf24"
  },
  text: "#e2e8f0",
  textSecondary: "#94a3b8",
  glow: "#0ea5e9"
} as const

// Physics constants
const PHYSICS = {
  centerForce: 0.01,
  repelForce: 8000,
  attractForce: 0.1,
  damping: 0.95,
  minDistance: 50,
  maxDistance: 200,
  nodeSize: { min: 8, max: 25 },
  edgeStrength: { min: 0.1, max: 2.0 }
}

// Performance constants
const PARTICLE_SPEED = 0.025
const TARGET_FPS = 60
const FRAME_INTERVAL = 1000 / TARGET_FPS
const MAX_PARTICLES = 100

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
let isDragging = false
let draggedNode: string | null = null

// Selection state
let selectedFromChain: string | null = $state(null)
let selectedToChain: string | null = $state(null)

// Physics state
let nodes = new Map<string, ForceNode>()
let edges = new Map<string, ForceEdge>()
let particles: ForceParticle[] = []

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

  initializeForceLayout()
}

function initializeForceLayout() {
  if (!Option.isSome(chains.data)) return

  const chainData = chains.data.value
  if (!chainData?.length) return

  nodes.clear()
  edges.clear()

  const centerX = canvasWidth / 2
  const centerY = canvasHeight / 2

  // Initialize nodes with random positions
  chainData.forEach((chain, index) => {
    const angle = (index / chainData.length) * 2 * Math.PI
    const radius = Math.min(canvasWidth, canvasHeight) * 0.2
    const x = centerX + Math.cos(angle) * radius + (Math.random() - 0.5) * 100
    const y = centerY + Math.sin(angle) * radius + (Math.random() - 0.5) * 100

    nodes.set(chain.universal_chain_id, {
      id: chain.universal_chain_id,
      x,
      y,
      vx: 0,
      vy: 0,
      size: PHYSICS.nodeSize.min,
      mass: 1,
      color: COLORS.node.default,
      displayName: chain.display_name || chain.chain_id,
      activity: 0,
      connections: 0,
      transferCount: 0,
      charge: -500,
      fixed: false
    })
  })
}

function createParticleFromTransfer(transfer: EnhancedTransferListItem) {
  if (particles.length >= MAX_PARTICLES) {
    particles.splice(0, Math.floor(MAX_PARTICLES * 0.3))
  }

  const sourceId = transfer.source_chain.universal_chain_id
  const destId = transfer.destination_chain.universal_chain_id

  if (!nodes.has(sourceId) || !nodes.has(destId)) return

  const fromNode = nodes.get(sourceId)!
  const toNode = nodes.get(destId)!

  // Update edge strength
  const edgeKey = `${sourceId}-${destId}`
  const reverseKey = `${destId}-${sourceId}`
  
  if (edges.has(edgeKey)) {
    edges.get(edgeKey)!.transferCount += 1
    edges.get(edgeKey)!.strength = Math.min(edges.get(edgeKey)!.strength + 0.1, PHYSICS.edgeStrength.max)
  } else if (edges.has(reverseKey)) {
    edges.get(reverseKey)!.transferCount += 1
    edges.get(reverseKey)!.strength = Math.min(edges.get(reverseKey)!.strength + 0.1, PHYSICS.edgeStrength.max)
  } else {
    edges.set(edgeKey, {
      source: sourceId,
      target: destId,
      strength: PHYSICS.edgeStrength.min,
      transferCount: 1
    })
  }

  // Update node properties
  fromNode.activity = Math.min(fromNode.activity + 1, 10)
  toNode.activity = Math.min(toNode.activity + 1, 10)
  fromNode.transferCount += 1
  toNode.transferCount += 1
  fromNode.connections += 1
  toNode.connections += 1

  // Update node sizes based on activity
  fromNode.size = Math.min(PHYSICS.nodeSize.min + fromNode.connections * 1.5, PHYSICS.nodeSize.max)
  toNode.size = Math.min(PHYSICS.nodeSize.min + toNode.connections * 1.5, PHYSICS.nodeSize.max)

  const particle: ForceParticle = {
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
    color: transfer.isTestnetTransfer ? COLORS.particle.testnet : COLORS.particle.default,
    size: 2.5,
    velocity: { x: 0, y: 0 }
  }

  particles.push(particle)
}

function applyForces() {
  const centerX = canvasWidth / 2
  const centerY = canvasHeight / 2

  // Reset forces
  for (const node of nodes.values()) {
    if (!node.fixed) {
      node.vx *= PHYSICS.damping
      node.vy *= PHYSICS.damping

      // Center force
      const dx = centerX - node.x
      const dy = centerY - node.y
      const distanceToCenter = Math.sqrt(dx * dx + dy * dy)
      
      if (distanceToCenter > 0) {
        node.vx += (dx / distanceToCenter) * PHYSICS.centerForce
        node.vy += (dy / distanceToCenter) * PHYSICS.centerForce
      }
    }
  }

  // Node repulsion
  const nodeArray = Array.from(nodes.values())
  for (let i = 0; i < nodeArray.length; i++) {
    const nodeA = nodeArray[i]
    if (nodeA.fixed) continue

    for (let j = i + 1; j < nodeArray.length; j++) {
      const nodeB = nodeArray[j]
      
      const dx = nodeB.x - nodeA.x
      const dy = nodeB.y - nodeA.y
      const distance = Math.sqrt(dx * dx + dy * dy)
      
      if (distance > 0 && distance < PHYSICS.maxDistance) {
        const force = PHYSICS.repelForce / (distance * distance)
        const fx = (dx / distance) * force
        const fy = (dy / distance) * force
        
        nodeA.vx -= fx
        nodeA.vy -= fy
        if (!nodeB.fixed) {
          nodeB.vx += fx
          nodeB.vy += fy
        }
      }
    }
  }

  // Edge attraction
  for (const edge of edges.values()) {
    const source = nodes.get(edge.source)
    const target = nodes.get(edge.target)
    
    if (!source || !target) continue
    
    const dx = target.x - source.x
    const dy = target.y - source.y
    const distance = Math.sqrt(dx * dx + dy * dy)
    
    if (distance > PHYSICS.minDistance) {
      const force = (distance - PHYSICS.minDistance) * PHYSICS.attractForce * edge.strength
      const fx = (dx / distance) * force
      const fy = (dy / distance) * force
      
      if (!source.fixed) {
        source.vx += fx
        source.vy += fy
      }
      if (!target.fixed) {
        target.vx -= fx
        target.vy -= fy
      }
    }
  }

  // Apply velocities
  for (const node of nodes.values()) {
    if (!node.fixed) {
      node.x += node.vx
      node.y += node.vy
      
      // Boundary constraints with soft bounce
      const margin = node.size + 10
      if (node.x < margin) {
        node.x = margin
        node.vx *= -0.3
      }
      if (node.x > canvasWidth - margin) {
        node.x = canvasWidth - margin
        node.vx *= -0.3
      }
      if (node.y < margin) {
        node.y = margin
        node.vy *= -0.3
      }
      if (node.y > canvasHeight - margin) {
        node.y = canvasHeight - margin
        node.vy *= -0.3
      }
    }
  }
}

function drawEdges() {
  for (const edge of edges.values()) {
    const source = nodes.get(edge.source)
    const target = nodes.get(edge.target)
    
    if (!source || !target) continue
    
    const isSelected = (selectedFromChain === edge.source && selectedToChain === edge.target) ||
                      (selectedFromChain === edge.target && selectedToChain === edge.source)
    
    ctx.strokeStyle = isSelected ? COLORS.edge.active : COLORS.edge.default
    ctx.lineWidth = Math.max(1, edge.strength * 2)
    ctx.globalAlpha = isSelected ? 0.8 : Math.min(0.6, edge.transferCount * 0.1)
    
    ctx.beginPath()
    ctx.moveTo(source.x, source.y)
    ctx.lineTo(target.x, target.y)
    ctx.stroke()
  }
  
  ctx.globalAlpha = 1
}

function drawParticles() {
  for (const particle of particles) {
    // Update particle position along the path
    const t = particle.progress
    const sourceNode = nodes.get(particle.fromChain)
    const targetNode = nodes.get(particle.toChain)
    
    if (sourceNode && targetNode) {
      // Update target position if nodes moved
      particle.targetX = targetNode.x
      particle.targetY = targetNode.y
      
      // Smooth interpolation
      particle.x = particle.startX + (particle.targetX - particle.startX) * t
      particle.y = particle.startY + (particle.targetY - particle.startY) * t
    }

    // Draw particle with glow
    ctx.shadowColor = particle.color
    ctx.shadowBlur = 10
    ctx.fillStyle = particle.color
    ctx.beginPath()
    ctx.arc(particle.x, particle.y, particle.size, 0, 2 * Math.PI)
    ctx.fill()
    ctx.shadowBlur = 0
  }
}

function drawNodes() {
  for (const [nodeId, node] of nodes) {
    const isSelected = nodeId === selectedFromChain || nodeId === selectedToChain
    const isHovered = nodeId === hoveredNode
    
    // Activity-based color
    let nodeColor = COLORS.node.default
    if (node.connections > 5) nodeColor = COLORS.node.hub
    else if (node.activity > 5) nodeColor = COLORS.node.hot
    else if (node.activity > 0) nodeColor = COLORS.node.active
    
    if (isSelected) nodeColor = COLORS.node.selected
    
    // Node glow for activity
    if (node.activity > 0) {
      ctx.shadowColor = nodeColor
      ctx.shadowBlur = Math.min(node.activity * 2, 20)
    }
    
    // Main node
    ctx.fillStyle = nodeColor
    ctx.beginPath()
    ctx.arc(node.x, node.y, node.size, 0, 2 * Math.PI)
    ctx.fill()
    ctx.shadowBlur = 0
    
    // Selection ring
    if (isSelected) {
      ctx.strokeStyle = COLORS.node.selected
      ctx.lineWidth = 3
      ctx.beginPath()
      ctx.arc(node.x, node.y, node.size + 5, 0, 2 * Math.PI)
      ctx.stroke()
    }
    
    // Connection count
    if (node.connections > 0) {
      ctx.fillStyle = COLORS.text
      ctx.font = "10px monospace"
      ctx.textAlign = "center"
      ctx.fillText(node.connections.toString(), node.x, node.y - node.size - 12)
    }
    
    // Node label on hover
    if (isHovered) {
      ctx.fillStyle = COLORS.text
      ctx.font = "12px sans-serif"
      ctx.textAlign = "center"
      ctx.fillText(node.displayName, node.x, node.y + node.size + 18)
    }
    
    // Decay activity
    node.activity = Math.max(node.activity - 0.1, 0)
  }
}

function checkHover() {
  hoveredNode = null
  for (const [nodeId, node] of nodes) {
    const distance = Math.sqrt(
      (mouseX - node.x) ** 2 + (mouseY - node.y) ** 2
    )
    if (distance <= node.size + 5) {
      hoveredNode = nodeId
      break
    }
  }
  
  if (canvas) {
    canvas.style.cursor = hoveredNode ? "pointer" : "default"
  }
}

function handleMouseDown(e: MouseEvent) {
  if (hoveredNode) {
    isDragging = true
    draggedNode = hoveredNode
    const node = nodes.get(hoveredNode)!
    node.fixed = true
  }
}

function handleMouseUp() {
  if (isDragging && draggedNode) {
    const node = nodes.get(draggedNode)!
    node.fixed = false
    isDragging = false
    draggedNode = null
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
  
  // Update dragged node position
  if (isDragging && draggedNode) {
    const node = nodes.get(draggedNode)!
    node.x = mouseX
    node.y = mouseY
    node.vx = 0
    node.vy = 0
  }

  // Apply physics
  applyForces()

  // Clear canvas
  ctx.fillStyle = COLORS.background
  ctx.fillRect(0, 0, canvasWidth, canvasHeight)

  // Update particles
  const activeParticles: ForceParticle[] = []
  for (const particle of particles) {
    particle.progress += PARTICLE_SPEED
    
    if (particle.progress >= 1) {
      const toNode = nodes.get(particle.toChain)
      if (toNode) {
        toNode.activity = Math.min(toNode.activity + 2, 10)
      }
    } else {
      activeParticles.push(particle)
    }
  }
  particles = activeParticles

  // Draw everything
  drawEdges()
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
  hoveredNode = null
  canvas.style.cursor = "default"
  if (isDragging) {
    handleMouseUp()
  }
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
    initializeForceLayout()
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
        { element: canvas, event: "mousedown", handler: handleMouseDown as (event: Event) => void },
        { element: canvas, event: "mouseup", handler: handleMouseUp as (event: Event) => void },
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
      <div class="absolute top-0 left-0 z-10 bg-black/90 rounded-lg p-3 text-white min-w-52">
        <div class="text-xs mb-1 text-cyan-400">Force-Directed Network</div>
        {#if selectedFromChain && selectedToChain}
          <div class="text-sm font-medium mb-2">
            {nodes.get(selectedFromChain)?.displayName} → 
            {nodes.get(selectedToChain)?.displayName}
          </div>
          <div class="text-xs mb-1">
            Edge Strength: {edges.get(`${selectedFromChain}-${selectedToChain}`)?.strength.toFixed(1) || 
                           edges.get(`${selectedToChain}-${selectedFromChain}`)?.strength.toFixed(1) || '0.0'}
          </div>
          <div class="text-xs">
            Transfers: {edges.get(`${selectedFromChain}-${selectedToChain}`)?.transferCount || 
                       edges.get(`${selectedToChain}-${selectedFromChain}`)?.transferCount || 0}
          </div>
        {:else if selectedFromChain}
          <div class="text-sm font-medium mb-2">
            {nodes.get(selectedFromChain)?.displayName}
          </div>
          <div class="text-xs mb-1">
            Connections: {nodes.get(selectedFromChain)?.connections || 0}
          </div>
          <div class="text-xs">
            Total Transfers: {nodes.get(selectedFromChain)?.transferCount || 0}
          </div>
        {/if}
        <div class="text-xs mt-2 text-gray-400">
          Drag nodes to reposition • Click to select
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