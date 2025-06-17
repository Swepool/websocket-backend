<script lang="ts">
import { chains } from "$lib/stores/chains.svelte"
import { Option } from "effect"
import { onMount } from "svelte"
import Card from "./ui/Card.svelte"
import type { TransferListItem } from "@unionlabs/sdk/schema"

// Three.js imports - we'll use CDN for this example
declare global {
  interface Window {
    THREE: any;
  }
}

type EnhancedTransferListItem = TransferListItem & {
  isTestnetTransfer?: boolean
  formattedTimestamp?: string
  routeKey?: string
  senderDisplay?: string
  receiverDisplay?: string
}

interface Node3D {
  id: string
  position: { x: number; y: number; z: number }
  mesh: any // THREE.Mesh
  displayName: string
  transferCount: number
  activity: number
  connections: number
  isSelected: boolean
  originalColor: number
  glowMaterial: any
}

interface Particle3D {
  id: string
  mesh: any // THREE.Mesh
  fromChain: string
  toChain: string
  progress: number
  startPos: { x: number; y: number; z: number }
  endPos: { x: number; y: number; z: number }
  color: number
  trail: any[] // Trail points
}

interface Connection3D {
  source: string
  target: string
  line: any // THREE.Line
  transferCount: number
  isVisible: boolean
}

let {
  transfers = [],
  onChainSelection = () => {},
}: {
  transfers: EnhancedTransferListItem[]
  onChainSelection?: (fromChain: string | null, toChain: string | null) => void
} = $props()

// 3D Configuration
const CONFIG_3D = {
  sphereRadius: 250,
  nodeSize: { min: 4, max: 16 },
  particleSize: 2,
  trailLength: 25,
  cameraDistance: 400,
  animationSpeed: 0.015,
  rotationSpeed: 0.002,
  orbitSpeed: 0.01,
  orbitRadius: 150,
  colors: {
    background: 0x0a0a1a,
    nodeDefault: 0x9333ea,
    nodeActive: 0x06d6a0,
    nodeSelected: 0xfbbf24,
    nodeHub: 0x8b5cf6,
    particleMainnet: 0x06d6a0,
    particleTestnet: 0x3b82f6,
    connection: 0x4c1d95,
    connectionActive: 0x06d6a0,
    trail: 0x8b5cf6,
    stars: 0xffffff,
    nebula: 0x4c1d95
  }
}

// State variables
let containerElement: HTMLElement
let scene: any
let camera: any
let renderer: any
let controls: any
let animationFrame: number
let canvasReady = $state(false)

// 3D Objects
let nodes3D = new Map<string, Node3D>()
let particles3D: Particle3D[] = []
let connections3D = new Map<string, Connection3D>()
let nodeGroup: any
let particleGroup: any
let connectionGroup: any
let starField: any
let nebula: any

// Camera orbit state
let isOrbiting = false
let orbitTarget: { x: number; y: number; z: number } | null = null
let orbitAngle = 0

// Interaction state
let raycaster: any
let mouse = { x: 0, y: 0 }
let hoveredNode: string | null = null
let selectedFromChain: string | null = $state(null)
let selectedToChain: string | null = $state(null)

// Fullscreen state
let isFullscreen = $state(false)
let fullscreenContainer: HTMLElement

// Reactive values
let transfersLength = $derived(transfers.length)
let processedTransferCount = $state(0)

// Load Three.js
function loadThreeJS(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (window.THREE) {
      resolve()
      return
    }

    const script = document.createElement('script')
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js'
    script.onload = () => {
      // Load OrbitControls
      const controlsScript = document.createElement('script')
      controlsScript.src = 'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js'
      controlsScript.onload = () => resolve()
      controlsScript.onerror = reject
      document.head.appendChild(controlsScript)
    }
    script.onerror = reject
    document.head.appendChild(script)
  })
}

function initializeScene() {
  const THREE = window.THREE
  const rect = containerElement.getBoundingClientRect()

  // Scene setup
  scene = new THREE.Scene()
  scene.background = new THREE.Color(CONFIG_3D.colors.background)

  // Camera setup
  camera = new THREE.PerspectiveCamera(
    75,
    rect.width / rect.height,
    0.1,
    2000
  )
  camera.position.set(0, 0, CONFIG_3D.cameraDistance)

  // Renderer setup
  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true })
  renderer.setSize(rect.width, rect.height)
  renderer.setPixelRatio(window.devicePixelRatio)
  renderer.shadowMap.enabled = true
  renderer.shadowMap.type = THREE.PCFSoftShadowMap
  containerElement.appendChild(renderer.domElement)

  // Controls setup
  controls = new (window as any).THREE.OrbitControls(camera, renderer.domElement)
  controls.enableDamping = true
  controls.dampingFactor = 0.05
  controls.maxDistance = 800
  controls.minDistance = 100

  // Create cosmic starfield background
  createStarField()
  createNebula()

  // Cosmic lighting setup
  const ambientLight = new THREE.AmbientLight(0x4a1a5a, 0.4)
  scene.add(ambientLight)

  const directionalLight = new THREE.DirectionalLight(0x8b5cf6, 0.8)
  directionalLight.position.set(200, 200, 100)
  directionalLight.castShadow = true
  scene.add(directionalLight)

  const pointLight1 = new THREE.PointLight(0x06d6a0, 0.6, 800)
  pointLight1.position.set(-200, -200, 200)
  scene.add(pointLight1)

  const pointLight2 = new THREE.PointLight(0x3b82f6, 0.4, 600)
  pointLight2.position.set(200, -200, -200)
  scene.add(pointLight2)

  // Groups for organization
  nodeGroup = new THREE.Group()
  particleGroup = new THREE.Group()
  connectionGroup = new THREE.Group()
  scene.add(nodeGroup)
  scene.add(particleGroup)
  scene.add(connectionGroup)

  // Raycaster for mouse interaction
  raycaster = new THREE.Raycaster()

  // Event listeners
  renderer.domElement.addEventListener('mousemove', onMouseMove)
  renderer.domElement.addEventListener('click', onMouseClick)
  window.addEventListener('resize', onWindowResize)
  document.addEventListener('fullscreenchange', handleFullscreenChange)
  document.addEventListener('keydown', handleKeyPress)
}

function setupNodes() {
  if (!Option.isSome(chains.data)) return

  const THREE = window.THREE
  const chainData = chains.data.value
  if (!chainData?.length) return

  // Clear existing nodes
  nodeGroup.clear()
  nodes3D.clear()

  // Create sphere layout
  chainData.forEach((chain, index) => {
    const phi = Math.acos(-1 + (2 * index) / chainData.length)
    const theta = Math.sqrt(chainData.length * Math.PI) * phi

    const position = {
      x: CONFIG_3D.sphereRadius * Math.cos(theta) * Math.sin(phi),
      y: CONFIG_3D.sphereRadius * Math.cos(phi),
      z: CONFIG_3D.sphereRadius * Math.sin(theta) * Math.sin(phi)
    }

    // Create node geometry and material
    const geometry = new THREE.SphereGeometry(CONFIG_3D.nodeSize.min, 16, 16)
    const material = new THREE.MeshPhongMaterial({
      color: CONFIG_3D.colors.nodeDefault,
      shininess: 100,
      transparent: true,
      opacity: 0.9
    })

    const mesh = new THREE.Mesh(geometry, material)
    mesh.position.set(position.x, position.y, position.z)
    mesh.castShadow = true
    mesh.receiveShadow = true
    mesh.userData = { chainId: chain.universal_chain_id }

    // Glow material for activity
    const glowGeometry = new THREE.SphereGeometry(CONFIG_3D.nodeSize.min * 1.5, 16, 16)
    const glowMaterial = new THREE.MeshBasicMaterial({
      color: CONFIG_3D.colors.nodeActive,
      transparent: true,
      opacity: 0
    })
    const glowMesh = new THREE.Mesh(glowGeometry, glowMaterial)
    glowMesh.position.copy(mesh.position)

    nodeGroup.add(mesh)
    nodeGroup.add(glowMesh)

    nodes3D.set(chain.universal_chain_id, {
      id: chain.universal_chain_id,
      position,
      mesh,
      displayName: chain.display_name || chain.chain_id,
      transferCount: 0,
      activity: 0,
      connections: 0,
      isSelected: false,
      originalColor: CONFIG_3D.colors.nodeDefault,
      glowMaterial: glowMaterial
    })
  })
}

function createParticle3D(transfer: EnhancedTransferListItem) {
  const THREE = window.THREE
  
  if (particles3D.length >= 50) {
    // Remove old particles
    const oldParticle = particles3D.shift()
    if (oldParticle) {
      particleGroup.remove(oldParticle.mesh)
      oldParticle.trail.forEach(trailPoint => particleGroup.remove(trailPoint))
    }
  }

  const sourceId = transfer.source_chain.universal_chain_id
  const targetId = transfer.destination_chain.universal_chain_id

  if (!nodes3D.has(sourceId) || !nodes3D.has(targetId)) return

  const sourceNode = nodes3D.get(sourceId)!
  const targetNode = nodes3D.get(targetId)!

  // Update node properties
  sourceNode.transferCount += 1
  targetNode.transferCount += 1
  sourceNode.activity = Math.min(sourceNode.activity + 1, 10)
  targetNode.activity = Math.min(targetNode.activity + 1, 10)
  sourceNode.connections += 1
  targetNode.connections += 1

  // Update node size and glow
  const sourceSize = Math.min(CONFIG_3D.nodeSize.max, CONFIG_3D.nodeSize.min + sourceNode.connections * 0.5)
  const targetSize = Math.min(CONFIG_3D.nodeSize.max, CONFIG_3D.nodeSize.min + targetNode.connections * 0.5)
  
  sourceNode.mesh.scale.setScalar(sourceSize / CONFIG_3D.nodeSize.min)
  targetNode.mesh.scale.setScalar(targetSize / CONFIG_3D.nodeSize.min)

  // Update glow based on activity
  sourceNode.glowMaterial.opacity = Math.min(sourceNode.activity * 0.1, 0.6)
  targetNode.glowMaterial.opacity = Math.min(targetNode.activity * 0.1, 0.6)

  // Create connection line if it doesn't exist
  const connectionKey = `${sourceId}-${targetId}`
  if (!connections3D.has(connectionKey)) {
    const geometry = new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(sourceNode.position.x, sourceNode.position.y, sourceNode.position.z),
      new THREE.Vector3(targetNode.position.x, targetNode.position.y, targetNode.position.z)
    ])
    const material = new THREE.LineBasicMaterial({
      color: CONFIG_3D.colors.connection,
      transparent: true,
      opacity: 0.3
    })
    const line = new THREE.Line(geometry, material)
    connectionGroup.add(line)

    connections3D.set(connectionKey, {
      source: sourceId,
      target: targetId,
      line,
      transferCount: 1,
      isVisible: true
    })
  } else {
    connections3D.get(connectionKey)!.transferCount += 1
  }

  // Create particle
  const particleColor = transfer.isTestnetTransfer ? 
    CONFIG_3D.colors.particleTestnet : CONFIG_3D.colors.particleMainnet

  const geometry = new THREE.SphereGeometry(CONFIG_3D.particleSize, 8, 8)
  const material = new THREE.MeshBasicMaterial({ 
    color: particleColor,
    transparent: true,
    opacity: 0.8
  })
  const mesh = new THREE.Mesh(geometry, material)
  mesh.position.copy(sourceNode.mesh.position)
  particleGroup.add(mesh)

  const particle: Particle3D = {
    id: transfer.packet_hash,
    mesh,
    fromChain: sourceId,
    toChain: targetId,
    progress: 0,
    startPos: { ...sourceNode.position },
    endPos: { ...targetNode.position },
    color: particleColor,
    trail: []
  }

  particles3D.push(particle)
}

function updateParticles() {
  const THREE = window.THREE
  
  const activeParticles: Particle3D[] = []

  for (const particle of particles3D) {
    particle.progress += CONFIG_3D.animationSpeed

    if (particle.progress >= 1) {
      // Particle reached destination
      particleGroup.remove(particle.mesh)
      particle.trail.forEach(trailPoint => particleGroup.remove(trailPoint))
      
      // Add cosmic impact effect
      const targetNode = nodes3D.get(particle.toChain)
      if (targetNode) {
        targetNode.activity = Math.min(targetNode.activity + 2, 10)
        targetNode.mesh.material.emissive.setHex(0x06d6a0)
        setTimeout(() => {
          targetNode.mesh.material.emissive.setHex(0x000000)
        }, 300)
      }
    } else {
      // Create organic, flowing path with random variations
      const t = particle.progress
      
      // Smooth easing with cosmic flow
      const easedT = 1 - Math.cos(t * Math.PI * 0.5)
      
      const startVec = new THREE.Vector3(particle.startPos.x, particle.startPos.y, particle.startPos.z)
      const endVec = new THREE.Vector3(particle.endPos.x, particle.endPos.y, particle.endPos.z)
      
      // Add random seed based on particle ID for consistent randomness
      const seed = parseInt(particle.id.slice(-6), 16) / 0xffffff
      
      // Create multiple control points for organic curves
      const midPoint1 = startVec.clone().lerp(endVec, 0.3)
      const midPoint2 = startVec.clone().lerp(endVec, 0.7)
      
      // Add random offsets for organic movement
      const randomOffset1 = new THREE.Vector3(
        (Math.sin(seed * 73) - 0.5) * 80,
        (Math.cos(seed * 37) - 0.5) * 80,
        (Math.sin(seed * 19) - 0.5) * 80
      )
      
      const randomOffset2 = new THREE.Vector3(
        (Math.sin(seed * 43) - 0.5) * 60,
        (Math.cos(seed * 67) - 0.5) * 60,
        (Math.sin(seed * 29) - 0.5) * 60
      )
      
      midPoint1.add(randomOffset1)
      midPoint2.add(randomOffset2)
      
      // Create flowing spiral motion
      const spiralIntensity = Math.sin(t * Math.PI * 3 + seed * 10) * 20
      const spiralOffset = new THREE.Vector3(
        Math.cos(t * Math.PI * 6 + seed * 15) * spiralIntensity,
        Math.sin(t * Math.PI * 4 + seed * 12) * spiralIntensity,
        Math.cos(t * Math.PI * 5 + seed * 8) * spiralIntensity * 0.5
      )
      
      // Cubic Bezier curve for smooth organic motion
      const curve = new THREE.CubicBezierCurve3(startVec, midPoint1, midPoint2, endVec)
      const basePosition = curve.getPoint(easedT)
      const finalPosition = basePosition.add(spiralOffset)
      
      particle.mesh.position.copy(finalPosition)

      // Enhanced cosmic trail system
      if (particle.trail.length >= CONFIG_3D.trailLength) {
        const oldTrail = particle.trail.shift()
        if (oldTrail) particleGroup.remove(oldTrail)
      }

      // Add trail points more frequently for smoother cosmic trails
      if (Math.floor(particle.progress * 200) % 3 === 0) {
        const trailIndex = particle.trail.length
        const trailOpacity = 0.9 * Math.pow(1 - trailIndex / CONFIG_3D.trailLength, 2)
        
        const trailSize = CONFIG_3D.particleSize * (1.2 - trailIndex * 0.04)
        const trailGeometry = new THREE.SphereGeometry(trailSize, 8, 8)
        const trailMaterial = new THREE.MeshBasicMaterial({
          color: particle.color,
          transparent: true,
          opacity: trailOpacity
        })
        
        const trailMesh = new THREE.Mesh(trailGeometry, trailMaterial)
        trailMesh.position.copy(finalPosition)
        particle.trail.push(trailMesh)
        particleGroup.add(trailMesh)
      }

      // Dynamic trail fading with cosmic glow
      particle.trail.forEach((trailPoint, index) => {
        const fadeRatio = Math.pow((particle.trail.length - index) / particle.trail.length, 1.5)
        const opacity = 0.9 * fadeRatio * (1 - particle.progress * 0.2)
        trailPoint.material.opacity = Math.max(0, opacity)
        
        // Add subtle glow effect
        if (opacity > 0.3) {
          trailPoint.material.emissive.setHex(particle.color)
          trailPoint.material.emissiveIntensity = opacity * 0.2
        }
      })

      activeParticles.push(particle)
    }
  }

  particles3D = activeParticles
}

function updateNodeActivity() {
  for (const node of nodes3D.values()) {
    // Decay activity
    node.activity = Math.max(node.activity - 0.05, 0)
    node.glowMaterial.opacity = Math.max(node.glowMaterial.opacity - 0.01, 0)
    
    // Update node color based on activity and selection
    if (node.isSelected) {
      node.mesh.material.color.setHex(CONFIG_3D.colors.nodeSelected)
    } else if (node.activity > 5) {
      node.mesh.material.color.setHex(CONFIG_3D.colors.nodeHub)
    } else if (node.activity > 0) {
      node.mesh.material.color.setHex(CONFIG_3D.colors.nodeActive)
    } else {
      node.mesh.material.color.setHex(CONFIG_3D.colors.nodeDefault)
    }
  }
}

function onMouseMove(event: MouseEvent) {
  const rect = renderer.domElement.getBoundingClientRect()
  mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
  mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1

  // Raycast for hover detection
  raycaster.setFromCamera(mouse, camera)
  const intersects = raycaster.intersectObjects(nodeGroup.children.filter((child: any) => child.userData.chainId))

  if (intersects.length > 0) {
    const chainId = intersects[0].object.userData.chainId
    hoveredNode = chainId
    renderer.domElement.style.cursor = 'pointer'
  } else {
    hoveredNode = null
    renderer.domElement.style.cursor = 'default'
  }
}

function onMouseClick() {
  if (!hoveredNode) {
    // Clear selection and stop orbiting
    for (const node of nodes3D.values()) {
      node.isSelected = false
    }
    selectedFromChain = null
    selectedToChain = null
    isOrbiting = false
    orbitTarget = null
    onChainSelection(null, null)
    return
  }

  const selectedNode = nodes3D.get(hoveredNode)!

  if (!selectedFromChain) {
    selectedFromChain = hoveredNode
    selectedNode.isSelected = true
    
    // Start orbiting around the selected chain
    isOrbiting = true
    orbitTarget = { ...selectedNode.position }
    orbitAngle = 0
    
    onChainSelection(hoveredNode, null)
  } else if (!selectedToChain && hoveredNode !== selectedFromChain) {
    selectedToChain = hoveredNode
    selectedNode.isSelected = true
    onChainSelection(selectedFromChain, hoveredNode)
  } else {
    // Reset and select new
    for (const node of nodes3D.values()) {
      node.isSelected = false
    }
    selectedFromChain = hoveredNode
    selectedToChain = null
    selectedNode.isSelected = true
    
    // Start orbiting around the new selection
    isOrbiting = true
    orbitTarget = { ...selectedNode.position }
    orbitAngle = 0
    
    onChainSelection(hoveredNode, null)
  }
}

function createStarField() {
  const THREE = window.THREE
  const starGeometry = new THREE.BufferGeometry()
  const starCount = 2000
  
  const positions = new Float32Array(starCount * 3)
  const colors = new Float32Array(starCount * 3)
  
  for (let i = 0; i < starCount; i++) {
    // Random positions in a large sphere
    const radius = 1000 + Math.random() * 500
    const theta = Math.random() * Math.PI * 2
    const phi = Math.acos(2 * Math.random() - 1)
    
    positions[i * 3] = radius * Math.sin(phi) * Math.cos(theta)
    positions[i * 3 + 1] = radius * Math.sin(phi) * Math.sin(theta)
    positions[i * 3 + 2] = radius * Math.cos(phi)
    
    // Random star colors (white to blue-white)
    const intensity = 0.5 + Math.random() * 0.5
    colors[i * 3] = intensity
    colors[i * 3 + 1] = intensity
    colors[i * 3 + 2] = Math.min(1, intensity + Math.random() * 0.3)
  }
  
  starGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  starGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3))
  
  const starMaterial = new THREE.PointsMaterial({
    size: 2,
    vertexColors: true,
    transparent: true,
    opacity: 0.8
  })
  
  starField = new THREE.Points(starGeometry, starMaterial)
  scene.add(starField)
}

function createNebula() {
  const THREE = window.THREE
  
  // Create multiple nebula clouds
  for (let i = 0; i < 5; i++) {
    const nebulaGeometry = new THREE.SphereGeometry(200 + Math.random() * 300, 32, 32)
    const nebulaMaterial = new THREE.MeshBasicMaterial({
      color: i % 2 === 0 ? CONFIG_3D.colors.nebula : 0x2d1b69,
      transparent: true,
      opacity: 0.05 + Math.random() * 0.05,
      side: THREE.DoubleSide
    })
    
    const nebulaMesh = new THREE.Mesh(nebulaGeometry, nebulaMaterial)
    nebulaMesh.position.set(
      (Math.random() - 0.5) * 1000,
      (Math.random() - 0.5) * 1000,
      (Math.random() - 0.5) * 1000
    )
    
    scene.add(nebulaMesh)
  }
}

function onWindowResize() {
  if (!camera || !renderer || !containerElement) return

  const rect = containerElement.getBoundingClientRect()
  camera.aspect = rect.width / rect.height
  camera.updateProjectionMatrix()
  renderer.setSize(rect.width, rect.height)
}

// Fullscreen functionality
function toggleFullscreen() {
  if (!document.fullscreenElement) {
    enterFullscreen()
  } else {
    exitFullscreen()
  }
}

function enterFullscreen() {
  if (fullscreenContainer && fullscreenContainer.requestFullscreen) {
    fullscreenContainer.requestFullscreen().catch(err => {
      console.error('Error entering fullscreen:', err)
    })
  }
}

function exitFullscreen() {
  if (document.exitFullscreen) {
    document.exitFullscreen().catch(err => {
      console.error('Error exiting fullscreen:', err)
    })
  }
}

function handleFullscreenChange() {
  isFullscreen = !!document.fullscreenElement
  
  // Resize the renderer when entering/exiting fullscreen
  setTimeout(() => {
    onWindowResize()
  }, 100) // Small delay to let the DOM update
}

function handleKeyPress(event: KeyboardEvent) {
  // Press 'F' to toggle fullscreen
  if (event.key === 'f' || event.key === 'F') {
    event.preventDefault()
    toggleFullscreen()
  }
  
  // Press 'Escape' to exit fullscreen (this is usually handled by browser, but good to have)
  if (event.key === 'Escape' && document.fullscreenElement) {
    exitFullscreen()
  }
}

function animate() {
  if (!renderer || !scene || !camera) {
    animationFrame = requestAnimationFrame(animate)
    return
  }

  // Handle camera orbiting around selected chain
  if (isOrbiting && orbitTarget) {
    orbitAngle += CONFIG_3D.orbitSpeed
    
    const orbitX = orbitTarget.x + Math.cos(orbitAngle) * CONFIG_3D.orbitRadius
    const orbitY = orbitTarget.y + Math.sin(orbitAngle * 0.3) * 30 // Gentle vertical movement
    const orbitZ = orbitTarget.z + Math.sin(orbitAngle) * CONFIG_3D.orbitRadius
    
    camera.position.set(orbitX, orbitY, orbitZ)
    camera.lookAt(orbitTarget.x, orbitTarget.y, orbitTarget.z)
    
    // Disable controls during orbiting
    controls.enabled = false
  } else {
    controls.enabled = true
    controls.update()
  }

  updateParticles()
  updateNodeActivity()

  // Gentle auto-rotation of the entire network
  if (!isOrbiting) {
    nodeGroup.rotation.y += CONFIG_3D.rotationSpeed
    
    // Subtle starfield rotation for cosmic effect
    if (starField) {
      starField.rotation.y += CONFIG_3D.rotationSpeed * 0.1
      starField.rotation.x += CONFIG_3D.rotationSpeed * 0.05
    }
  }

  renderer.render(scene, camera)
  animationFrame = requestAnimationFrame(animate)
}

// Process new transfers
$effect(() => {
  if (canvasReady && transfersLength > processedTransferCount) {
    const newTransfers = transfers.slice(processedTransferCount)
    newTransfers.forEach(transfer => {
      createParticle3D(transfer)
    })
    processedTransferCount = transfers.length
  }
})

// Setup chains when data is available
$effect(() => {
  if (Option.isSome(chains.data) && canvasReady) {
    setupNodes()
  }
})

onMount(async () => {
  try {
    await loadThreeJS()
    initializeScene()
    canvasReady = true
    animate()
  } catch (error) {
    console.error('Failed to load Three.js:', error)
  }

  return () => {
    if (animationFrame) {
      cancelAnimationFrame(animationFrame)
    }
    if (renderer) {
      renderer.dispose()
      containerElement?.removeChild(renderer.domElement)
    }
    window.removeEventListener('resize', onWindowResize)
    document.removeEventListener('fullscreenchange', handleFullscreenChange)
    document.removeEventListener('keydown', handleKeyPress)
  }
})
</script>

<Card class="h-full p-3">
  <div class="relative w-full h-full" bind:this={fullscreenContainer}>
    <div class="relative w-full h-full" bind:this={containerElement}>
      {#if selectedFromChain || selectedToChain}
        <div class="absolute top-0 left-0 z-10 bg-black/95 rounded-lg p-3 text-white min-w-56">
          <div class="text-xs mb-1 text-blue-400">3D Network Sphere</div>
          {#if selectedFromChain && selectedToChain}
            <div class="text-sm font-medium mb-2">
              {nodes3D.get(selectedFromChain)?.displayName} → 
              {nodes3D.get(selectedToChain)?.displayName}
            </div>
            <div class="text-xs mb-1">
              Transfers: {connections3D.get(`${selectedFromChain}-${selectedToChain}`)?.transferCount || 
                         connections3D.get(`${selectedToChain}-${selectedFromChain}`)?.transferCount || 0}
            </div>
            <div class="text-xs">
              3D Distance: {(() => {
                const from = nodes3D.get(selectedFromChain)
                const to = nodes3D.get(selectedToChain)
                if (!from || !to) return '0'
                const dx = from.position.x - to.position.x
                const dy = from.position.y - to.position.y
                const dz = from.position.z - to.position.z
                return Math.sqrt(dx*dx + dy*dy + dz*dz).toFixed(0)
              })()}
            </div>
          {:else if selectedFromChain}
            <div class="text-sm font-medium mb-2">
              {nodes3D.get(selectedFromChain)?.displayName}
            </div>
            <div class="text-xs mb-1">
              Connections: {nodes3D.get(selectedFromChain)?.connections || 0}
            </div>
            <div class="text-xs mb-1">
              Activity: {nodes3D.get(selectedFromChain)?.activity.toFixed(1) || 0}
            </div>
            <div class="text-xs">
              Transfers: {nodes3D.get(selectedFromChain)?.transferCount || 0}
            </div>
          {/if}
          <div class="text-xs mt-2 text-gray-400">
            Click chain to orbit around it • Drag to navigate • F for fullscreen
          </div>
        </div>
      {/if}

      <!-- Fullscreen button -->
      <button
        class="absolute top-3 right-3 z-20 bg-black/70 hover:bg-black/90 text-white p-2 rounded-lg transition-colors duration-200"
        onclick={toggleFullscreen}
        title={isFullscreen ? 'Exit Fullscreen (F)' : 'Enter Fullscreen (F)'}
      >
        {#if isFullscreen}
          <!-- Exit fullscreen icon -->
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
          </svg>
        {:else}
          <!-- Enter fullscreen icon -->
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4"/>
          </svg>
        {/if}
      </button>

      {#if !canvasReady}
        <div class="absolute inset-0 flex items-center justify-center">
          <div class="text-white text-center">
            <div class="animate-spin w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full mx-auto mb-2"></div>
            <div class="text-sm">Loading 3D Engine...</div>
          </div>
        </div>
      {/if}
    </div>
  </div>
</Card> 