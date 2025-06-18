<script lang="ts">
  // Waterfall latency chart - shows sequential IBC packet latency stages
  
  let { latencyData = [] } = $props()
  
  // State management for filtering
  let selectedSource = $state("all")
  let selectedDestination = $state("all")
  
  // Computed values - Extract unique chains
  const allSources = $derived(latencyData && latencyData.length > 0
    ? [...new Set(latencyData.map(item => item.sourceName).filter(Boolean))].sort()
    : [])
  
  const allDestinations = $derived(latencyData && latencyData.length > 0
    ? [...new Set(latencyData.map(item => item.destinationName).filter(Boolean))].sort()
    : [])
  
  const currentData = $derived(latencyData && latencyData.length > 0 
    ? latencyData
        .filter(item => 
          item.packetRecv && item.writeAck && item.packetAck &&
          item.packetRecv.median !== undefined && 
          item.writeAck.median !== undefined && 
          item.packetAck.median !== undefined)
        .filter(item => selectedSource === "all" || item.sourceName === selectedSource)
        .filter(item => selectedDestination === "all" || item.destinationName === selectedDestination)
        .map(item => {
          // The raw data represents cumulative times from start
          // packetRecv: time until packet received  
          // writeAck: time until write acknowledgment (cumulative)
          // packetAck: time until packet acknowledgment (total end-to-end time)
          
          const totalLatency = {
            p5: item.packetAck.p5,
            median: item.packetAck.median,
            p95: item.packetAck.p95
          }
          
          // Calculate actual stage durations from medians (only medians can be subtracted)
          const stageDurations = {
            packetRecv: {
              duration: item.packetRecv.median,
              label: "Packet Recv",
              color: "bg-blue-600"
            },
            writeAck: {
              duration: Math.max(0, item.writeAck.median - item.packetRecv.median),
              label: "Write Ack", 
              color: "bg-yellow-600"
            },
            packetAck: {
              duration: Math.max(0, item.packetAck.median - item.writeAck.median),
              label: "Packet Ack",
              color: "bg-green-600"
            }
          }
          
          // Calculate cumulative positions for waterfall
          let cumulativeTime = 0
          const waterfallStages = Object.entries(stageDurations).map(([key, stage]) => {
            const startTime = cumulativeTime
            cumulativeTime += stage.duration
            return {
              key,
              ...stage,
              startTime,
              endTime: cumulativeTime
            }
          })
          
          // Debug log for zero durations
          if (stageDurations.writeAck.duration === 0) {
            console.log(`⚠️ Zero writeAck duration for ${item.sourceName} → ${item.destinationName}`)
          }
          
          return {
            ...item,
            totalLatency,
            stageDurations,
            waterfallStages
          }
        })
        .sort((a, b) => a.totalLatency.median - b.totalLatency.median) // Sort by total median latency
        .slice(0, 12) // Show top 12 for good visibility
    : [])
  
  const hasData = $derived(currentData.length > 0)
  const isLoading = $derived(!hasData && (!latencyData || latencyData.length === 0))
  
  // Calculate scale for waterfall chart
  const dataMaxLatency = $derived(currentData.length > 0 ? Math.max(...currentData.map(item => item.totalLatency.p95)) : 100)
  const dataMinLatency = $derived(0) // Start from 0 for waterfall
  
  // Add padding and use nice bounds
  const minLatency = $derived(0)
  const maxLatency = $derived(dataMaxLatency * 1.1) // 10% padding above
  
  const sqrtMaxLatency = $derived(Math.sqrt(maxLatency))
  const sqrtMinLatency = $derived(Math.sqrt(minLatency))
  const sqrtLatencyRange = $derived(sqrtMaxLatency - sqrtMinLatency || 1)
  
  // Utility functions
  function formatLatency(seconds) {
    if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`
    if (seconds < 10) return `${seconds.toFixed(1)}s`
    if (seconds < 60) return `${seconds.toFixed(0)}s`
    if (seconds < 3600) return `${(seconds / 60).toFixed(1)}min`
    return `${(seconds / 3600).toFixed(1)}h`
  }
  
  function formatChainName(name) {
    return name ? name.toLowerCase().replace(/\s+/g, "_") : "unknown"
  }
  
  function getPosition(value, minValue, maxValue) {
    const range = maxValue - minValue || 1
    return ((value - minValue) / range) * 100
  }
  
  // Calculate positions for total latency visualization (P5, median, P95 only)
  function getTotalLatencyPositions(totalLatency, routeScale) {
    const p5Pos = getPosition(totalLatency.p5, routeScale.min, routeScale.max)
    const medianPos = getPosition(totalLatency.median, routeScale.min, routeScale.max)
    const p95Pos = getPosition(totalLatency.p95, routeScale.min, routeScale.max)
    
    return {
      p5Pos,
      medianPos,
      p95Pos
    }
  }
  
  // Per-route scale - from 0 to total latency max
  function getRouteScale(item) {
    const maxValue = item.totalLatency.p95
    const padding = maxValue * 0.05
    
    return {
      min: 0,
      max: maxValue + padding
    }
  }
  
  // Per-route scrubber state
  let hoveredRoute = $state(-1)
  let scrubberX = $state(0)
  let scrubberTime = $state(0)
  let scrubberOffsetX = $state(0) // Track offset position for accurate alignment
  
  function handleMouseMove(event, routeScale, routeIndex) {
    const rect = event.currentTarget.getBoundingClientRect()
    const x = event.clientX - rect.left
    const percentage = (x / rect.width) * 100
    const time = routeScale.min + ((percentage / 100) * (routeScale.max - routeScale.min))
    
    // Calculate the offset position within the full stage container
    // We need to account for the stage label width and gap
    const stageContainer = event.currentTarget.parentElement
    const stageRect = stageContainer.getBoundingClientRect()
    const chartOffsetX = rect.left - stageRect.left
    const scrubberPixelPos = chartOffsetX + x
    const scrubberContainerPercentage = (scrubberPixelPos / stageRect.width) * 100
    
    hoveredRoute = routeIndex
    scrubberX = percentage // Keep this for the time calculation
    scrubberOffsetX = scrubberContainerPercentage // Use this for positioning
    scrubberTime = time
  }
  
  function handleMouseLeave() {
    hoveredRoute = -1
  }
</script>

<div class="h-full p-0 bg-zinc-950 border border-zinc-800">
<div class="flex flex-col h-full font-mono">
  <!-- Terminal Header -->
  <header class="flex items-center justify-between p-2 border-b border-zinc-800">
    <div class="flex items-center space-x-2">
      <span class="text-zinc-500 text-xs">$</span>
      <h3 class="text-xs text-zinc-300 font-semibold">latency-waterfall</h3>
      <span class="text-zinc-600 text-xs">--stages=all</span>
      {#if selectedSource !== "all"}
        <span class="text-zinc-600 text-xs">--source={formatChainName(selectedSource)}</span>
      {/if}
      {#if selectedDestination !== "all"}
        <span class="text-zinc-600 text-xs">--dest={formatChainName(selectedDestination)}</span>
      {/if}
    </div>
    <div class="text-xs text-zinc-500">
      {#if isLoading}
        loading...
      {:else if !hasData}
        no data yet
      {/if}
    </div>
  </header>

  <!-- Controls -->
  <div class="pt-2 px-2">
    <!-- Chart Guide -->
    <div class="mb-3 p-2 bg-zinc-900 border border-zinc-800">
      <div class="text-xs text-zinc-400 mb-2 font-mono">waterfall chart elements:</div>
      <div class="flex items-center justify-between gap-6 text-[10px] text-zinc-500">
        <div class="flex items-center gap-1">
          <div class="relative w-6 h-3">
            <div class="absolute w-6 h-0.5 top-1.5 bg-zinc-500"></div>
            <div class="absolute w-0.5 h-1 left-0 top-1 bg-zinc-500"></div>
            <div class="absolute w-0.5 h-1 right-0 top-1 bg-zinc-500"></div>
          </div>
          <span>total range<br/>(P5-P95)</span>
        </div>
        <div class="flex items-center gap-1">
          <div class="w-0.5 h-3 bg-white"></div>
          <span>median<br/>(center)</span>
        </div>
        <div class="flex items-center gap-1">
          <div class="w-6 h-2 bg-blue-600"></div>
          <span>stage bars<br/>(durations)</span>
        </div>
      </div>
    </div>
    
    <!-- Filters -->
    <div class="flex flex-col sm:flex-row gap-2 mb-2">
      <!-- Source Filter -->
      <div class="flex items-center gap-1">
        <span class="text-zinc-600 text-xs font-mono">source:</span>
        <select 
          bind:value={selectedSource}
          class="px-2 py-1 text-xs font-mono bg-zinc-900 border border-zinc-700 text-zinc-300 hover:border-zinc-600 focus:border-zinc-500 focus:outline-none transition-colors"
        >
          <option value="all">all</option>
          {#each allSources as source}
            <option value={source}>{formatChainName(source)}</option>
          {/each}
        </select>
      </div>
      
      <!-- Destination Filter -->
      <div class="flex items-center gap-1">
        <span class="text-zinc-600 text-xs font-mono">destination:</span>
        <select 
          bind:value={selectedDestination}
          class="px-2 py-1 text-xs font-mono bg-zinc-900 border border-zinc-700 text-zinc-300 hover:border-zinc-600 focus:border-zinc-500 focus:outline-none transition-colors"
        >
          <option value="all">all</option>
          {#each allDestinations as destination}
            <option value={destination}>{formatChainName(destination)}</option>
          {/each}
        </select>
      </div>
      
      <!-- Clear Filters -->
      {#if selectedSource !== "all" || selectedDestination !== "all"}
        <button
          class="px-2 py-1 text-xs font-mono border border-zinc-700 bg-zinc-900 text-zinc-400 hover:border-zinc-600 hover:text-zinc-300 transition-colors"
          on:click={() => { selectedSource = "all"; selectedDestination = "all"; }}
          title="Clear all filters"
        >
          clear
        </button>
      {/if}
    </div>
    
    <!-- Legend -->
    <div class="flex items-center gap-4 text-xs text-zinc-500 mb-1">
      <div class="flex items-center gap-1">
        <div class="relative w-5 h-4">
          <div class="absolute w-5 h-0.5 top-2 bg-zinc-500"></div>
          <div class="absolute w-0.5 h-4 left-2.5 bg-white"></div>
        </div>
        <span>Total Latency</span>
      </div>
      <div class="flex items-center gap-1">
        <div class="w-4 h-2 bg-blue-600 border border-zinc-500"></div>
        <span>Packet Recv</span>
      </div>
      <div class="flex items-center gap-1">
        <div class="w-4 h-2 bg-yellow-600 border border-zinc-500"></div>
        <span>Write Ack</span>
      </div>
      <div class="flex items-center gap-1">
        <div class="w-4 h-2 bg-green-600 border border-zinc-500"></div>
        <span>Packet Ack</span>
      </div>
    </div>
  </div>

  <!-- Waterfall Chart -->
  <main class="flex-1 flex flex-col p-2 min-h-0">
    <div class="text-xs text-zinc-500 font-mono font-medium mb-2">
      waterfall_latency: {currentData.length} routes
      {#if selectedSource !== "all" || selectedDestination !== "all"}
        <span class="text-zinc-600">
          (filtered from {latencyData?.length || 0} total)
        </span>
      {/if}
    </div>

    <div class="flex-1 overflow-y-auto">
      {#if isLoading}
        <!-- Loading State -->
        <div class="space-y-3">
          {#each Array(8) as _, index}
            <div class="flex items-center gap-3">
              <div class="w-32 h-3 bg-zinc-700 animate-pulse"></div>
              <div class="flex-1 h-8 bg-zinc-800 animate-pulse"></div>
            </div>
          {/each}
        </div>
      {:else if !hasData}
        <!-- No Data State -->
        <div class="flex-1 flex items-center justify-center">
          <div class="text-center">
            <div class="text-zinc-600 font-mono">no_latency_data</div>
            <div class="text-zinc-700 text-xs mt-1">waiting for api fix...</div>
          </div>
        </div>
      {:else}
        <!-- Waterfall Data -->
        <div class="space-y-3">
          {#each currentData as item, index}
            {@const routeScale = getRouteScale(item)}
            {@const totalLatencyPositions = getTotalLatencyPositions(item.totalLatency, routeScale)}
            
            <!-- Mobile: Stacked Layout -->
            <div class="flex flex-col sm:hidden group mb-4">
              <!-- Route Header -->
              <div class="flex justify-between items-start mb-2">
                <div class="text-xs text-zinc-300 truncate flex-shrink-0">
                  <div class="font-medium">
                    {formatChainName(item.sourceName)}
                  </div>
                  <div class="text-zinc-500 text-[10px]">
                    → {formatChainName(item.destinationName)}
                  </div>
                </div>
                
                <div class="text-xs text-zinc-400 flex-shrink-0 tabular-nums text-right">
                  <div class="font-medium">
                    {formatLatency(item.totalLatency.median)}
                  </div>
                  <div class="text-[10px] text-zinc-600">
                    {formatLatency(item.totalLatency.p5)}-{formatLatency(item.totalLatency.p95)}
                  </div>
                </div>
              </div>
              
              <!-- Total Latency Box Plot -->
              <div class="mb-2">
                <div class="flex items-center gap-2">
                  <div class="w-12 text-[10px] text-zinc-500 font-mono">total</div>
                  <div 
                    class="flex-1 relative h-8 bg-zinc-900 border border-zinc-800 cursor-crosshair"
                    on:mousemove={(e) => handleMouseMove(e, routeScale, index)}
                    on:mouseleave={handleMouseLeave}
                  >
                    <!-- Whisker Line (P5 to P95) -->
                    <div 
                      class="absolute h-0.5 top-4 bg-zinc-500"
                      style="
                        left: {totalLatencyPositions.p5Pos}%; 
                        width: {Math.max(0.5, totalLatencyPositions.p95Pos - totalLatencyPositions.p5Pos).toFixed(1)}%;
                      "
                    ></div>
                    
                    <!-- Median Line -->
                    <div 
                      class="absolute w-0.5 h-6 top-1 bg-white z-10"
                      style="left: {totalLatencyPositions.medianPos}%"
                      title="Total Latency Median: {formatLatency(item.totalLatency.median)}"
                    ></div>
                    
                    <!-- P5 Cap -->
                    <div 
                      class="absolute w-0.5 h-2 top-3 bg-zinc-500"
                      style="left: {totalLatencyPositions.p5Pos}%"
                      title="P5: {formatLatency(item.totalLatency.p5)}"
                    ></div>
                    
                    <!-- P95 Cap -->
                    <div 
                      class="absolute w-0.5 h-2 top-3 bg-zinc-500"
                      style="left: {totalLatencyPositions.p95Pos}%"
                      title="P95: {formatLatency(item.totalLatency.p95)}"
                    ></div>
                  </div>
                  <div class="w-12 text-[10px] text-zinc-400 tabular-nums text-right">
                    {formatLatency(item.totalLatency.median)}
                  </div>
                </div>
              </div>

              <!-- Waterfall Stage Duration Bars -->
              <div class="space-y-1 relative">
                <!-- Hover Scrubber -->
                {#if hoveredRoute === index}
                  <div 
                    class="absolute top-0 bottom-0 w-0.5 bg-white opacity-80 z-20 pointer-events-none"
                    style="left: {scrubberOffsetX}%"
                  ></div>
                  <div 
                    class="absolute top-0 left-0 bg-zinc-900 border border-zinc-700 px-2 py-1 text-[10px] text-white font-mono z-30 pointer-events-none"
                    style="left: {Math.min(scrubberOffsetX, 80)}%; transform: translateX(-50%)"
                  >
                    {formatLatency(scrubberTime)}
                  </div>
                {/if}
                
                {#each item.waterfallStages as stage}
                  <div class="flex items-center gap-2">
                    <div class="w-12 text-[10px] text-zinc-500 font-mono">
                      {stage.key === 'packetRecv' ? 'recv' : stage.key === 'writeAck' ? 'write' : 'ack'}
                    </div>
                    <div 
                      class="flex-1 relative h-6 bg-zinc-900 border border-zinc-800 cursor-crosshair"
                      on:mousemove={(e) => handleMouseMove(e, routeScale, index)}
                      on:mouseleave={handleMouseLeave}
                    >
                      {#if stage.duration > 0}
                        <!-- Duration Bar -->
                        <div 
                          class="absolute h-4 top-1 {stage.color} border border-zinc-500"
                          style="
                            left: {getPosition(stage.startTime, routeScale.min, routeScale.max).toFixed(1)}%; 
                            width: {Math.max(1, getPosition(stage.endTime, routeScale.min, routeScale.max) - getPosition(stage.startTime, routeScale.min, routeScale.max)).toFixed(1)}%;
                          "
                          title="{stage.label}: {formatLatency(stage.duration)}"
                        ></div>
                      {:else}
                        <!-- Zero duration - show as thin line -->
                        <div 
                          class="absolute w-0.5 h-4 top-1 bg-zinc-400 opacity-60"
                          style="left: {getPosition(stage.startTime, routeScale.min, routeScale.max).toFixed(1)}%"
                          title="{stage.label}: instant (0.0s)"
                        ></div>
                      {/if}
                    </div>
                    <div class="w-12 text-[10px] text-zinc-400 tabular-nums text-right">
                      {stage.duration > 0 ? formatLatency(stage.duration) : '0.0s'}
                    </div>
                  </div>
                {/each}
              </div>
            </div>

            <!-- Desktop: Waterfall Layout -->
            <div class="hidden sm:block group mb-6">
              <!-- Route Header -->
              <div class="flex items-center justify-between mb-2">
                <div class="text-xs text-zinc-300 font-medium">
                  {formatChainName(item.sourceName)} → {formatChainName(item.destinationName)}
                </div>
                <div class="text-xs text-zinc-400 tabular-nums">
                  <span class="font-medium">{formatLatency(item.totalLatency.median)}</span>
                  <span class="text-zinc-600 ml-1">
                    ({formatLatency(item.totalLatency.p5)}-{formatLatency(item.totalLatency.p95)})
                  </span>
                </div>
              </div>
              
              <!-- Total Latency Box Plot -->
              <div class="mb-2">
                <div class="flex items-center gap-3">
                  <div class="w-16 text-xs text-zinc-500 font-mono">total</div>
                  <div 
                    class="flex-1 relative h-8 bg-zinc-900 border border-zinc-800 cursor-crosshair group-hover:border-zinc-600 transition-colors"
                    on:mousemove={(e) => handleMouseMove(e, routeScale, index)}
                    on:mouseleave={handleMouseLeave}
                  >
                    <!-- Whisker Line (P5 to P95) -->
                    <div 
                      class="absolute h-0.5 top-4 bg-zinc-500"
                      style="
                        left: {totalLatencyPositions.p5Pos}%; 
                        width: {Math.max(0.5, totalLatencyPositions.p95Pos - totalLatencyPositions.p5Pos).toFixed(1)}%;
                      "
                    ></div>
                    
                    <!-- Median Line -->
                    <div 
                      class="absolute w-0.5 h-6 top-1 bg-white z-10"
                      style="left: {totalLatencyPositions.medianPos}%"
                      title="Total Latency Median: {formatLatency(item.totalLatency.median)}"
                    ></div>
                    
                    <!-- P5 Cap -->
                    <div 
                      class="absolute w-0.5 h-2 top-3 bg-zinc-500"
                      style="left: {totalLatencyPositions.p5Pos}%"
                      title="P5: {formatLatency(item.totalLatency.p5)}"
                    ></div>
                    
                    <!-- P95 Cap -->
                    <div 
                      class="absolute w-0.5 h-2 top-3 bg-zinc-500"
                      style="left: {totalLatencyPositions.p95Pos}%"
                      title="P95: {formatLatency(item.totalLatency.p95)}"
                    ></div>
                  </div>
                  <div class="w-16 text-xs text-zinc-400 tabular-nums text-right">
                    {formatLatency(item.totalLatency.median)}
                  </div>
                </div>
              </div>

              <!-- Waterfall Stage Duration Bars -->
              <div class="space-y-1 relative">
                <!-- Hover Scrubber -->
                {#if hoveredRoute === index}
                  <div 
                    class="absolute top-0 bottom-0 w-0.5 bg-white opacity-80 z-20 pointer-events-none"
                    style="left: {scrubberOffsetX}%"
                  ></div>
                  <div 
                    class="absolute top-0 left-0 bg-zinc-900 border border-zinc-700 px-2 py-1 text-[10px] text-white font-mono z-30 pointer-events-none"
                    style="left: {Math.min(scrubberOffsetX, 80)}%; transform: translateX(-50%)"
                  >
                    {formatLatency(scrubberTime)}
                  </div>
                {/if}
                
                {#each item.waterfallStages as stage}
                  <div class="flex items-center gap-3">
                    <div class="w-16 text-xs text-zinc-500 font-mono">
                      {stage.key === 'packetRecv' ? 'recv' : stage.key === 'writeAck' ? 'write' : 'ack'}
                    </div>
                    <div 
                      class="flex-1 relative h-6 bg-zinc-900 border border-zinc-800 cursor-crosshair"
                      on:mousemove={(e) => handleMouseMove(e, routeScale, index)}
                      on:mouseleave={handleMouseLeave}
                    >
                      {#if stage.duration > 0}
                        <!-- Duration Bar -->
                        <div 
                          class="absolute h-4 top-1 {stage.color} border border-zinc-500"
                          style="
                            left: {getPosition(stage.startTime, routeScale.min, routeScale.max).toFixed(1)}%; 
                            width: {Math.max(1, getPosition(stage.endTime, routeScale.min, routeScale.max) - getPosition(stage.startTime, routeScale.min, routeScale.max)).toFixed(1)}%;
                          "
                          title="{stage.label}: {formatLatency(stage.duration)}"
                        ></div>
                      {:else}
                        <!-- Zero duration - show as thin line -->
                        <div 
                          class="absolute w-0.5 h-4 top-1 bg-zinc-400 opacity-60"
                          style="left: {getPosition(stage.startTime, routeScale.min, routeScale.max).toFixed(1)}%"
                          title="{stage.label}: instant (0.0s)"
                        ></div>
                      {/if}
                    </div>
                    <div class="w-16 text-xs text-zinc-400 tabular-nums text-right">
                      {stage.duration > 0 ? formatLatency(stage.duration) : '0.0s'}
                    </div>
                  </div>
                {/each}
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  </main>
</div>
</div>

<style>
/* Custom scrollbar styling */
.overflow-y-auto::-webkit-scrollbar {
width: 4px;
}

.overflow-y-auto::-webkit-scrollbar-track {
background: #27272a;
}

.overflow-y-auto::-webkit-scrollbar-thumb {
background: #52525b;
border-radius: 2px;
}

.overflow-y-auto::-webkit-scrollbar-thumb:hover {
background: #71717a;
}
</style>
  
  