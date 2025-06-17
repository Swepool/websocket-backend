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
            
            // For proper waterfall stacking, calculate individual stage durations
            const stages = {
              packetRecv: {
                // Stage 1: 0 to packetRecv time
                p5: item.packetRecv.p5,
                median: item.packetRecv.median,
                p95: item.packetRecv.p95
              },
              writeAck: {
                // Stage 2: packetRecv to writeAck time (duration of writeAck stage)
                p5: Math.max(0, item.writeAck.p5 - item.packetRecv.p5),
                median: Math.max(0, item.writeAck.median - item.packetRecv.median),
                p95: Math.max(0, item.writeAck.p95 - item.packetRecv.p95)
              },
              packetAck: {
                // Stage 3: writeAck to packetAck time (duration of final ack stage)
                p5: Math.max(0, item.packetAck.p5 - item.writeAck.p5),
                median: Math.max(0, item.packetAck.median - item.writeAck.median),
                p95: Math.max(0, item.packetAck.p95 - item.writeAck.p95)
              }
            }
            
            return {
              ...item,
              totalLatency,
              stages
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
      if (seconds < 60) return `${seconds.toFixed(1)}s`
      if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`
      return `${(seconds / 3600).toFixed(1)}h`
    }
    
    function formatChainName(name) {
      return name ? name.toLowerCase().replace(/\s+/g, "_") : "unknown"
    }
    
    function getPosition(value) {
      return ((Math.sqrt(value) - sqrtMinLatency) / sqrtLatencyRange) * 100
    }
</script>

<div class="h-full p-0 bg-zinc-950 border border-zinc-800 rounded">
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
          <div class="w-3 h-2 bg-blue-600"></div>
          <span>Packet Recv</span>
        </div>
        <div class="flex items-center gap-1">
          <div class="w-3 h-2 bg-yellow-600"></div>
          <span>Write Ack</span>
        </div>
        <div class="flex items-center gap-1">
          <div class="w-3 h-2 bg-green-600"></div>
          <span>Packet Ack</span>
        </div>
        <div class="flex items-center gap-1">
          <div class="w-1 h-3 bg-zinc-300"></div>
          <span>P5/P95 Range</span>
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
                <div class="w-32 h-3 bg-zinc-700 rounded animate-pulse"></div>
                <div class="flex-1 h-8 bg-zinc-800 rounded animate-pulse"></div>
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
              <!-- Mobile: Stacked Layout -->
              <div class="flex flex-col sm:hidden group">
                <!-- Top row: Route name and total time -->
                <div class="flex justify-between items-start mb-1">
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
                
                <!-- Bottom row: Waterfall chart -->
                <div class="relative h-8 bg-zinc-900 rounded border border-zinc-800 group-hover:border-zinc-600 transition-colors"
                     title="Route: {item.sourceName} → {item.destinationName}&#10;Total: {formatLatency(item.totalLatency.median)} ({formatLatency(item.totalLatency.p5)}-{formatLatency(item.totalLatency.p95)})&#10;Recv: {formatLatency(item.stages.packetRecv.median)}&#10;WriteAck: {formatLatency(item.stages.writeAck.median)}&#10;PacketAck: {formatLatency(item.stages.packetAck.median)}">
                  
                  <!-- Packet Recv Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-blue-600 group-hover:bg-blue-500 transition-colors"
                    style="
                      left: 0%; 
                      width: {getPosition(item.stages.packetRecv.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- Write Ack Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-yellow-600 group-hover:bg-yellow-500 transition-colors"
                    style="
                      left: {getPosition(item.stages.packetRecv.median).toFixed(1)}%; 
                      width: {getPosition(item.stages.writeAck.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- Packet Ack Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-green-600 group-hover:bg-green-500 transition-colors"
                    style="
                      left: {(getPosition(item.stages.packetRecv.median) + getPosition(item.stages.writeAck.median)).toFixed(1)}%; 
                      width: {getPosition(item.stages.packetAck.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- P5 Range Marker -->
                  <div 
                    class="absolute w-0.5 h-8 top-0 bg-zinc-300 opacity-70"
                    style="left: {getPosition(item.totalLatency.p5).toFixed(1)}%"
                  ></div>
                  
                  <!-- P95 Range Marker -->
                  <div 
                    class="absolute w-0.5 h-8 top-0 bg-zinc-300 opacity-70"
                    style="left: {getPosition(item.totalLatency.p95).toFixed(1)}%"
                  ></div>
                </div>
              </div>

              <!-- Desktop: Horizontal Layout -->
              <div class="hidden sm:flex items-center gap-3 group">
                <!-- Route Label -->
                <div class="w-32 text-xs text-zinc-300 truncate flex-shrink-0">
                  <div class="font-medium">
                    {formatChainName(item.sourceName)}
                  </div>
                  <div class="text-zinc-500 text-[10px]">
                    → {formatChainName(item.destinationName)}
                  </div>
                </div>

                <!-- Waterfall Chart Container -->
                <div class="flex-1 relative h-8 bg-zinc-900 rounded border border-zinc-800 group-hover:border-zinc-600 transition-colors"
                     title="Route: {item.sourceName} → {item.destinationName}&#10;Total: {formatLatency(item.totalLatency.median)} ({formatLatency(item.totalLatency.p5)}-{formatLatency(item.totalLatency.p95)})&#10;Recv: {formatLatency(item.stages.packetRecv.median)}&#10;WriteAck: {formatLatency(item.stages.writeAck.median)}&#10;PacketAck: {formatLatency(item.stages.packetAck.median)}">
                  
                  <!-- Packet Recv Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-blue-600 group-hover:bg-blue-500 transition-colors rounded-l"
                    style="
                      left: 0%; 
                      width: {getPosition(item.stages.packetRecv.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- Write Ack Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-yellow-600 group-hover:bg-yellow-500 transition-colors"
                    style="
                      left: {getPosition(item.stages.packetRecv.median).toFixed(1)}%; 
                      width: {getPosition(item.stages.writeAck.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- Packet Ack Stage -->
                  <div 
                    class="absolute h-6 top-1 bg-green-600 group-hover:bg-green-500 transition-colors rounded-r"
                    style="
                      left: {(getPosition(item.stages.packetRecv.median) + getPosition(item.stages.writeAck.median)).toFixed(1)}%; 
                      width: {getPosition(item.stages.packetAck.median).toFixed(1)}%;
                    "
                  ></div>
                  
                  <!-- P5 Range Marker -->
                  <div 
                    class="absolute w-0.5 h-8 top-0 bg-zinc-300 opacity-70"
                    style="left: {getPosition(item.totalLatency.p5).toFixed(1)}%"
                  ></div>
                  
                  <!-- P95 Range Marker -->
                  <div 
                    class="absolute w-0.5 h-8 top-0 bg-zinc-300 opacity-70"
                    style="left: {getPosition(item.totalLatency.p95).toFixed(1)}%"
                  ></div>
                </div>

                <!-- Total Latency Values -->
                <div class="w-20 text-xs text-zinc-400 flex-shrink-0 tabular-nums">
                  <div class="font-medium">
                    {formatLatency(item.totalLatency.median)}
                  </div>
                  <div class="text-[10px] text-zinc-600">
                    {formatLatency(item.totalLatency.p5)}-{formatLatency(item.totalLatency.p95)}
                  </div>
                </div>
              </div>
            {/each}
          </div>

          <!-- Scale Labels -->
          <div class="mt-4 pt-2 border-t border-zinc-800">
            <div class="flex justify-between text-xs text-zinc-600">
              <span>0s</span>
              <span class="text-zinc-500">total latency scale (√)</span>
              <span>{formatLatency(maxLatency)}</span>
            </div>
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
    