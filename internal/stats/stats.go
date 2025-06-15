package stats

import (
	"sort"
	"sync"
	"time"
	"websocket-backend/internal/models"
)

// RouteStats represents statistics for a specific route
type RouteStats struct {
	Count       int64   `json:"count"`
	CountChange float64 `json:"countChange"`
	FromChain   string  `json:"fromChain"`
	ToChain     string  `json:"toChain"`
	FromName    string  `json:"fromName"`
	ToName      string  `json:"toName"`
}

// WalletStats represents statistics for a wallet address
type WalletStats struct {
	Count        int64     `json:"count"`
	Address      string    `json:"address"`
	LastActivity time.Time `json:"lastActivity"`
}

// TransferRates represents current transfer rate statistics
type TransferRates struct {
	TxPerMinute         float64          `json:"txPerMinute"`
	TxPerHour           float64          `json:"txPerHour"`
	TxPerDay            float64          `json:"txPerDay"`
	TxPer7Days          float64          `json:"txPer7Days"`
	TxPer14Days         float64          `json:"txPer14Days"`
	TxPer30Days         float64          `json:"txPer30Days"`
	// Percentage changes from previous periods
	TxPerMinuteChange   float64          `json:"txPerMinuteChange"`
	TxPerHourChange     float64          `json:"txPerHourChange"`
	TxPerDayChange      float64          `json:"txPerDayChange"`
	TxPer7DaysChange    float64          `json:"txPer7DaysChange"`
	TxPer14DaysChange   float64          `json:"txPer14DaysChange"`
	TxPer30DaysChange   float64          `json:"txPer30DaysChange"`

	TotalTracked        int64            `json:"totalTracked"`
	DataAvailability    DataAvailability `json:"dataAvailability"`
	ServerUptimeSeconds float64          `json:"serverUptimeSeconds"`
}

// ActiveWalletRates represents active wallet rate statistics
type ActiveWalletRates struct {
	SendersLastMin      int `json:"sendersLastMin"`
	SendersLastHour     int `json:"sendersLastHour"`
	SendersLastDay      int `json:"sendersLastDay"`
	SendersLast7d       int `json:"sendersLast7d"`
	SendersLast14d      int `json:"sendersLast14d"`
	SendersLast30d      int `json:"sendersLast30d"`
	// Percentage changes for senders
	SendersLastMinChange    float64 `json:"sendersLastMinChange"`
	SendersLastHourChange   float64 `json:"sendersLastHourChange"`
	SendersLastDayChange    float64 `json:"sendersLastDayChange"`
	SendersLast7dChange     float64 `json:"sendersLast7dChange"`
	SendersLast14dChange    float64 `json:"sendersLast14dChange"`
	SendersLast30dChange    float64 `json:"sendersLast30dChange"`
	ReceiversLastMin    int `json:"receiversLastMin"`
	ReceiversLastHour   int `json:"receiversLastHour"`
	ReceiversLastDay    int `json:"receiversLastDay"`
	ReceiversLast7d     int `json:"receiversLast7d"`
	ReceiversLast14d    int `json:"receiversLast14d"`
	ReceiversLast30d    int `json:"receiversLast30d"`
	// Percentage changes for receivers
	ReceiversLastMinChange  float64 `json:"receiversLastMinChange"`
	ReceiversLastHourChange float64 `json:"receiversLastHourChange"`
	ReceiversLastDayChange  float64 `json:"receiversLastDayChange"`
	ReceiversLast7dChange   float64 `json:"receiversLast7dChange"`
	ReceiversLast14dChange  float64 `json:"receiversLast14dChange"`
	ReceiversLast30dChange  float64 `json:"receiversLast30dChange"`
	// Totals
	TotalLastMin        int `json:"totalLastMin"`
	TotalLastHour       int `json:"totalLastHour"`
	TotalLastDay        int `json:"totalLastDay"`
	TotalLast7d         int `json:"totalLast7d"`
	TotalLast14d        int `json:"totalLast14d"`
	TotalLast30d        int `json:"totalLast30d"`
	// Percentage changes for total active wallets
	TotalLastMinChange      float64 `json:"totalLastMinChange"`
	TotalLastHourChange     float64 `json:"totalLastHourChange"`
	TotalLastDayChange      float64 `json:"totalLastDayChange"`
	TotalLast7dChange       float64 `json:"totalLast7dChange"`
	TotalLast14dChange      float64 `json:"totalLast14dChange"`
	TotalLast30dChange      float64 `json:"totalLast30dChange"`
	
	UniqueSendersTotal  int `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int `json:"uniqueReceiversTotal"`
	UniqueTotalWallets  int `json:"uniqueTotalWallets"`
	ServerUptimeSeconds float64 `json:"serverUptimeSeconds"`
}

// ChartData represents basic chart data structure
type ChartData struct {
	CurrentRates    TransferRates    `json:"currentRates"`
	PopularRoutes   []*RouteStats    `json:"popularRoutes"`
	ActiveSenders   []*WalletStats   `json:"activeSenders"`
	ActiveReceivers []*WalletStats   `json:"activeReceivers"`
}

// DataAvailability represents data availability for different time periods
type DataAvailability struct {
	HasMinute bool `json:"hasMinute"`
	HasHour   bool `json:"hasHour"`
	HasDay    bool `json:"hasDay"`
	Has7Days  bool `json:"has7Days"`
	Has14Days bool `json:"has14Days"`
	Has30Days bool `json:"has30Days"`
}

// EnhancedChartData represents enhanced chart data with additional fields
type EnhancedChartData struct {
	CurrentRates             TransferRates                    `json:"currentRates"`
	ActiveWalletRates        ActiveWalletRates                `json:"activeWalletRates"`
	PopularRoutes            []*RouteStats                    `json:"popularRoutes"`
	PopularRoutesTimeScale   map[string][]*RouteStats         `json:"popularRoutesTimeScale"`
	ActiveSenders            []*WalletStats                   `json:"activeSenders"`
	ActiveReceivers          []*WalletStats                   `json:"activeReceivers"`
	ActiveSendersTimeScale   map[string][]*WalletStats        `json:"activeSendersTimeScale"`
	ActiveReceiversTimeScale map[string][]*WalletStats        `json:"activeReceiversTimeScale"`
	DataAvailability         DataAvailability                 `json:"dataAvailability"`
}

// Time-based data structures for analytics
type RouteTimeData struct {
	Timestamp time.Time `json:"timestamp"`
	RouteKey  string    `json:"routeKey"`
	Count     int64     `json:"count"`
}

type WalletTimeData struct {
	Timestamp time.Time `json:"timestamp"`
	Sender    string    `json:"sender"`
	Receiver  string    `json:"receiver"`
}

type TimeBucket struct {
	Timestamp     time.Time `json:"timestamp"`
	TransferCount int64     `json:"transferCount"`
	SenderCount   int64     `json:"senderCount"`
	ReceiverCount int64     `json:"receiverCount"`
	TotalCount    int64     `json:"totalCount"`
	Senders       []string  `json:"senders"`
	Receivers     []string  `json:"receivers"`
}

// PreviousPeriodData stores data from previous periods for comparison
type PreviousPeriodData struct {
	TransferRates     TransferRates            `json:"transferRates"`
	ActiveWalletRates ActiveWalletRates        `json:"activeWalletRates"`
	PopularRoutes     map[string][]*RouteStats `json:"popularRoutes"` // timeframe -> routes
	Timestamp         time.Time                `json:"timestamp"`
}

// EnhancedCollector provides enhanced statistics collection
type EnhancedCollector struct {
	mu               sync.RWMutex
	routeStats       map[string]*RouteStats
	senderStats      map[string]*WalletStats
	receiverStats    map[string]*WalletStats
	routeTimeData    []RouteTimeData
	walletTimeData   []WalletTimeData
	timeBuckets      []TimeBucket
	totalTransfers   int64
	startTime        time.Time
	lastUpdateTime   time.Time
	// Previous period tracking for percentage calculations
	previousMinute   *PreviousPeriodData
	previousHour     *PreviousPeriodData
	previousDay      *PreviousPeriodData
	previous7Days    *PreviousPeriodData
	previous14Days   *PreviousPeriodData
	previous30Days   *PreviousPeriodData
	// Timers for period rotation
	lastMinuteCheck  time.Time
	lastHourCheck    time.Time
	lastDayCheck     time.Time
	last7DaysCheck   time.Time
	last14DaysCheck  time.Time
	last30DaysCheck  time.Time
}

// NewEnhancedCollector creates a new enhanced statistics collector
func NewEnhancedCollector() *EnhancedCollector {
	now := time.Now()
	return &EnhancedCollector{
		routeStats:       make(map[string]*RouteStats),
		senderStats:      make(map[string]*WalletStats),
		receiverStats:    make(map[string]*WalletStats),
		routeTimeData:    make([]RouteTimeData, 0),
		walletTimeData:   make([]WalletTimeData, 0),
		timeBuckets:      make([]TimeBucket, 0),
		startTime:        now,
		lastUpdateTime:   now,
		// Initialize period check timers
		lastMinuteCheck:  now,
		lastHourCheck:    now,
		lastDayCheck:     now,
		last7DaysCheck:   now,
		last14DaysCheck:  now,
		last30DaysCheck:  now,
	}
}

// UpdateTransferStats updates statistics with new transfers
func (ec *EnhancedCollector) UpdateTransferStats(transfers []models.Transfer) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	now := time.Now()
	ec.lastUpdateTime = now

	// Check for period rotations before processing new transfers
	ec.checkAndRotatePeriods(now)

	for _, transfer := range transfers {
		ec.totalTransfers++

		// Update route stats
		routeKey := transfer.SourceChain.UniversalChainID + "->" + transfer.DestinationChain.UniversalChainID
		if routeStats, exists := ec.routeStats[routeKey]; exists {
			routeStats.Count++
		} else {
			ec.routeStats[routeKey] = &RouteStats{
				Count:     1,
				FromChain: transfer.SourceChain.UniversalChainID,
				ToChain:   transfer.DestinationChain.UniversalChainID,
				FromName:  transfer.SourceChain.DisplayName,
				ToName:    transfer.DestinationChain.DisplayName,
			}
		}

		// Update sender stats (use formatted address for display)
		senderAddress := transfer.SenderDisplay
		if senderAddress == "" {
			senderAddress = transfer.SenderCanonical // Fallback to canonical if no formatted address
		}
		if senderStats, exists := ec.senderStats[senderAddress]; exists {
			senderStats.Count++
			// Update LastActivity to the most recent transfer timestamp
			if transfer.TransferSendTimestamp.After(senderStats.LastActivity) {
				senderStats.LastActivity = transfer.TransferSendTimestamp
			}
		} else {
			ec.senderStats[senderAddress] = &WalletStats{
				Count:        1,
				Address:      senderAddress,
				LastActivity: transfer.TransferSendTimestamp,
			}
		}

		// Update receiver stats (use formatted address for display)
		receiverAddress := transfer.ReceiverDisplay
		if receiverAddress == "" {
			receiverAddress = transfer.ReceiverCanonical // Fallback to canonical if no formatted address
		}
		if receiverStats, exists := ec.receiverStats[receiverAddress]; exists {
			receiverStats.Count++
			// Update LastActivity to the most recent transfer timestamp
			if transfer.TransferSendTimestamp.After(receiverStats.LastActivity) {
				receiverStats.LastActivity = transfer.TransferSendTimestamp
			}
		} else {
			ec.receiverStats[receiverAddress] = &WalletStats{
				Count:        1,
				Address:      receiverAddress,
				LastActivity: transfer.TransferSendTimestamp,
			}
		}

		// Add to time buckets for transfer counting
		ec.timeBuckets = append(ec.timeBuckets, TimeBucket{
			Timestamp:     transfer.TransferSendTimestamp,
			TransferCount: 1,
			SenderCount:   1,
			ReceiverCount: 1,
			TotalCount:    1,
			Senders:       []string{senderAddress},
			Receivers:     []string{receiverAddress},
		})

		// Add wallet activity to time-based tracking
		ec.walletTimeData = append(ec.walletTimeData, WalletTimeData{
			Timestamp: transfer.TransferSendTimestamp,
			Sender:    senderAddress,
			Receiver:  receiverAddress,
		})

		// Add route activity to time-based tracking for timeframe-specific analysis
		ec.routeTimeData = append(ec.routeTimeData, RouteTimeData{
			Timestamp: transfer.TransferSendTimestamp,
			RouteKey:  routeKey,
			Count:     1,
		})
	}

	// Clean up old time data (keep last 30 days)
	cutoff := now.AddDate(0, 0, -30)
	ec.cleanupOldTimeData(cutoff)
}

// cleanupOldTimeData removes time data older than the cutoff
func (ec *EnhancedCollector) cleanupOldTimeData(cutoff time.Time) {
	// Clean up time buckets
	newTimeBuckets := make([]TimeBucket, 0)
	for _, bucket := range ec.timeBuckets {
		if bucket.Timestamp.After(cutoff) {
			newTimeBuckets = append(newTimeBuckets, bucket)
		}
	}
	ec.timeBuckets = newTimeBuckets

	// Clean up route time data
	newRouteTimeData := make([]RouteTimeData, 0)
	for _, data := range ec.routeTimeData {
		if data.Timestamp.After(cutoff) {
			newRouteTimeData = append(newRouteTimeData, data)
		}
	}
	ec.routeTimeData = newRouteTimeData

	// Clean up wallet time data
	newWalletTimeData := make([]WalletTimeData, 0)
	for _, data := range ec.walletTimeData {
		if data.Timestamp.After(cutoff) {
			newWalletTimeData = append(newWalletTimeData, data)
		}
	}
	ec.walletTimeData = newWalletTimeData
}

// GetChartData returns basic chart data
func (ec *EnhancedCollector) GetChartData() ChartData {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	return ChartData{
		CurrentRates:    ec.calculateTransferRates(),
		PopularRoutes:   ec.getTopRoutes(10),
		ActiveSenders:   ec.getTopSenders(10),
		ActiveReceivers: ec.getTopReceivers(10),
	}
}

// GetEnhancedChartData returns enhanced chart data
func (ec *EnhancedCollector) GetEnhancedChartData() EnhancedChartData {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	return EnhancedChartData{
		CurrentRates:             ec.calculateTransferRates(),
		ActiveWalletRates:        ec.calculateActiveWalletRates(),
		PopularRoutes:            ec.getTopRoutes(10),
		PopularRoutesTimeScale:   ec.getPopularRoutesTimeScale(),
		ActiveSenders:            ec.getTopSenders(10),
		ActiveReceivers:          ec.getTopReceivers(10),
		ActiveSendersTimeScale:   ec.getActiveSendersTimeScale(),
		ActiveReceiversTimeScale: ec.getActiveReceiversTimeScale(),
		DataAvailability:         ec.calculateDataAvailability(),
	}
}

// calculateDataAvailability determines what time periods have data available
// Based on server uptime - we can only claim to have data for periods we've been running
func (ec *EnhancedCollector) calculateDataAvailability() DataAvailability {
	now := time.Now()
	uptime := now.Sub(ec.startTime)
	
	return DataAvailability{
		HasMinute: uptime >= time.Minute && ec.countTransfersInPeriod(now.Add(-time.Minute)) > 0,
		HasHour:   uptime >= time.Hour && ec.countTransfersInPeriod(now.Add(-time.Hour)) > 0,
		HasDay:    uptime >= 24*time.Hour && ec.countTransfersInPeriod(now.Add(-24*time.Hour)) > 0,
		Has7Days:  uptime >= 7*24*time.Hour && ec.countTransfersInPeriod(now.AddDate(0, 0, -7)) > 0,
		Has14Days: uptime >= 14*24*time.Hour && ec.countTransfersInPeriod(now.AddDate(0, 0, -14)) > 0,
		Has30Days: uptime >= 30*24*time.Hour && ec.countTransfersInPeriod(now.AddDate(0, 0, -30)) > 0,
	}
}

// calculateTransferRates calculates transfer rates for different time periods
func (ec *EnhancedCollector) calculateTransferRates() TransferRates {
	now := time.Now()
	
	minute := float64(ec.countTransfersInPeriod(now.Add(-time.Minute)))
	hour := float64(ec.countTransfersInPeriod(now.Add(-time.Hour)))
	day := float64(ec.countTransfersInPeriod(now.Add(-24 * time.Hour)))
	sevenDays := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -7)))
	fourteenDays := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -14)))
	thirtyDays := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -30)))

	// Calculate percentage changes from previous periods
	var minuteChange, hourChange, dayChange, sevenDaysChange, fourteenDaysChange, thirtyDaysChange float64

	if ec.previousMinute != nil {
		minuteChange = ec.calculatePercentageChange(minute, ec.previousMinute.TransferRates.TxPerMinute)
	}
	if ec.previousHour != nil {
		hourChange = ec.calculatePercentageChange(hour, ec.previousHour.TransferRates.TxPerHour)
	}
	if ec.previousDay != nil {
		dayChange = ec.calculatePercentageChange(day, ec.previousDay.TransferRates.TxPerDay)
	}
	if ec.previous7Days != nil {
		sevenDaysChange = ec.calculatePercentageChange(sevenDays, ec.previous7Days.TransferRates.TxPer7Days)
	}
	if ec.previous14Days != nil {
		fourteenDaysChange = ec.calculatePercentageChange(fourteenDays, ec.previous14Days.TransferRates.TxPer14Days)
	}
	if ec.previous30Days != nil {
		thirtyDaysChange = ec.calculatePercentageChange(thirtyDays, ec.previous30Days.TransferRates.TxPer30Days)
	}

	return TransferRates{
		TxPerMinute:         minute,
		TxPerHour:           hour,
		TxPerDay:            day,
		TxPer7Days:          sevenDays,
		TxPer14Days:         fourteenDays,
		TxPer30Days:         thirtyDays,
		// Percentage changes
		TxPerMinuteChange:   minuteChange,
		TxPerHourChange:     hourChange,
		TxPerDayChange:      dayChange,
		TxPer7DaysChange:    sevenDaysChange,
		TxPer14DaysChange:   fourteenDaysChange,
		TxPer30DaysChange:   thirtyDaysChange,
		TotalTracked:        ec.totalTransfers,
		DataAvailability:    ec.calculateDataAvailability(),
		ServerUptimeSeconds: now.Sub(ec.startTime).Seconds(),
	}
}

// calculateActiveWalletRates calculates active wallet rates for different time periods
func (ec *EnhancedCollector) calculateActiveWalletRates() ActiveWalletRates {
	now := time.Now()
	
	// Calculate rolling windows (each window is independent and complete)
	// 1 minute window: wallets active in the last 1 minute
	sendersMin := float64(ec.countActiveSendersInPeriod(now.Add(-time.Minute)))
	// 1 hour window: wallets active in the last 1 hour (full hour, not excluding minute)
	sendersHour := float64(ec.countActiveSendersInPeriod(now.Add(-time.Hour)))
	// 1 day window: wallets active in the last 1 day (full day, not excluding hour)
	sendersDay := float64(ec.countActiveSendersInPeriod(now.Add(-24*time.Hour)))
	// 7 day window: wallets active in the last 7 days
	senders7d := float64(ec.countActiveSendersInPeriod(now.AddDate(0, 0, -7)))
	// 14 day window: wallets active in the last 14 days
	senders14d := float64(ec.countActiveSendersInPeriod(now.AddDate(0, 0, -14)))
	// 30 day window: wallets active in the last 30 days
	senders30d := float64(ec.countActiveSendersInPeriod(now.AddDate(0, 0, -30)))
	
	// Same logic for receivers
	receiversMin := float64(ec.countActiveReceiversInPeriod(now.Add(-time.Minute)))
	receiversHour := float64(ec.countActiveReceiversInPeriod(now.Add(-time.Hour)))
	receiversDay := float64(ec.countActiveReceiversInPeriod(now.Add(-24*time.Hour)))
	receivers7d := float64(ec.countActiveReceiversInPeriod(now.AddDate(0, 0, -7)))
	receivers14d := float64(ec.countActiveReceiversInPeriod(now.AddDate(0, 0, -14)))
	receivers30d := float64(ec.countActiveReceiversInPeriod(now.AddDate(0, 0, -30)))
	
	// Same logic for total wallets
	totalMin := float64(ec.countActiveWalletsInPeriod(now.Add(-time.Minute)))
	totalHour := float64(ec.countActiveWalletsInPeriod(now.Add(-time.Hour)))
	totalDay := float64(ec.countActiveWalletsInPeriod(now.Add(-24*time.Hour)))
	total7d := float64(ec.countActiveWalletsInPeriod(now.AddDate(0, 0, -7)))
	total14d := float64(ec.countActiveWalletsInPeriod(now.AddDate(0, 0, -14)))
	total30d := float64(ec.countActiveWalletsInPeriod(now.AddDate(0, 0, -30)))

	// Calculate percentage changes
	var sendersMinChange, sendersHourChange, sendersDayChange, senders7dChange, senders14dChange, senders30dChange float64
	var receiversMinChange, receiversHourChange, receiversDayChange, receivers7dChange, receivers14dChange, receivers30dChange float64
	var totalMinChange, totalHourChange, totalDayChange, total7dChange, total14dChange, total30dChange float64

	if ec.previousMinute != nil {
		sendersMinChange = ec.calculatePercentageChange(sendersMin, float64(ec.previousMinute.ActiveWalletRates.SendersLastMin))
		receiversMinChange = ec.calculatePercentageChange(receiversMin, float64(ec.previousMinute.ActiveWalletRates.ReceiversLastMin))
		totalMinChange = ec.calculatePercentageChange(totalMin, float64(ec.previousMinute.ActiveWalletRates.TotalLastMin))
	}
	if ec.previousHour != nil {
		sendersHourChange = ec.calculatePercentageChange(sendersHour, float64(ec.previousHour.ActiveWalletRates.SendersLastHour))
		receiversHourChange = ec.calculatePercentageChange(receiversHour, float64(ec.previousHour.ActiveWalletRates.ReceiversLastHour))
		totalHourChange = ec.calculatePercentageChange(totalHour, float64(ec.previousHour.ActiveWalletRates.TotalLastHour))
	}
	if ec.previousDay != nil {
		sendersDayChange = ec.calculatePercentageChange(sendersDay, float64(ec.previousDay.ActiveWalletRates.SendersLastDay))
		receiversDayChange = ec.calculatePercentageChange(receiversDay, float64(ec.previousDay.ActiveWalletRates.ReceiversLastDay))
		totalDayChange = ec.calculatePercentageChange(totalDay, float64(ec.previousDay.ActiveWalletRates.TotalLastDay))
	}
	if ec.previous7Days != nil {
		senders7dChange = ec.calculatePercentageChange(senders7d, float64(ec.previous7Days.ActiveWalletRates.SendersLast7d))
		receivers7dChange = ec.calculatePercentageChange(receivers7d, float64(ec.previous7Days.ActiveWalletRates.ReceiversLast7d))
		total7dChange = ec.calculatePercentageChange(total7d, float64(ec.previous7Days.ActiveWalletRates.TotalLast7d))
	}
	if ec.previous14Days != nil {
		senders14dChange = ec.calculatePercentageChange(senders14d, float64(ec.previous14Days.ActiveWalletRates.SendersLast14d))
		receivers14dChange = ec.calculatePercentageChange(receivers14d, float64(ec.previous14Days.ActiveWalletRates.ReceiversLast14d))
		total14dChange = ec.calculatePercentageChange(total14d, float64(ec.previous14Days.ActiveWalletRates.TotalLast14d))
	}
	if ec.previous30Days != nil {
		senders30dChange = ec.calculatePercentageChange(senders30d, float64(ec.previous30Days.ActiveWalletRates.SendersLast30d))
		receivers30dChange = ec.calculatePercentageChange(receivers30d, float64(ec.previous30Days.ActiveWalletRates.ReceiversLast30d))
		total30dChange = ec.calculatePercentageChange(total30d, float64(ec.previous30Days.ActiveWalletRates.TotalLast30d))
	}
	
	return ActiveWalletRates{
		SendersLastMin:       int(sendersMin),
		SendersLastHour:      int(sendersHour),
		SendersLastDay:       int(sendersDay),
		SendersLast7d:        int(senders7d),
		SendersLast14d:       int(senders14d),
		SendersLast30d:       int(senders30d),
		// Sender percentage changes
		SendersLastMinChange:    sendersMinChange,
		SendersLastHourChange:   sendersHourChange,
		SendersLastDayChange:    sendersDayChange,
		SendersLast7dChange:     senders7dChange,
		SendersLast14dChange:    senders14dChange,
		SendersLast30dChange:    senders30dChange,
		ReceiversLastMin:     int(receiversMin),
		ReceiversLastHour:    int(receiversHour),
		ReceiversLastDay:     int(receiversDay),
		ReceiversLast7d:      int(receivers7d),
		ReceiversLast14d:     int(receivers14d),
		ReceiversLast30d:     int(receivers30d),
		// Receiver percentage changes
		ReceiversLastMinChange:  receiversMinChange,
		ReceiversLastHourChange: receiversHourChange,
		ReceiversLastDayChange:  receiversDayChange,
		ReceiversLast7dChange:   receivers7dChange,
		ReceiversLast14dChange:  receivers14dChange,
		ReceiversLast30dChange:  receivers30dChange,
		TotalLastMin:         int(totalMin),
		TotalLastHour:        int(totalHour),
		TotalLastDay:         int(totalDay),
		TotalLast7d:          int(total7d),
		TotalLast14d:         int(total14d),
		TotalLast30d:         int(total30d),
		// Total wallet percentage changes
		TotalLastMinChange:      totalMinChange,
		TotalLastHourChange:     totalHourChange,
		TotalLastDayChange:      totalDayChange,
		TotalLast7dChange:       total7dChange,
		TotalLast14dChange:      total14dChange,
		TotalLast30dChange:      total30dChange,
		UniqueSendersTotal:   len(ec.senderStats),
		UniqueReceiversTotal: len(ec.receiverStats),
		UniqueTotalWallets:   ec.countTotalUniqueWallets(),
		ServerUptimeSeconds:  now.Sub(ec.startTime).Seconds(),
	}
}

// countTransfersInPeriod counts transfers since the given time
func (ec *EnhancedCollector) countTransfersInPeriod(since time.Time) int64 {
	var count int64
	for _, bucket := range ec.timeBuckets {
		if bucket.Timestamp.After(since) {
			count += bucket.TransferCount
		}
	}
	return count
}

// countActiveWalletsInPeriod counts active wallets since the given time
func (ec *EnhancedCollector) countActiveWalletsInPeriod(since time.Time) int {
	activeWallets := make(map[string]bool)
	
	for _, activity := range ec.walletTimeData {
		if activity.Timestamp.After(since) {
			if activity.Sender != "" {
				activeWallets[activity.Sender] = true
			}
			if activity.Receiver != "" {
				activeWallets[activity.Receiver] = true
			}
		}
	}
	
	return len(activeWallets)
}

// countActiveSendersInPeriod counts active senders since the given time
func (ec *EnhancedCollector) countActiveSendersInPeriod(since time.Time) int {
	activeSenders := make(map[string]bool)
	for _, activity := range ec.walletTimeData {
		if activity.Timestamp.After(since) && activity.Sender != "" {
			activeSenders[activity.Sender] = true
		}
	}
	return len(activeSenders)
}

// countActiveReceiversInPeriod counts active receivers since the given time
func (ec *EnhancedCollector) countActiveReceiversInPeriod(since time.Time) int {
	activeReceivers := make(map[string]bool)
	for _, activity := range ec.walletTimeData {
		if activity.Timestamp.After(since) && activity.Receiver != "" {
			activeReceivers[activity.Receiver] = true
		}
	}
	return len(activeReceivers)
}

// countTotalUniqueWallets counts total unique wallets (senders + receivers, deduplicated)
func (ec *EnhancedCollector) countTotalUniqueWallets() int {
	uniqueWallets := make(map[string]bool)
	
	// Add all sender addresses
	for address := range ec.senderStats {
		uniqueWallets[address] = true
	}
	
	// Add all receiver addresses
	for address := range ec.receiverStats {
		uniqueWallets[address] = true
	}
	
	return len(uniqueWallets)
}

// getTopRoutes returns the top N routes by transfer count
func (ec *EnhancedCollector) getTopRoutes(n int) []*RouteStats {
	routes := make([]*RouteStats, 0, len(ec.routeStats))
	for _, route := range ec.routeStats {
		routes = append(routes, route)
	}
	
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})
	
	if len(routes) > n {
		routes = routes[:n]
	}
	
	return routes
}

// getTopRoutesInPeriod returns the top N routes for a specific time period
func (ec *EnhancedCollector) getTopRoutesInPeriod(since time.Time, n int, timeframeKey string) []*RouteStats {
	routeCounts := make(map[string]*RouteStats)
	
	// Count routes within the time period
	for _, routeData := range ec.routeTimeData {
		if routeData.Timestamp.After(since) {
			if route, exists := routeCounts[routeData.RouteKey]; exists {
				route.Count += routeData.Count
			} else {
				// Get route info from routeStats for display names
				if originalRoute, found := ec.routeStats[routeData.RouteKey]; found {
					routeCounts[routeData.RouteKey] = &RouteStats{
						Count:       routeData.Count,
						CountChange: 0, // Will be calculated below
						FromChain:   originalRoute.FromChain,
						ToChain:     originalRoute.ToChain,
						FromName:    originalRoute.FromName,
						ToName:      originalRoute.ToName,
					}
				}
			}
		}
	}
	
	// Calculate percentage changes from previous period
	ec.calculateRoutePercentageChanges(routeCounts, timeframeKey)
	
	// Convert to slice
	routes := make([]*RouteStats, 0, len(routeCounts))
	for _, route := range routeCounts {
		routes = append(routes, route)
	}
	
	// Sort by count
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})
	
	// Limit to top N
	if len(routes) > n {
		routes = routes[:n]
	}
	
	return routes
}

// checkAndRotatePeriods checks if we need to rotate period data and does so
func (ec *EnhancedCollector) checkAndRotatePeriods(now time.Time) {
	// Check minute rotation (every minute)
	if now.Sub(ec.lastMinuteCheck) >= time.Minute {
		ec.rotatePeriod("minute", now)
		ec.lastMinuteCheck = now
	}
	
	// Check hour rotation (every hour)
	if now.Sub(ec.lastHourCheck) >= time.Hour {
		ec.rotatePeriod("hour", now)
		ec.lastHourCheck = now
	}
	
	// Check day rotation (every day)
	if now.Sub(ec.lastDayCheck) >= 24*time.Hour {
		ec.rotatePeriod("day", now)
		ec.lastDayCheck = now
	}
	
	// Check 7-day rotation (every 7 days)
	if now.Sub(ec.last7DaysCheck) >= 7*24*time.Hour {
		ec.rotatePeriod("7days", now)
		ec.last7DaysCheck = now
	}
	
	// Check 14-day rotation (every 14 days)
	if now.Sub(ec.last14DaysCheck) >= 14*24*time.Hour {
		ec.rotatePeriod("14days", now)
		ec.last14DaysCheck = now
	}
	
	// Check 30-day rotation (every 30 days)
	if now.Sub(ec.last30DaysCheck) >= 30*24*time.Hour {
		ec.rotatePeriod("30days", now)
		ec.last30DaysCheck = now
	}
}

// rotatePeriod saves current data as previous data for the specified period
func (ec *EnhancedCollector) rotatePeriod(period string, timestamp time.Time) {
	// Calculate current rates and wallet stats
	currentTransferRates := ec.calculateTransferRates()
	currentWalletRates := ec.calculateActiveWalletRates()
	currentPopularRoutes := ec.getPopularRoutesTimeScale()
	
	// Create snapshot of current data
	snapshot := &PreviousPeriodData{
		TransferRates:     currentTransferRates,
		ActiveWalletRates: currentWalletRates,
		PopularRoutes:     currentPopularRoutes,
		Timestamp:         timestamp,
	}
	
	// Store in appropriate previous period slot
	switch period {
	case "minute":
		ec.previousMinute = snapshot
	case "hour":
		ec.previousHour = snapshot
	case "day":
		ec.previousDay = snapshot
	case "7days":
		ec.previous7Days = snapshot
	case "14days":
		ec.previous14Days = snapshot
	case "30days":
		ec.previous30Days = snapshot
	}
}

// calculatePercentageChange calculates the percentage change between two values
func (ec *EnhancedCollector) calculatePercentageChange(current, previous float64) float64 {
	if previous == 0 {
		if current == 0 {
			return 0
		}
		return 100 // If we had 0 before and have something now, that's 100% increase
	}
	return ((current - previous) / previous) * 100
}

// calculateRoutePercentageChanges calculates percentage changes for routes
func (ec *EnhancedCollector) calculateRoutePercentageChanges(routeCounts map[string]*RouteStats, timeframeKey string) {
	var previousData *PreviousPeriodData
	
	// Get appropriate previous period data based on timeframe
	switch timeframeKey {
	case "1m":
		previousData = ec.previousMinute
	case "1h":
		previousData = ec.previousHour
	case "1d":
		previousData = ec.previousDay
	case "7d":
		previousData = ec.previous7Days
	case "14d":
		previousData = ec.previous14Days
	case "30d":
		previousData = ec.previous30Days
	}
	
	// If we have previous data, calculate percentage changes
	if previousData != nil && previousData.PopularRoutes != nil {
		previousRoutes := previousData.PopularRoutes[timeframeKey]
		if previousRoutes != nil {
			// Create a map of previous route counts for quick lookup
			previousRouteCounts := make(map[string]int64)
			for _, prevRoute := range previousRoutes {
				routeKey := prevRoute.FromChain + "->" + prevRoute.ToChain
				previousRouteCounts[routeKey] = prevRoute.Count
			}
			
			// Calculate percentage changes for current routes
			for routeKey, currentRoute := range routeCounts {
				if previousCount, exists := previousRouteCounts[routeKey]; exists {
					currentRoute.CountChange = ec.calculatePercentageChange(
						float64(currentRoute.Count),
						float64(previousCount),
					)
				} else {
					// Route didn't exist in previous period, so it's 100% increase
					currentRoute.CountChange = 100
				}
			}
		}
	}
}

// getTopSenders returns the top N senders by transfer count
func (ec *EnhancedCollector) getTopSenders(n int) []*WalletStats {
	senders := make([]*WalletStats, 0, len(ec.senderStats))
	for _, sender := range ec.senderStats {
		senders = append(senders, sender)
	}
	
	sort.Slice(senders, func(i, j int) bool {
		return senders[i].Count > senders[j].Count
	})
	
	if len(senders) > n {
		senders = senders[:n]
	}
	
	return senders
}

// getTopReceivers returns the top N receivers by transfer count
func (ec *EnhancedCollector) getTopReceivers(n int) []*WalletStats {
	receivers := make([]*WalletStats, 0, len(ec.receiverStats))
	for _, receiver := range ec.receiverStats {
		receivers = append(receivers, receiver)
	}
	
	sort.Slice(receivers, func(i, j int) bool {
		return receivers[i].Count > receivers[j].Count
	})
	
	if len(receivers) > n {
		receivers = receivers[:n]
	}
	
	return receivers
}

// getPopularRoutesTimeScale returns popular routes for different time scales
func (ec *EnhancedCollector) getPopularRoutesTimeScale() map[string][]*RouteStats {
	now := time.Now()
	
	return map[string][]*RouteStats{
		"1m":  ec.getTopRoutesInPeriod(now.Add(-time.Minute), 7, "1m"),
		"1h":  ec.getTopRoutesInPeriod(now.Add(-time.Hour), 7, "1h"),
		"1d":  ec.getTopRoutesInPeriod(now.Add(-24*time.Hour), 7, "1d"),
		"7d":  ec.getTopRoutesInPeriod(now.AddDate(0, 0, -7), 7, "7d"),
		"14d": ec.getTopRoutesInPeriod(now.AddDate(0, 0, -14), 7, "14d"),
		"30d": ec.getTopRoutesInPeriod(now.AddDate(0, 0, -30), 7, "30d"),
	}
}

// getActiveSendersTimeScale returns active senders for different time scales
func (ec *EnhancedCollector) getActiveSendersTimeScale() map[string][]*WalletStats {
	now := time.Now()
	
	return map[string][]*WalletStats{
		"1m":  ec.getTopSendersInPeriod(now.Add(-time.Minute), 7),
		"1h":  ec.getTopSendersInPeriod(now.Add(-time.Hour), 7),
		"1d":  ec.getTopSendersInPeriod(now.Add(-24*time.Hour), 7),
		"7d":  ec.getTopSendersInPeriod(now.AddDate(0, 0, -7), 7),
		"14d": ec.getTopSendersInPeriod(now.AddDate(0, 0, -14), 7),
		"30d": ec.getTopSendersInPeriod(now.AddDate(0, 0, -30), 7),
	}
}

// getActiveReceiversTimeScale returns active receivers for different time scales
func (ec *EnhancedCollector) getActiveReceiversTimeScale() map[string][]*WalletStats {
	now := time.Now()
	
	return map[string][]*WalletStats{
		"1m":  ec.getTopReceiversInPeriod(now.Add(-time.Minute), 7),
		"1h":  ec.getTopReceiversInPeriod(now.Add(-time.Hour), 7),
		"1d":  ec.getTopReceiversInPeriod(now.Add(-24*time.Hour), 7),
		"7d":  ec.getTopReceiversInPeriod(now.AddDate(0, 0, -7), 7),
		"14d": ec.getTopReceiversInPeriod(now.AddDate(0, 0, -14), 7),
		"30d": ec.getTopReceiversInPeriod(now.AddDate(0, 0, -30), 7),
	}
}

// getTopSendersInPeriod returns the top N senders active in a specific time period
func (ec *EnhancedCollector) getTopSendersInPeriod(since time.Time, n int) []*WalletStats {
	senderCounts := make(map[string]int64)
	senderLastActivity := make(map[string]time.Time)
	
	// Count sender activity in the specified time period
	for _, activity := range ec.walletTimeData {
		if activity.Timestamp.After(since) && activity.Sender != "" {
			senderCounts[activity.Sender]++
			if activity.Timestamp.After(senderLastActivity[activity.Sender]) {
				senderLastActivity[activity.Sender] = activity.Timestamp
			}
		}
	}
	
	// Convert to WalletStats slice
	senders := make([]*WalletStats, 0, len(senderCounts))
	for address, count := range senderCounts {
		senders = append(senders, &WalletStats{
			Count:        count,
			Address:      address,
			LastActivity: senderLastActivity[address],
		})
	}
	
	// Sort by count
	sort.Slice(senders, func(i, j int) bool {
		return senders[i].Count > senders[j].Count
	})
	
	// Limit to top N
	if len(senders) > n {
		senders = senders[:n]
	}
	
	return senders
}

// getTopReceiversInPeriod returns the top N receivers active in a specific time period
func (ec *EnhancedCollector) getTopReceiversInPeriod(since time.Time, n int) []*WalletStats {
	receiverCounts := make(map[string]int64)
	receiverLastActivity := make(map[string]time.Time)
	
	// Count receiver activity in the specified time period
	for _, activity := range ec.walletTimeData {
		if activity.Timestamp.After(since) && activity.Receiver != "" {
			receiverCounts[activity.Receiver]++
			if activity.Timestamp.After(receiverLastActivity[activity.Receiver]) {
				receiverLastActivity[activity.Receiver] = activity.Timestamp
			}
		}
	}
	
	// Convert to WalletStats slice
	receivers := make([]*WalletStats, 0, len(receiverCounts))
	for address, count := range receiverCounts {
		receivers = append(receivers, &WalletStats{
			Count:        count,
			Address:      address,
			LastActivity: receiverLastActivity[address],
		})
	}
	
	// Sort by count
	sort.Slice(receivers, func(i, j int) bool {
		return receivers[i].Count > receivers[j].Count
	})
	
	// Limit to top N
	if len(receivers) > n {
		receivers = receivers[:n]
	}
	
	return receivers
}







 