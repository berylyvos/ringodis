package timewheel

import (
	"container/list"
	"ringodis/lib/logger"
	"time"
)

type location struct {
	slot  int
	eTask *list.Element
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {
	interval          time.Duration
	ticker            *time.Ticker
	slots             []*list.List
	timer             map[string]*location
	currentPos        int
	slotNum           int
	addTaskChannel    chan *task
	removeTaskChannel chan string
	stopChannel       chan bool
}

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan *task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()
	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case t := <-tw.addTaskChannel:
			tw.addTask(t)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- &task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		if t.circle > 0 {
			t.circle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			f := t.job
			f()
		}()
		next := e.Next()
		l.Remove(e)
		if t.key != "" {
			delete(tw.timer, t.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(t *task) {
	pos, circle := tw.getPosAndCircle(t.delay)
	t.circle = circle

	e := tw.slots[pos].PushBack(t)
	loc := &location{
		slot:  pos,
		eTask: e,
	}
	if t.key != "" {
		if _, ok := tw.timer[t.key]; ok {
			tw.removeTask(t.key)
		}
	}
	tw.timer[t.key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	loc, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[loc.slot]
	l.Remove(loc.eTask)
	delete(tw.timer, key)
}

func (tw *TimeWheel) getPosAndCircle(d time.Duration) (pos, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum
	return
}
