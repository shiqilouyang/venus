package paralle

import (
	"sync"
	"time"
)

type Par struct {
	parCtl chan interface{}
	index  int64
	group  sync.WaitGroup
	errs   *MultiError
	locker sync.Mutex
}

func NewPar(maxParCount int) *Par {
	return &Par{
		parCtl: make(chan interface{}, maxParCount),
		group:  sync.WaitGroup{},
		errs:   new(MultiError),
	}
}

func (p *Par) Go(f func() error) {
	p.group.Add(1)

	p.parCtl <- p.index
	p.index++

	go func() {
		defer func() {
			<-p.parCtl
			p.group.Done()
		}()

		if err := f(); err != nil {
			p.locker.Lock()
			defer p.locker.Unlock()
			p.errs.AddError(err)
		}
	}()
}

func (p *Par) GoV2(f func(args ...interface{}) error, args ...interface{}) {
	p.group.Add(1)

	p.parCtl <- p.index
	p.index++

	go func() {
		defer func() {
			<-p.parCtl
			p.group.Done()
		}()
		if err := f(args...); err != nil {
			p.locker.Lock()
			defer p.locker.Unlock()
			p.errs.AddError(err)
		}
	}()
}

func (p *Par) Wait() (time.Duration, *MultiError) {
	begin := time.Now()
	p.group.Wait()
	return time.Since(begin), p.errs
}
