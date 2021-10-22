package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"mosn.io/layotto/components/lock"
	"mosn.io/layotto/components/pkg/utils"
	"mosn.io/pkg/log"
	"strings"
	"sync"
	"time"
)

//RedLock
//at least 5 hosts
type ClusterRedisLock struct {
	clients   []*redis.Client
	metadata utils.RedisClusterMetadata
	replicas int

	features []lock.Feature
	logger   log.ErrorLogger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewClusterRedisLock returns a new redis lock store
func NewClusterRedisLock(logger log.ErrorLogger) *ClusterRedisLock {
	s := &ClusterRedisLock{
		features: make([]lock.Feature, 0),
		logger:   logger,
	}

	return s
}

type resultMsg struct {
	error error
	host string
	status lock.LockStatus
}

func (c *ClusterRedisLock)Init(metadata lock.Metadata) error {
	fmt.Println("1111111111111111111111111")
	m, err := utils.ParseRedisClusterMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	c.metadata = m
	c.clients = utils.NewClusterRedisClient(m)
	c.ctx, c.cancel = context.WithCancel(context.Background())

	for i,client := range c.clients{
		if _, err = client.Ping(c.ctx).Result(); err != nil {
			return fmt.Errorf("[ClusterRedisLock]: error connecting to redis at %s: %s", c.metadata.Hosts[i], err)
		}
	}
	return err
}

func (c *ClusterRedisLock)Features() []lock.Feature {
	return c.features
}

func (c *ClusterRedisLock)TryLock(req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	intervalStart := time.Now().UnixNano()
	//resourceId,lockOwner,expire := req.ResourceId,req.LockOwner,req.Expire
	//the time interval of lock far less than expire time
	intervalLimit := int64(req.Expire)*1000/10
	wg := sync.WaitGroup{}
	wg.Add(len(c.clients))
	resultChan := make(chan resultMsg,len(c.clients))
	for i := range c.clients {
		go c.LockSingleRedis(i,req,&wg,resultChan)
	}
	wg.Wait()
	intervalEnd := time.Now().UnixNano()
	if intervalLimit < intervalEnd-intervalStart {
		_,_ = c.UnlockAllRedis(&lock.UnlockRequest{
			ResourceId: req.ResourceId,
			LockOwner:  req.LockOwner,
		}, &wg)
		return &lock.TryLockResponse{
			Success: false,
		},fmt.Errorf("[ClusterRedisLock]: lock timeout. ResourceId: %s",req.ResourceId)
	}
	close(resultChan)
	successCount := 0
	errorStrs := make([]string,0,len(c.clients))
	for msg := range resultChan {
		if msg.error != nil {
			errorStrs = append(errorStrs,msg.error.Error())
			continue
		}
		successCount++
	}
	if successCount*2 > len(c.clients){
		return &lock.TryLockResponse{
			Success: true,
		},nil
	}else{
		_,_ = c.UnlockAllRedis(&lock.UnlockRequest{
			ResourceId: req.ResourceId,
			LockOwner:  req.LockOwner,
		}, &wg)
		return &lock.TryLockResponse{
			Success: false,
		},fmt.Errorf(strings.Join(errorStrs,"\n"))
	}
}

func (c *ClusterRedisLock)Unlock(req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	wg := sync.WaitGroup{}
	status,err := c.UnlockAllRedis(req,&wg)
	if err != nil {
		return newInternalErrorUnlockResponse(),err
	}
	return &lock.UnlockResponse{
		Status: status,
	},nil
}

func (c *ClusterRedisLock)UnlockAllRedis(req *lock.UnlockRequest,wg *sync.WaitGroup) (lock.LockStatus,error) {
	wg.Add(len(c.clients))
	ch := make(chan resultMsg,len(c.clients))
	for i := range c.clients{
		go c.UnlockSingleRedis(i,req,wg,ch)
	}
	wg.Wait()
	close(ch)
	errorStrs := make([]string,0,len(c.clients))
	status := lock.SUCCESS
	for msg := range ch {
		if msg.status == lock.INTERNAL_ERROR{
			status = msg.status
			errorStrs = append(errorStrs,msg.error.Error())
		}
	}
	if len(errorStrs) > 0 {
		return status,fmt.Errorf(strings.Join(errorStrs,"\n"))
	}
	return status,nil
}

func (c *ClusterRedisLock)LockSingleRedis(clientIndex int,req *lock.TryLockRequest,wg *sync.WaitGroup,ch chan resultMsg){
	defer wg.Done()
	msg := resultMsg{
		host:  c.metadata.Hosts[clientIndex],
	}
	nx := c.clients[clientIndex].SetNX(c.ctx, req.ResourceId, req.LockOwner, time.Second*time.Duration(req.Expire))
	if nx == nil {
		msg.error = fmt.Errorf("[ClusterRedisLock]: SetNX returned nil. host: %s \n ResourceId: %s", c.clients[clientIndex],req.ResourceId)
		ch <- msg
		return
	}
	if nx.Err() != nil {
		msg.error = fmt.Errorf("[ClusterRedisLock]: %s host: %s \n ResourceId: %s",nx.Err().Error(),c.clients[clientIndex],req.ResourceId)
	}
	ch <- msg
}

func (c *ClusterRedisLock)UnlockSingleRedis(clientIndex int,req *lock.UnlockRequest,wg *sync.WaitGroup,ch chan resultMsg){
	defer wg.Done()
	eval := c.clients[clientIndex].Eval(c.ctx, unlockScript, []string{req.ResourceId}, req.LockOwner)
	msg := resultMsg{}
	msg.status = lock.INTERNAL_ERROR
	if eval == nil {
		msg.error = fmt.Errorf("[ClusterRedisLock]: Eval unlock script returned nil. host: %s \n ResourceId: %s", c.clients[clientIndex],req.ResourceId)
		ch <- msg
		return
	}
	if eval.Err() != nil {
		msg.error = fmt.Errorf("[ClusterRedisLock]: %s host: %s \n ResourceId: %s",eval.Err().Error(),c.clients[clientIndex],req.ResourceId)
		ch <- msg
		return
	}
	i, err := eval.Int()
	if err != nil {
		msg.error = err
		ch <- msg
		return
	}
	if i >= 0 {
		msg.status = lock.SUCCESS
	} else if i == -1 {
		msg.status = lock.LOCK_UNEXIST
	} else if i == -2 {
		msg.status = lock.LOCK_BELONG_TO_OTHERS
	}
	ch <- msg
}