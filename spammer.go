package main

import (
	"fmt"
	"sort"
	"sync"
)

func runCmd(command cmd, in, out chan interface{}, wg *sync.WaitGroup) {
	command(in, out)
	close(out)
	wg.Done()
}

func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	wg.Add(len(cmds))
	in := make(chan interface{})
	close(in)
	for _, command := range cmds {
		out := make(chan interface{})
		go runCmd(command, in, out, wg)
		in = out
	}
	wg.Wait()
}

func toString(i interface{}) string {
	res, ok := i.(string)
	if !ok {
		panic("cannot convert to string")
	}
	return res
}

func toUint64(i interface{}) uint64 {
	res, ok := i.(uint64)
	if !ok {
		panic("cannot convert to uint64")
	}
	return res
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	var userMap sync.Map
	for i := range in {
		wg.Add(1)
		go func(email string) {
			user := GetUser(email)
			userMap.Store(user.ID, user.Email)
			wg.Done()
		}(toString(i))
	}
	wg.Wait()

	userMap.Range(func(k, v interface{}) bool {
		out <- User{ID: toUint64(k), Email: toString(v)}
		return true
	})
}

func toUser(inUser interface{}) User {
	outUser, ok := inUser.(User)
	if !ok {
		panic("entity is not a User type")
	}
	return outUser
}

func getMsgBatch(userBatch []User, wg *sync.WaitGroup, cnt int, out chan interface{}) {
	userBatchCpy := make([]User, GetMessagesMaxUsersBatch)
	copy(userBatchCpy, userBatch)
	wg.Add(1)
	go func() {
		msgBatch, err := GetMessages(userBatchCpy[:cnt]...)
		if err != nil {
			panic("too many users")
		}
		for _, v := range msgBatch {
			out <- v
		}
		wg.Done()
	}()
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	userBatch := make([]User, GetMessagesMaxUsersBatch)
	cnt := 0
	for user := range in {
		userBatch[cnt] = toUser(user)
		cnt++
		if cnt == GetMessagesMaxUsersBatch {
			getMsgBatch(userBatch, wg, GetMessagesMaxUsersBatch, out)
			cnt = 0
		}
	}
	if cnt != 0 {
		getMsgBatch(userBatch, wg, cnt, out)
	}
	wg.Wait()
}

func toMsgID(msgID interface{}) MsgID {
	outMsg, ok := msgID.(MsgID)
	if !ok {
		panic("cannot convert to MsgID")
	}
	return outMsg
}

func CheckSpam(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	guard := make(chan struct{}, HasSpamMaxAsyncRequests)
	for msgID := range in {
		guard <- struct{}{}
		wg.Add(1)
		go func(msgID MsgID) {
			flag, err := HasSpam(msgID)
			<-guard
			if err != nil {
				panic("cannot convert to MsgID")
			}
			out <- MsgData{msgID, flag}
			wg.Done()
		}(toMsgID(msgID))
	}
	wg.Wait()
}

func toMsgData(msgData interface{}) MsgData {
	outMsgData, ok := msgData.(MsgData)
	if !ok {
		panic("cannot convert to MsgData")
	}
	return outMsgData
}

func CombineResults(in, out chan interface{}) {
	var msgs []MsgData
	for msgData := range in {
		msgs = append(msgs, toMsgData(msgData))
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].HasSpam == msgs[j].HasSpam {
			return msgs[i].ID < msgs[j].ID
		}
		return msgs[i].HasSpam
	})
	for _, msg := range msgs {
		out <- fmt.Sprintf("%t %v", msg.HasSpam, uint64(msg.ID))
	}
}
