package main

import (
	"fmt"
	"sort"
	"strconv"
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

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	var userMap sync.Map
	for i := range in {
		wg.Add(1)
		go func(email string) {
			user := GetUser(email)
			userMap.Store(user.ID, user.Email)
			wg.Done()
		}(i.(string))
	}
	wg.Wait()

	var outUsers []User
	userMap.Range(func(k, v interface{}) bool {
		outUsers = append(outUsers, User{ID: k.(uint64), Email: v.(string)})
		return true
	})

	for _, v := range outUsers {
		out <- v
	}
}

func toUser(inUser interface{}) User {
	outUser, ok := inUser.(User)
	if !ok {
		fmt.Println("entity is not a User type")
	}
	return outUser
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	userBatch := make([]User, GetMessagesMaxUsersBatch)
	var msgs []MsgID
	cnt := 0
	for user := range in {
		userBatch[cnt] = toUser(user)
		if cnt == 1 {
			userBatchCpy := userBatch
			wg.Add(1)
			go func() {
				msgBatch, err := GetMessages(userBatchCpy[0], userBatchCpy[1])
				if err != nil {
					fmt.Println("too many users")
				}
				for _, v := range msgBatch {
					msgs = append(msgs, v)
				}
				wg.Done()
			}()
			cnt = 0
		} else {
			cnt++
		}
	}
	if cnt == 1 {
		userBatchCpy := userBatch
		wg.Add(1)
		go func() {
			msgBatch, err := GetMessages(userBatchCpy[0])
			if err != nil {
				fmt.Println("too many users")
			}
			for _, v := range msgBatch {
				msgs = append(msgs, v)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	for _, v := range msgs {
		out <- v
	}
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
				fmt.Println("cannot convert to MsgID")
			}
			out <- MsgData{msgID, flag}
			wg.Done()
		}(msgID.(MsgID))
	}
	wg.Wait()
}

func IDToInt(id MsgID) uint64 {
	return uint64(id)
}

func CombineResults(in, out chan interface{}) {
	var msgs []MsgData
	for msgData := range in {
		msgs = append(msgs, msgData.(MsgData))
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].HasSpam == msgs[j].HasSpam {
			return msgs[i].ID < msgs[j].ID
		} else {
			return msgs[i].HasSpam
		}
	})
	for _, msg := range msgs {
		str := strconv.FormatBool(msg.HasSpam) + " " + strconv.FormatUint(IDToInt(msg.ID), 10)
		out <- str
	}
}
