/*
Copyright 2019 gomemcache Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

type errorsSlice []error

var errUnknownStat = errors.New("unknown stat")

func (errs errorsSlice) Error() string {
	switch len(errs) {
	case 0:
		return ""
	case 1:
		return errs[0].Error()
	}
	n := len(errs) - 1
	for i := 0; i < len(errs); i++ {
		n += len(errs[i].Error())
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(errs[0].Error())
	for _, err := range errs[1:] {
		b.WriteByte(':')
		b.WriteString(err.Error())
	}
	return b.String()
}

func (errs *errorsSlice) AppendError(err error) {
	if err == nil {
		return
	}
	if errs == nil {
		errs = &errorsSlice{err}
		return
	}
	set := *errs
	set = append(set, err)
	*errs = set
}

type Queue struct {
	Name             string
	Items            uint64
	Waiters          uint64
	OpenTransactions uint64
}

// ServerStats contains the general statistics from one server.
type ServerStats struct {
	// ServerErr error if can't get the stats information.
	ServerErr error
	// Errs contains the errors that occurred while parsing the response.
	Errs errorsSlice
	// UnknownStats contains the stats that are not in the struct. This can be useful if the memcached developers add new stats in newer versions.
	UnknownStats map[string]string

	// Version version string of this server.
	Version string
	// AcceptingConns whether or not server is accepting conns.
	Uptime uint32
	// Time current UNIX time according to the server.
	Time uint32
	// CurrConnections number of open connections.
	CurrConnections uint32
	// TotalConnections total number of connections opened since the server started running.
	TotalConnections uint32
	// CurrItems current number of items stored.
	CurrItems uint64
	// TotalItems total number of items stored since the server started.
	TotalItems uint64
	// CmdGet cumulative number of retrieval reqs.
	CmdGet uint64
	// CmdSet cumulative number of storage reqs.
	CmdSet uint64
	// Queue Info
	Queues map[string]*Queue
}

// StatsServers returns the general statistics of the servers
// retrieved with the `stats` command.
func (c *Client) StatsServers() (servers map[net.Addr]*ServerStats, err error) {
	servers = make(map[net.Addr]*ServerStats)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = new(ServerStats)
			servers[addr].Queues = make(map[string]*Queue)
			servers[addr].UnknownStats = make(map[string]string)
			return nil
		},
	)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(addrs))
	for _, addr := range addrs {
		go func(addr net.Addr) {
			server := servers[addr]
			c.statsFromAddr(server, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	return
}

func (c *Client) statsFromAddr(server *ServerStats, addr net.Addr) {
	err := c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		for {
			line, err := rw.ReadBytes('\n')
			if err != nil {
				return err
			}
			if bytes.Equal(line, resultEnd) {
				return nil
			}
			// STAT <name> <value>\r\n
			tkns := bytes.Split(line[5:len(line)-2], space)
			err = parseSetStatValue(reflect.ValueOf(server), string(tkns[0]), string(tkns[1]))
			if err != nil {
				if err != errUnknownStat {
					server.Errs.AppendError(err)
				} else {
					// could be unknown or could be a queue item.
					if strings.HasPrefix(string(tkns[0]), "queue_") {
						//parse this line and add/edit queue item.
						if strings.HasSuffix(string(tkns[0]), "_open_transactions") {
							q := c.parseStatQueue(string(tkns[0]), "_open_transactions")
							StatsQueue := c.findOrCreateQueue(server, q)
							uintv, err := strconv.ParseUint(string(tkns[1]), 10, 64)
							if err != nil {
								return errors.New("unable to parse uint value " + string(tkns[1]) + ":" + err.Error())
							}
							StatsQueue.OpenTransactions = uintv
						} else if strings.HasSuffix(string(tkns[0]), "_items") {
							q := c.parseStatQueue(string(tkns[0]), "_items")
							StatsQueue := c.findOrCreateQueue(server, q)
							uintv, err := strconv.ParseUint(string(tkns[1]), 10, 64)
							if err != nil {
								return errors.New("unable to parse uint value " + string(tkns[1]) + ":" + err.Error())
							}
							StatsQueue.Items = uintv
						} else if strings.HasSuffix(string(tkns[0]), "_waiters") {
							q := c.parseStatQueue(string(tkns[0]), "_waiters")
							StatsQueue := c.findOrCreateQueue(server, q)
							uintv, err := strconv.ParseUint(string(tkns[1]), 10, 64)
							if err != nil {
								return errors.New("unable to parse uint value " + string(tkns[1]) + ":" + err.Error())
							}
							StatsQueue.Waiters = uintv
						}
					} else {
						server.UnknownStats[string(tkns[0])] = string(tkns[1])
					}
				}
			}
		}
	})
	server.ServerErr = err
	return
}

func (c *Client) parseStatQueue(stat, suffix string) string {
	return strings.Replace(strings.Replace(stat, "queue_", "", 1), suffix, "", 1)
}

func (c *Client) findOrCreateQueue(server *ServerStats, queue string) *Queue {
	if val, ok := server.Queues[queue]; ok {
		return val
	}
	server.Queues[queue] = &Queue{
		Name:             queue,
	}
	return server.Queues[queue]
}

// parseSetStatValue parses and sets the value of the stat to the corresponding struct field.
//
// In order to know the corresponding field of the stat, the snake_case stat name is converted to CamelCase
func parseSetStatValue(strPntr reflect.Value, stat, value string) error {
	statCamel := toCamel(stat)
	f := reflect.Indirect(strPntr).FieldByName(statCamel)
	switch f.Kind() {
	case reflect.Uint32:
		uintv, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return errors.New("unable to parse uint value " + stat + ":" + err.Error())
		}
		f.SetUint(uintv)
	case reflect.Uint64:
		uintv, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.New("unable to parse uint value " + stat + ":" + err.Error())
		}
		f.SetUint(uintv)
	case reflect.Float64:
		floatv, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.New("unable to parse float value for " + stat + ":" + err.Error())
		}
		f.SetFloat(floatv)
	case reflect.String:
		f.SetString(value)
	case reflect.Bool:
		f.SetBool(value == "1")
	default:
		return errUnknownStat
	}
	return nil
}

func toCamel(s string) string {
	if s == "" {
		return ""
	}
	// Compute number of replacements.
	m := strings.Count(s, "_")
	if m == 0 {
		return string(unicode.ToUpper(rune(s[0]))) + s[1:] // avoid allocation
	}

	// Apply replacements to buffer.
	l := len(s) - m
	if l == 0 {
		return ""
	}
	t := make([]byte, l)

	w := 0
	start := 0
	for i := 0; i < m; i++ {
		j := start
		j += strings.Index(s[start:], "_")
		if start != j {
			t[w] = byte(unicode.ToUpper(rune(s[start])))
			w++
			w += copy(t[w:], s[start+1:j])
		}
		start = j + 1
	}
	if s[start:] != "" {
		t[w] = byte(unicode.ToUpper(rune(s[start])))
		w++
		w += copy(t[w:], s[start+1:])
	}
	return string(t[0:w])
}
