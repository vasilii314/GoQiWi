package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		go func() {
			wg.Add(1)
			defer wg.Done()
			for j := 0; j < 10; j++ {
				client := &http.Client{}
				req, err := http.NewRequest(http.MethodPut, "http://localhost:8081/v1/"+fmt.Sprintf("client%d", i), bytes.NewBuffer([]byte(fmt.Sprintf("data%d", j))))
				if err != nil {
					panic(err)
				}

				// set the request header Content-Type for json
				req.Header.Set("Content-Type", "text/plain")
				resp, err := client.Do(req)
				if err != nil {
					panic(err)
				}

				log.Println(resp.StatusCode, fmt.Sprintf("client%d", i), fmt.Sprintf("data%d", j))
				time.Sleep(2 * time.Second)
			}
		}()
		time.Sleep(2 * time.Second)
	}
	wg.Wait()
}
