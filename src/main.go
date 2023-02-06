package main

import (
	"GoQiWi/src/core/logger"
	"GoQiWi/src/core/store"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var transact logger.TransactionLogger

func initializeTransactionLog() error {
	var err error
	transact, err = logger.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	e, ok := logger.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case logger.EventDelete:
				err = store.Delete(e.Key)
			case logger.EventPut:
				err = store.Put(e.Key, e.Value)
			}
		}
	}
	transact.Run()
	return err
}

func KeyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = store.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func KeyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := store.Get(key)

	if errors.Is(err, store.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value))

}

func KeyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := store.Delete(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	transact.WriteDelete(key)
	w.WriteHeader(http.StatusCreated)
}

func WatchForShutdown(signals chan os.Signal, wg *sync.WaitGroup) {
	<-signals
	defer (*wg).Done()
	log.Println("Shutting down...")
	transact.Stop()
}

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", KeyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", KeyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", KeyValueDeleteHandler).Methods("DELETE")
	go func() {
		log.Fatal(http.ListenAndServe(":8081", r))
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go WatchForShutdown(sigChan, &wg)
	wg.Wait()
	os.Exit(0)
}
