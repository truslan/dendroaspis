package main

import (
	"github.com/mrb/riakpbc"
	"github.com/truslan/dendroaspis"
	"log"
	"net/http"
)

var storage dendroaspis.Storage

type functionHandler struct {
	f func(w http.ResponseWriter, r *http.Request)
}

func (h *functionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.f(w, r)
}

func displayHandlerFunc(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	if key == "" {
		log.Println("Empty key")
		w.WriteHeader(http.StatusNotAcceptable)
	} else {
		img, err := storage.GetById(key)
		if err != nil {
			if err == riakpbc.ErrObjectNotFound {
				log.Printf("Image not found: %s", key)
				w.WriteHeader(http.StatusNotFound)
			} else {
				log.Println(err.Error())
				w.WriteHeader(http.StatusInternalServerError)
			}
		} else {
			if len(img.Bytes) > 0 {
				w.Write(img.Bytes)
			} else {
				log.Printf("Empty body for image: %s\n", key)
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}
}

func main() {
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)
	storage = dendroaspis.NewStorage([]string{"127.0.0.1:8087"}, "images")
	defer storage.Close()
	storage.Dial()

	http.Handle("/image/", http.StripPrefix("/image/", &functionHandler{f: displayHandlerFunc}))

	http.ListenAndServe(":8080", nil)
}
