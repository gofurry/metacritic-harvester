package serve

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui/*
var uiFS embed.FS

func uiFileServer() http.Handler {
	sub, err := fs.Sub(uiFS, "ui")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
