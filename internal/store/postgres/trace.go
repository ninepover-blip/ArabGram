package postgres

import (
	"fmt"
	"path/filepath"
	"runtime"
)

func traceCaller(skip int) string {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	name := ""
	if fn := runtime.FuncForPC(pc); fn != nil {
		name = fn.Name()
	}
	if name == "" {
		return fmt.Sprintf("%s:%d", filepath.ToSlash(file), line)
	}
	return fmt.Sprintf("%s %s:%d", name, filepath.ToSlash(file), line)
}
