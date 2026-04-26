package serve

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/config"
)

type managedFileView struct {
	Name       string `json:"name"`
	Relative   string `json:"relative"`
	FullPath   string `json:"full_path"`
	ModifiedAt string `json:"modified_at"`
	Size       int64  `json:"size"`
}

func listManagedYAMLFiles(root string) ([]managedFileView, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}

	files := make([]managedFileView, 0, 8)
	err = filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !isYAMLPath(path) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return err
		}
		normalizedRel := filepath.ToSlash(rel)
		files = append(files, managedFileView{
			Name:       filepath.Base(path),
			Relative:   normalizedRel,
			FullPath:   path,
			ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
			Size:       info.Size(),
		})
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return []managedFileView{}, nil
		}
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Relative < files[j].Relative
	})
	return files, nil
}

func listManagedBatchFiles(root string) ([]managedFileView, error) {
	files, err := listManagedYAMLFiles(root)
	if err != nil {
		return nil, err
	}
	filtered := make([]managedFileView, 0, len(files))
	for _, file := range files {
		if _, err := config.LoadBatchFile(file.FullPath); err == nil {
			filtered = append(filtered, file)
		}
	}
	return filtered, nil
}

func listManagedScheduleFiles(root string) ([]managedFileView, error) {
	files, err := listManagedYAMLFiles(root)
	if err != nil {
		return nil, err
	}
	filtered := make([]managedFileView, 0, len(files))
	for _, file := range files {
		if _, err := config.LoadScheduleFile(file.FullPath); err == nil {
			filtered = append(filtered, file)
		}
	}
	return filtered, nil
}

func resolveManagedYAMLFile(root string, rawName string) (string, string, error) {
	trimmed := strings.TrimSpace(rawName)
	if trimmed == "" {
		return "", "", fmt.Errorf("file name must not be empty")
	}
	decoded, err := url.PathUnescape(trimmed)
	if err != nil {
		return "", "", fmt.Errorf("decode file name: %w", err)
	}
	if strings.Contains(decoded, "\x00") {
		return "", "", fmt.Errorf("file name contains invalid characters")
	}
	cleaned := filepath.Clean(filepath.FromSlash(decoded))
	if filepath.IsAbs(cleaned) {
		return "", "", fmt.Errorf("absolute paths are not allowed")
	}
	if cleaned == "." || strings.HasPrefix(cleaned, "..") {
		return "", "", fmt.Errorf("path traversal is not allowed")
	}
	if !isYAMLPath(cleaned) {
		return "", "", fmt.Errorf("only .yaml and .yml files are allowed")
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", "", fmt.Errorf("resolve root: %w", err)
	}
	resolved := filepath.Join(absRoot, cleaned)
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return "", "", fmt.Errorf("resolve file path: %w", err)
	}
	rel, err := filepath.Rel(absRoot, absResolved)
	if err != nil {
		return "", "", fmt.Errorf("verify file path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("path traversal is not allowed")
	}
	if _, err := os.Stat(absResolved); err != nil {
		if os.IsNotExist(err) {
			return "", "", fmt.Errorf("file not found")
		}
		return "", "", err
	}
	return absResolved, filepath.ToSlash(rel), nil
}

func isYAMLPath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".yaml" || ext == ".yml"
}
