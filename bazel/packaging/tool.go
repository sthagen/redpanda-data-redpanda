// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing/fstest"
	"time"

	"github.com/goreleaser/nfpm/v2"
	"github.com/goreleaser/nfpm/v2/deb"
	"github.com/goreleaser/nfpm/v2/files"
)

type fileConfig struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	SourcePath string `json:"source"`
}

// directory, deb, or tarball
type packageType string

type debConfig struct {
	Depends        []string `json:"depends"`
	Description    string   `json:"description"`
	SystemdService string   `json:"systemd_service,omitempty"`
	SystemdSlice   string   `json:"systemd_slice,omitempty"`
	Scripts        struct {
		PreInst  string `json:"preinst,omitempty"`
		PostInst string `json:"postinst,omitempty"`
		PreRm    string `json:"prerm,omitempty"`
		PostRm   string `json:"postrm,omitempty"`
	} `json:"scripts,omitempty"`
}

type pkgConfig struct {
	Name               string            `json:"name"`
	PackageDirectories []string          `json:"package_dirs"`
	PackageFiles       []fileConfig      `json:"package_files"`
	PackageSymlinks    map[string]string `json:"package_symlinks"`
	PackageType        packageType       `json:"package_type"`
	Owner              int               `json:"owner"`
	Deb                debConfig         `json:"deb"`
}

const rootDir = "___root___"

// buildPackageStructure creates a MapFS structure based on the provided package configuration.
// It sets up directories and package files specified in the configuration.
// The root directory is defined as "___root___" and all paths are relative to this root.
// NOTE: the root directory is required to be present in the MapFS structure for the walkDir function to work.

func buildPackageStructure(cfg pkgConfig) (fstest.MapFS, error) {
	mapFs := fstest.MapFS{}

	// Add directories
	for _, dir := range cfg.PackageDirectories {
		path := path.Join(rootDir, dir)
		mapFs[path] = &fstest.MapFile{
			Mode: fs.ModeDir,
		}
	}

	// Add package files
	for _, file := range cfg.PackageFiles {
		path := path.Join(rootDir, file.Path, file.Name)
		mapFs[path] = &fstest.MapFile{}
	}

	return mapFs, nil
}

func createPackageDirectories(mapFs fstest.MapFS, dirFunction func(path string) error) error {
	return fs.WalkDir(mapFs, rootDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == rootDir {
			return nil // Skip the root directory itself
		}
		// Get the relative path from the root directory, we do not want the root directory in the path
		relativePath, err := filepath.Rel(rootDir, path)

		if err != nil {
			return err
		}
		if info.IsDir() {
			return dirFunction(relativePath)
		}
		return nil
	})
}

func createPackage(cfg pkgConfig, createDir func(path string) error, createFile func(fileConfig fileConfig) error) error {
	// Create package directory structure
	mapFs, err := buildPackageStructure(cfg)
	if err != nil {
		return fmt.Errorf("unable to build package structure: %w", err)
	}

	err = createPackageDirectories(mapFs, createDir)
	if err != nil {
		return fmt.Errorf("error creating package directories: %w", err)
	}

	for _, f := range cfg.PackageFiles {
		if err := createFile(f); err != nil {
			return fmt.Errorf("error creating package file %s: %w", f.SourcePath, err)
		}
	}

	return nil
}

func createTarball(cfg pkgConfig, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() {
		if err := tw.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing tar writer: %v\n", err)
		}
	}()
	writeFile := func(fileConfig fileConfig) error {
		file, err := os.Open(fileConfig.SourcePath)
		if err != nil {
			return err
		}
		defer func() {
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing file %s: %v\n", fileConfig.SourcePath, err)
			}
		}()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		err = tw.WriteHeader(&tar.Header{
			Name:     path.Join(fileConfig.Path, fileConfig.Name),
			Mode:     int64(info.Mode()),
			Typeflag: tar.TypeReg,
			ModTime:  time.Unix(0, 0),
			Uid:      cfg.Owner,
			Gid:      cfg.Owner,
			Size:     info.Size(),
		})
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		return err
	}
	writeDir := func(path string) error {
		return tw.WriteHeader(&tar.Header{
			Name:     path,
			Mode:     0755,
			Typeflag: tar.TypeDir,
			ModTime:  time.Unix(0, 0),
			Uid:      cfg.Owner,
			Gid:      cfg.Owner,
		})
	}
	return createPackage(cfg, writeDir, writeFile)
}

func copyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := srcFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing source file %s: %v\n", src, err)
		}
	}()
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if err := dstFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing destination file %s: %v\n", dst, err)
		}
	}()
	_, err = dstFile.ReadFrom(srcFile)
	if err != nil {
		return err
	}

	return nil
}

func createPackageDir(cfg pkgConfig, output string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}

	dir := func(p string) error {
		if err := os.Mkdir(path.Join(output, p), 0755); err != nil {
			return fmt.Errorf("error creating directory %s: %v", p, err)
		}
		return nil
	}

	file := func(fileConfig fileConfig) error {
		if err := copyFile(fileConfig.SourcePath, path.Join(output, fileConfig.Path, fileConfig.Name)); err != nil {
			return fmt.Errorf("error copying file %s: %v", fileConfig.SourcePath, err)
		}
		return nil
	}

	return createPackage(cfg, dir, file)
}

func createDeb(cfg pkgConfig, output io.Writer) error {
	pkgInfo := nfpm.WithDefaults(&nfpm.Info{
		Name:     cfg.Name,
		Arch:     runtime.GOARCH,
		Platform: "linux",
		// TODO: Wire in our version
		Version:         "0.0.0",
		Prerelease:      "dev",
		Maintainer:      "Redpanda Data <hi@redpanda.com>",
		Homepage:        "https://redpanda.com",
		Priority:        "optional",
		Section:         "misc",
		Description:     cfg.Deb.Description,
		DisableGlobbing: true,
		Overridables: nfpm.Overridables{
			Conflicts: nil, // TODO: fill this out
			Depends:   cfg.Deb.Depends,
			Deb: nfpm.Deb{
				Arch: runtime.GOARCH,
			},
			Scripts: nfpm.Scripts{
				PreInstall:  cfg.Deb.Scripts.PreInst,
				PostInstall: cfg.Deb.Scripts.PostInst,
				PreRemove:   cfg.Deb.Scripts.PreRm,
				PostRemove:  cfg.Deb.Scripts.PostRm,
			},
		},
	})
	for _, file := range cfg.PackageFiles {
		absPath, err := filepath.Abs(file.SourcePath)
		if err != nil {
			return fmt.Errorf("unable to resolve absolute path for %s: %w", file.SourcePath, err)
		}
		// If we don't resolve symlinks, then nfpm will override our Type to be a symlink
		// Use EvalSymlinks to fully resolve all levels of symlinks
		absPath, err = filepath.EvalSymlinks(absPath)
		if err != nil {
			return fmt.Errorf("unable to resolve symlinks for %s: %w", file.SourcePath, err)
		}
		fileInfo, err := os.Stat(absPath)
		if err != nil {
			return fmt.Errorf("unable to stat file %s: %w", absPath, err)
		}
		pkgInfo.Contents = append(pkgInfo.Contents, &files.Content{
			Source:      absPath,
			Destination: path.Join(file.Path, file.Name),
			Type:        files.TypeFile,
			FileInfo: &files.ContentFileInfo{
				Owner: "root",
				Group: "root",
				Mode:  fileInfo.Mode(),
				MTime: fileInfo.ModTime(),
				Size:  fileInfo.Size(),
			},
		})
	}

	// Add directories
	for _, dir := range cfg.PackageDirectories {
		pkgInfo.Contents = append(pkgInfo.Contents, &files.Content{
			Destination: dir,
			Type:        files.TypeDir,
			FileInfo: &files.ContentFileInfo{
				Owner: "root",
				Group: "root",
				Mode:  0755,
			},
		})
	}

	// Add symlinks
	for dest, source := range cfg.PackageSymlinks {
		pkgInfo.Contents = append(pkgInfo.Contents, &files.Content{
			Source:      source,
			Destination: dest,
			Type:        files.TypeSymlink,
		})
	}

	// Add systemd service file if provided
	if cfg.Deb.SystemdService != "" {
		absPath, err := filepath.Abs(cfg.Deb.SystemdService)
		if err != nil {
			return fmt.Errorf("unable to resolve absolute path for systemd service %s: %w", cfg.Deb.SystemdService, err)
		}
		absPath, err = filepath.EvalSymlinks(absPath)
		if err != nil {
			return fmt.Errorf("unable to resolve symlinks for systemd service %s: %w", cfg.Deb.SystemdService, err)
		}
		fileInfo, err := os.Stat(absPath)
		if err != nil {
			return fmt.Errorf("unable to stat systemd service file %s: %w", absPath, err)
		}
		pkgInfo.Contents = append(pkgInfo.Contents, &files.Content{
			Source:      absPath,
			Destination: "/usr/lib/systemd/system/" + filepath.Base(absPath),
			Type:        files.TypeFile,
			FileInfo: &files.ContentFileInfo{
				Owner: "root",
				Group: "root",
				Mode:  fileInfo.Mode(),
				MTime: fileInfo.ModTime(),
				Size:  fileInfo.Size(),
			},
		})
	}

	// Add systemd slice file if provided
	if cfg.Deb.SystemdSlice != "" {
		absPath, err := filepath.Abs(cfg.Deb.SystemdSlice)
		if err != nil {
			return fmt.Errorf("unable to resolve absolute path for systemd slice %s: %w", cfg.Deb.SystemdSlice, err)
		}
		absPath, err = filepath.EvalSymlinks(absPath)
		if err != nil {
			return fmt.Errorf("unable to resolve symlinks for systemd slice %s: %w", cfg.Deb.SystemdSlice, err)
		}
		fileInfo, err := os.Stat(absPath)
		if err != nil {
			return fmt.Errorf("unable to stat systemd slice file %s: %w", absPath, err)
		}
		pkgInfo.Contents = append(pkgInfo.Contents, &files.Content{
			Source:      absPath,
			Destination: "/lib/systemd/system/" + filepath.Base(absPath),
			Type:        files.TypeFile,
			FileInfo: &files.ContentFileInfo{
				Owner: "root",
				Group: "root",
				Mode:  fileInfo.Mode(),
				MTime: fileInfo.ModTime(),
				Size:  fileInfo.Size(),
			},
		})
	}

	deb.Default.SetPackagerDefaults(pkgInfo)
	return deb.Default.Package(pkgInfo, output)
}

func runTool() error {
	configPath := flag.String("config", "", "path to a configuration file to create the tarball")
	output := flag.String("output", "redpanda.tar.gz", "the output .tar.gz location")
	flag.Parse()
	var cfg pkgConfig
	if b, err := os.ReadFile(*configPath); err != nil {
		return fmt.Errorf("unable to read file: %w", err)
	} else if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("unable to parse config: %w", err)
	}

	if cfg.PackageType == "directory" {
		if err := createPackageDir(cfg, *output); err != nil {
			return fmt.Errorf("unable to create package directory: %w", err)
		}
		return nil
	}

	file, err := os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("unable to open output file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing output file %s: %v\n", *output, err)
		}
	}()
	bw := bufio.NewWriter(file)
	defer func() {
		if err := bw.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "error flushing buffered writer: %v\n", err)
		}
	}()

	switch cfg.PackageType {
	case "tarball":
		gw := gzip.NewWriter(bw)
		defer func() {
			if err := gw.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing gzip writer: %v\n", err)
			}
		}()
		if err := createTarball(cfg, gw); err != nil {
			return fmt.Errorf("unable to create tarball: %w", err)
		}
	case "deb":
		if err := createDeb(cfg, bw); err != nil {
			return fmt.Errorf("unable to create tarball: %w", err)
		}
	default:
		return fmt.Errorf("unknown package type: %q", cfg.PackageType)
	}

	return nil
}

func main() {
	if err := runTool(); err != nil {
		fmt.Fprintf(os.Stderr, "unable to generate package: %s", err.Error())
		os.Exit(1)
	}
}
