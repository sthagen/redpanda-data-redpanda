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
	"os"
	"path/filepath"
	"time"
)

type pkgConfig struct {
	RedpandaBinary    string   `json:"redpanda_binary"`
	RPKBinary         *string  `json:"rpk"`
	SharedLibraries   []string `json:"shared_libraries"`
	DefaultYAMLConfig *string  `json:"default_yaml_config"`
	Owner             int      `json:"owner"`
	DirectoryMode     bool     `json:"directory_mode"`
}

func createTarball(cfg pkgConfig, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer tw.Close()
	writeFile := func(tarPath, fsPath string) error {
		file, err := os.Open(fsPath)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			return err
		}
		err = tw.WriteHeader(&tar.Header{
			Name:     tarPath,
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
	// Collect the layout of the tarball first, then execute creating the tarball,
	// so that defining the structure is not muddied up with error handling.
	var ops []func() error
	file := func(tarPath, fsPath string) {
		ops = append(ops, func() error { return writeFile(tarPath, fsPath) })
	}
	dir := func(path string) {
		ops = append(ops, func() error { return writeDir(path) })
	}

	if cfg.DefaultYAMLConfig != nil {
		dir("etc/")
		dir("etc/redpanda/")
		file("etc/redpanda/redpanda.yaml", *cfg.DefaultYAMLConfig)
	}
	dir("opt/")
	dir("opt/redpanda/")

	dir("opt/redpanda/lib/")
	for _, so := range cfg.SharedLibraries {
		file(filepath.Join("opt/redpanda/lib", filepath.Base(so)), so)
	}

	dir("opt/redpanda/bin/")
	file("opt/redpanda/bin/redpanda", cfg.RedpandaBinary)
	if cfg.RPKBinary != nil {
		file("opt/redpanda/bin/rpk", *cfg.RPKBinary)
	}
	dir("var/")
	dir("var/lib/")
	dir("var/lib/redpanda/")
	dir("var/lib/redpanda/data/")

	// Now execute the above plan, handling errors.
	for _, op := range ops {
		if err := op(); err != nil {
			return err
		}
	}
	return nil
}
func copyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	dstFile.ReadFrom(srcFile)

	return nil
}

func createPackageDir(cfg pkgConfig, output string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}

	dir := func(path string) error {
		if err := os.Mkdir(filepath.Join(output, path), 0755); err != nil {
			return fmt.Errorf("Error creating directory %s: %v", path, err)
		}
		return nil
	}

	file := func(src string, dir string, name string) error {
		if err := copyFile(src, filepath.Join(output, dir, name)); err != nil {
			return fmt.Errorf("Error copying file %s: %v", src, err)
		}
		return nil
	}

	if cfg.DefaultYAMLConfig != nil {
		if err := dir("config"); err != nil {
			return err
		}
	}
	if err := dir("bin"); err != nil {
		return err
	}
	if err := dir("lib"); err != nil {
		return err
	}

	if err := dir("libexec"); err != nil {
		return err
	}

	for _, so := range cfg.SharedLibraries {
		if err := file(so, "lib", filepath.Base(so)); err != nil {
			return err
		}
	}

	if err := file(cfg.RedpandaBinary, "libexec", "redpanda"); err != nil {
		return err
	}

	if cfg.RPKBinary != nil {
		if err := file(*cfg.RPKBinary, "bin", "rpk"); err != nil {
			return err
		}
	}

	return nil
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

	if cfg.DirectoryMode {
		if err := createPackageDir(cfg, *output); err != nil {
			return fmt.Errorf("unable to create package directory: %w", err)
		}
	} else {
		file, err := os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("unable to open output file: %w", err)
		}
		defer file.Close()
		bw := bufio.NewWriter(file)
		defer bw.Flush()
		gw := gzip.NewWriter(bw)
		defer gw.Close()
		if err := createTarball(cfg, gw); err != nil {
			return fmt.Errorf("unable to create tarball: %w", err)
		}
	}
	return nil
}

func main() {
	if err := runTool(); err != nil {
		fmt.Fprintf(os.Stderr, "unable to generate package: %s", err.Error())
		os.Exit(1)
	}
}
