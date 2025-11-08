package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 {
		return fmt.Errorf("unknown argument %q (this program should be run by protoc, not directly)", os.Args[1])
	}

	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("failed to read input: %w", err)
	}

	req := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(in, req); err != nil {
		return fmt.Errorf("failed to parse input proto: %w", err)
	}

	resp, err := generate(req)
	if err != nil {
		// Return error in response
		resp = &pluginpb.CodeGeneratorResponse{
			Error: proto.String(err.Error()),
		}
	}

	out, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal output proto: %w", err)
	}

	if _, err := os.Stdout.Write(out); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	return nil
}
