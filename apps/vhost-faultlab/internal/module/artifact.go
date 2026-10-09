package module

import (
	"context"
	"debug/elf"
	"embed"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

//go:embed assets/Makefile assets/vhost_fault.c
var source embed.FS

// OpenArtifact pins and validates the selected module before sensitive binding
// acquisition. The kernel still validates version/signature/support on loading.
func OpenArtifact(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := validateArtifact(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func validateArtifact(f *os.File) error {
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || stat.Size() > 64<<20 {
		return errors.New("module must be a regular ELF file at most 64 MiB")
	}
	object, err := elf.NewFile(f)
	if err != nil {
		return fmt.Errorf("module ELF: %w", err)
	}
	if object.Machine != elf.EM_X86_64 || object.Class != elf.ELFCLASS64 || object.Type != elf.ET_REL {
		return errors.New("module must be a Linux x86-64 relocatable ELF")
	}
	info := object.Section(".modinfo")
	if info == nil || info.Size > 1<<20 {
		return errors.New("module has no bounded .modinfo section")
	}
	data, err := info.Data()
	if err != nil {
		return err
	}
	for _, field := range strings.Split(string(data), "\x00") {
		if field == "name="+Name {
			return nil
		}
	}
	return errors.New("module name is not vhost_fault")
}

type BuildOptions struct {
	KernelBuildDir, Compiler, Output string
}

// Prepare builds only the embedded module sources. It never loads an asset or
// modifies kernel configuration. An existing output is never overwritten.
func Prepare(ctx context.Context, options BuildOptions) (result string, resultErr error) {
	if !filepath.IsAbs(options.KernelBuildDir) || !filepath.IsAbs(options.Output) ||
		strings.ContainsAny(options.KernelBuildDir, "\x00\n\r$`\"") {
		return "", errors.New("kernel build and output paths must be absolute and the build path must be a literal path")
	}
	if options.Compiler == "" || strings.ContainsAny(options.Compiler, " \t\r\n$`;&|\"'\\") {
		return "", errors.New("compiler must be one executable name or literal path")
	}
	compiler, err := exec.LookPath(options.Compiler)
	if err != nil {
		return "", err
	}
	compiler, err = filepath.Abs(compiler)
	if err != nil || strings.ContainsAny(compiler, " \t\r\n$`;&|\"'\\") {
		return "", errors.New("compiler resolved to an unsupported executable path")
	}
	if _, err := os.Stat(options.Output); err == nil {
		return "", errors.New("output already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	dir, err := os.MkdirTemp("", "vhost-faultlab-build-")
	if err != nil {
		return "", err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(dir)) }()
	for _, name := range []string{"Makefile", "vhost_fault.c"} {
		data, err := source.ReadFile("assets/" + name)
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			return "", err
		}
	}
	log := new(buildLog)
	cmd := exec.CommandContext(ctx, "make", "-C", dir, "KDIR="+options.KernelBuildDir, "CC="+compiler)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kernel module build: %w: %s", err, log.String())
	}
	artifact, err := OpenArtifact(filepath.Join(dir, "vhost_fault.ko"))
	if err != nil {
		return "", err
	}
	defer artifact.Close()
	out, err := os.OpenFile(options.Output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(out, artifact)
	closeErr := out.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", errors.Join(err, os.Remove(options.Output))
	}
	return options.Output, nil
}

const buildLogLimit = 64 << 10

type buildLog struct {
	mu   sync.Mutex
	data []byte
	lost bool
}

func (w *buildLog) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(data)
	if n >= buildLogLimit {
		w.data = append(w.data[:0], data[n-buildLogLimit:]...)
		w.lost = true
	} else {
		extra := len(w.data) + n - buildLogLimit
		if extra > 0 {
			copy(w.data, w.data[extra:])
			w.data = w.data[:len(w.data)-extra]
			w.lost = true
		}
		w.data = append(w.data, data...)
	}
	return n, nil
}

func (w *buildLog) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lost {
		return "[earlier build output omitted]\n" + string(w.data)
	}
	return string(w.data)
}
