package module_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/module"
)

func moduleELF(name string) []byte {
	strings := []byte("\x00.shstrtab\x00.modinfo\x00")
	info := []byte("name=" + name + "\x00")
	data := make([]byte, 256+len(strings)+len(info))
	copy(data, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(data[16:], 1)  // ET_REL
	binary.LittleEndian.PutUint16(data[18:], 62) // EM_X86_64
	binary.LittleEndian.PutUint32(data[20:], 1)
	binary.LittleEndian.PutUint64(data[40:], 64)
	binary.LittleEndian.PutUint16(data[52:], 64)
	binary.LittleEndian.PutUint16(data[58:], 64)
	binary.LittleEndian.PutUint16(data[60:], 3)
	binary.LittleEndian.PutUint16(data[62:], 1)
	for _, section := range []struct{ offset, name, kind, start, size uint64 }{
		{128, 1, 3, 256, uint64(len(strings))},
		{192, 11, 1, uint64(256 + len(strings)), uint64(len(info))},
	} {
		row := data[section.offset:]
		binary.LittleEndian.PutUint32(row, uint32(section.name))
		binary.LittleEndian.PutUint32(row[4:], uint32(section.kind))
		binary.LittleEndian.PutUint64(row[24:], section.start)
		binary.LittleEndian.PutUint64(row[32:], section.size)
		binary.LittleEndian.PutUint64(row[48:], 1)
	}
	copy(data[256:], strings)
	copy(data[256+len(strings):], info)
	return data
}

func TestArtifactNameAndArchitectureAreValidatedBeforeLoading(t *testing.T) {
	wrongArchitecture := moduleELF("vhost_fault")
	binary.LittleEndian.PutUint16(wrongArchitecture[18:], 183) // EM_AARCH64
	for _, tc := range []struct {
		name string
		data []byte
		ok   bool
	}{
		{"expected", moduleELF("vhost_fault"), true},
		{"other module", moduleELF("unrelated"), false},
		{"wrong architecture", wrongArchitecture, false},
		{"non ELF", []byte("not a module"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "module.ko")
			if err := os.WriteFile(path, tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			file, err := module.OpenArtifact(path)
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected validation: %v", err)
			}
			if file != nil {
				file.Close()
			}
		})
	}
}

func TestPrepareRejectsCommandSubstitutionAndExistingOutput(t *testing.T) {
	for _, build := range []module.BuildOptions{
		{KernelBuildDir: "/kernel/$(touch marker)", Compiler: "cc", Output: "/tmp/no-output.ko"},
		{KernelBuildDir: "/kernel", Compiler: "cc; touch marker", Output: "/tmp/no-output.ko"},
		{KernelBuildDir: "relative", Compiler: "cc", Output: "/tmp/no-output.ko"},
	} {
		if _, err := module.Prepare(context.Background(), build); err == nil {
			t.Fatal("unsafe build options accepted")
		}
	}
	output := filepath.Join(t.TempDir(), "existing.ko")
	original := []byte("retained artifact")
	if err := os.WriteFile(output, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Prepare(context.Background(), module.BuildOptions{KernelBuildDir: t.TempDir(), Compiler: "go", Output: output}); err == nil {
		t.Fatal("existing artifact accepted for overwrite")
	}
	got, err := os.ReadFile(output)
	if err != nil || string(got) != string(original) {
		t.Fatal("existing artifact changed", err)
	}
}
