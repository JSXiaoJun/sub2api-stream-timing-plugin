package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	root := flag.String("root", ".", "plugin root")
	keyFile := flag.String("key", ".keys/publisher.private", "private key (kept outside package)")
	keyID := flag.String("key-id", "inventory-stream-timing-v1", "publisher id")
	out := flag.String("out", "dist/stream-timing-0.1.4.s2plugin", "output")
	verify := flag.Bool("verify", false, "verify package only")
	flag.Parse()
	must(os.Chdir(*root))
	pubFile := filepath.Join("dist", "publisher.public")
	if *verify {
		verifyPackage(*out, pubFile)
		return
	}
	must(os.MkdirAll(filepath.Dir(*keyFile), 0700))
	must(os.MkdirAll("dist", 0755))
	var priv ed25519.PrivateKey
	if b, err := os.ReadFile(*keyFile); err == nil {
		raw, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
		must(e)
		if len(raw) != ed25519.PrivateKeySize {
			panic("invalid signing key")
		}
		priv = raw
	} else if os.IsNotExist(err) {
		_, p, e := ed25519.GenerateKey(rand.Reader)
		must(e)
		priv = p
		f, e := os.OpenFile(*keyFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		must(e)
		_, e = f.WriteString(base64.StdEncoding.EncodeToString(priv) + "\n")
		must(e)
		must(f.Close())
	} else {
		panic(err)
	}
	pub := priv.Public().(ed25519.PublicKey)
	must(os.WriteFile(pubFile, []byte(base64.StdEncoding.EncodeToString(pub)+"\n"), 0644))
	files := map[string][]byte{}
	for _, pair := range [][2]string{{"build/stream-timing-linux-amd64", "runtimes/linux-amd64/stream-timing"}, {"build/stream-timing-windows-amd64.exe", "runtimes/windows-amd64/stream-timing.exe"}} {
		b, e := os.ReadFile(pair[0])
		must(e)
		files[pair[1]] = b
	}
	must(filepath.WalkDir("ui", func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		b, e := os.ReadFile(p)
		if e == nil {
			files[filepath.ToSlash(p)] = b
		}
		return e
	}))
	b, e := os.ReadFile("manifest.source.json")
	must(e)
	var m map[string]any
	must(json.Unmarshal(b, &m))
	hashes := map[string]string{}
	for name, b := range files {
		h := sha256.Sum256(b)
		hashes[name] = hex.EncodeToString(h[:])
	}
	m["files"] = hashes
	m["runtimes"] = map[string]any{"linux-amd64": map[string]string{"path": "runtimes/linux-amd64/stream-timing"}, "windows-amd64": map[string]string{"path": "runtimes/windows-amd64/stream-timing.exe"}}
	manifest, e := json.MarshalIndent(m, "", "  ")
	must(e)
	files["manifest.json"] = manifest
	sig, e := json.Marshal(map[string]string{"algorithm": "ed25519", "key_id": *keyID, "signature": base64.StdEncoding.EncodeToString(ed25519.Sign(priv, manifest))})
	must(e)
	files["signature.json"] = sig
	f, e := os.Create(*out)
	must(e)
	z := zip.NewWriter(f)
	names := make([]string, 0, len(files))
	for k := range files {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		h := &zip.FileHeader{Name: n, Method: zip.Deflate}
		h.SetModTime(time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC))
		h.SetMode(0644)
		if strings.HasPrefix(n, "runtimes/") {
			h.SetMode(0755)
		}
		w, e := z.CreateHeader(h)
		must(e)
		_, e = w.Write(files[n])
		must(e)
	}
	must(z.Close())
	must(f.Close())
	yaml := "plugins:\n  allow_unsigned: false\n  trusted_publishers:\n    " + *keyID + ": \"" + base64.StdEncoding.EncodeToString(pub) + "\"\n"
	must(os.WriteFile("dist/trusted-publisher.example.yaml", []byte(yaml), 0644))
	verifyPackage(*out, pubFile)
}
func verifyPackage(name, pubFile string) {
	b, e := os.ReadFile(pubFile)
	must(e)
	pub, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	must(e)
	z, e := zip.OpenReader(name)
	must(e)
	defer z.Close()
	files := map[string][]byte{}
	for _, f := range z.File {
		if strings.Contains(f.Name, "..") || strings.HasPrefix(f.Name, "/") || strings.Contains(f.Name, "\\") {
			panic("unsafe package path")
		}
		if _, ok := files[f.Name]; ok {
			panic("duplicate file")
		}
		r, e := f.Open()
		must(e)
		b, e := io.ReadAll(io.LimitReader(r, 128*1024*1024))
		r.Close()
		must(e)
		files[f.Name] = b
	}
	var sig struct{ Algorithm, Signature string }
	must(json.Unmarshal(files["signature.json"], &sig))
	signature, e := base64.StdEncoding.DecodeString(sig.Signature)
	must(e)
	if sig.Algorithm != "ed25519" || len(pub) != 32 || !ed25519.Verify(pub, files["manifest.json"], signature) {
		panic("signature mismatch")
	}
	var m struct{ Files map[string]string }
	must(json.Unmarshal(files["manifest.json"], &m))
	if len(files) != len(m.Files)+2 {
		panic("undeclared files")
	}
	for n, expected := range m.Files {
		b, ok := files[n]
		h := sha256.Sum256(b)
		if !ok || hex.EncodeToString(h[:]) != expected {
			panic("file hash mismatch: " + n)
		}
	}
	b, e = os.ReadFile(name)
	must(e)
	h := sha256.Sum256(b)
	fmt.Printf("PACKAGE VERIFIED files=%d sha256=%x\n", len(files), h)
}
