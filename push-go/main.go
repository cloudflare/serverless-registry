package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

const (
	defaultCacheDir    = ".cache"
	defaultImageDir    = ".output-image"
	defaultMaxRetries  = 3
	defaultConcurrency = 5
)

type DockerManifest struct {
	Config string   `json:"Config"`
	Layers []string `json:"Layers"`
}

type OCILayer struct {
	MediaType string `json:"mediaType"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest"`
}

type OCIConfig struct {
	MediaType string `json:"mediaType"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest"`
}

type OCIManifest struct {
	SchemaVersion int        `json:"schemaVersion"`
	MediaType     string     `json:"mediaType"`
	Config        OCIConfig  `json:"config"`
	Layers        []OCILayer `json:"layers"`
}

type PushConfig struct {
	Image        string
	Username     string
	Password     string
	InsecureHTTP bool
	MaxRetries   int
	Concurrency  int
	CacheDir     string
	ImageDir     string
}

func main() {
	var config PushConfig

	flag.StringVar(&config.Username, "username", os.Getenv("USERNAME_REGISTRY"), "Registry username")
	flag.BoolVar(&config.InsecureHTTP, "insecure", os.Getenv("INSECURE_HTTP_PUSH") == "true", "Use HTTP instead of HTTPS")
	flag.IntVar(&config.MaxRetries, "max-retries", defaultMaxRetries, "Maximum number of retries per layer")
	flag.IntVar(&config.Concurrency, "concurrency", defaultConcurrency, "Number of concurrent uploads")
	flag.StringVar(&config.CacheDir, "cache-dir", defaultCacheDir, "Cache directory for compressed layers")
	flag.StringVar(&config.ImageDir, "image-dir", defaultImageDir, "Directory to extract image tar")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <image>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	config.Image = flag.Arg(0)

	// Read password from stdin
	if config.Username != "" {
		if term.IsTerminal(int(syscall.Stdin)) {
			fmt.Fprintf(os.Stderr, "You need to pass the password via stdin\n\n\techo <PASSWORD> | %s <image>\n", os.Args[0])
			os.Exit(1)
		}
		passwordBytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading password: %v\n", err)
			os.Exit(1)
		}
		config.Password = strings.TrimSpace(string(passwordBytes))
	}

	if config.Username == "" || config.Password == "" {
		if os.Getenv("SKIP_AUTH") != "true" {
			fmt.Fprintf(os.Stderr, "Username or password not defined, push won't be able to authenticate with registry\n")
			os.Exit(1)
		}
	}

	if err := pushImage(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func pushImage(config *PushConfig) error {
	// Get image ID
	fmt.Println("Preparing image...")
	cmd := exec.Command("docker", "images", "--format", "{{ .ID }}", config.Image)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("image %s doesn't exist or docker daemon is not running: %v", config.Image, err)
	}

	imageID := strings.TrimSpace(string(output))
	if imageID == "" {
		return fmt.Errorf("image %s doesn't exist", config.Image)
	}

	fmt.Printf("Image %s found locally, saving to disk...\n", config.Image)

	// Save image to tar
	tarFile := imageID + ".tar"
	if _, err := os.Stat(tarFile); os.IsNotExist(err) {
		cmd = exec.Command("docker", "save", config.Image, "--output", tarFile)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("error saving image: %v", err)
		}
		fmt.Printf("Image saved as %s, extracting...\n", tarFile)
	}

	// Extract tar
	if err := os.MkdirAll(config.ImageDir, 0755); err != nil {
		return fmt.Errorf("error creating image directory: %v", err)
	}

	if err := extractTar(tarFile, config.ImageDir); err != nil {
		return fmt.Errorf("error extracting tar: %v", err)
	}
	fmt.Printf("Extracted to %s\n", config.ImageDir)

	// Read manifest
	manifestPath := filepath.Join(config.ImageDir, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("error reading manifest: %v", err)
	}

	var manifests []DockerManifest
	if err := json.Unmarshal(manifestData, &manifests); err != nil {
		return fmt.Errorf("error parsing manifest: %v", err)
	}

	if len(manifests) == 0 {
		return fmt.Errorf("unexpected manifest of length 0")
	}

	if len(manifests) > 1 {
		fmt.Println("Manifest resolved to multiple images, picking the first one")
	}

	manifest := manifests[0]

	// Create cache directory
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return fmt.Errorf("error creating cache directory: %v", err)
	}

	// Compress layers
	fmt.Println("Compressing...")
	compressedDigests, err := compressLayers(manifest.Layers, config.ImageDir, config.CacheDir, config.Concurrency)
	if err != nil {
		return fmt.Errorf("error compressing layers: %v", err)
	}

	// Read config
	configPath := filepath.Join(config.ImageDir, manifest.Config)
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("error reading config: %v", err)
	}

	configHash := sha256.Sum256(configData)
	configDigest := fmt.Sprintf("sha256:%x", configHash)

	// Parse image URL
	proto := "https"
	if config.InsecureHTTP {
		proto = "http"
		fmt.Println("!! Using plain HTTP !!")
	}

	imageURL := proto + "://" + config.Image
	imageHost, repoPath, tag, err := parseImageURL(imageURL)
	if err != nil {
		return fmt.Errorf("error parsing image URL: %v", err)
	}

	// Create auth header
	authHeader := ""
	if config.Username != "" && config.Password != "" {
		authHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(config.Username+":"+config.Password))
	}

	// Push layers
	fmt.Println("Starting push to remote")
	pusher := &LayerPusher{
		Proto:       proto,
		Host:        imageHost,
		RepoPath:    repoPath,
		AuthHeader:  authHeader,
		MaxRetries:  config.MaxRetries,
		Concurrency: config.Concurrency,
	}

	layersManifest := make([]OCILayer, 0, len(compressedDigests))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, config.Concurrency)
	errChan := make(chan error, len(compressedDigests)+1)
	var mu sync.Mutex

	for _, digest := range compressedDigests {
		layerPath := filepath.Join(config.CacheDir, digest)
		info, err := os.Stat(layerPath)
		if err != nil {
			return fmt.Errorf("error stating layer %s: %v", digest, err)
		}

		layer := OCILayer{
			MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
			Size:      info.Size(),
			Digest:    "sha256:" + digest,
		}

		mu.Lock()
		layersManifest = append(layersManifest, layer)
		mu.Unlock()

		wg.Add(1)
		go func(digest string, size int64) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := pusher.PushLayer("sha256:"+digest, filepath.Join(config.CacheDir, digest), size); err != nil {
				errChan <- err
			}
		}(digest, info.Size())
	}

	// Push config
	wg.Add(1)
	go func() {
		defer wg.Done()
		semaphore <- struct{}{}
		defer func() { <-semaphore }()

		if err := pusher.PushLayerFromBytes(configDigest, configData); err != nil {
			errChan <- err
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Upload manifest
	ociManifest := OCIManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config: OCIConfig{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Size:      int64(len(configData)),
			Digest:    configDigest,
		},
		Layers: layersManifest,
	}

	manifestJSON, err := json.Marshal(ociManifest)
	if err != nil {
		return fmt.Errorf("error marshaling manifest: %v", err)
	}

	manifestURL := fmt.Sprintf("%s://%s/v2%s/manifests/%s", proto, imageHost, repoPath, tag)
	req, err := http.NewRequest("PUT", manifestURL, bytes.NewReader(manifestJSON))
	if err != nil {
		return fmt.Errorf("error creating manifest request: %v", err)
	}

	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	req.Header.Set("Content-Type", ociManifest.MediaType)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error uploading manifest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("manifest upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Println(string(manifestJSON))
	return nil
}

func extractTar(tarPath, destDir string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	tr := tar.NewReader(f)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			outFile, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
		}
	}
	return nil
}

func compressLayers(layers []string, imageDir, cacheDir string, concurrency int) ([]string, error) {
	type result struct {
		index  int
		digest string
		err    error
	}

	results := make(chan result, len(layers))
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, layer := range layers {
		wg.Add(1)
		go func(idx int, layerPath string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			digest, err := compressLayer(layerPath, imageDir, cacheDir)
			results <- result{index: idx, digest: digest, err: err}
		}(i, layer)
	}

	wg.Wait()
	close(results)

	digests := make([]string, len(layers))
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		digests[r.index] = r.digest
	}

	return digests, nil
}

func compressLayer(layer, imageDir, cacheDir string) (string, error) {
	layerPath := filepath.Join(imageDir, layer)

	// Handle both blob/sha256/<hash> and <hash>/layer.tar formats
	var layerName string
	if strings.HasSuffix(layer, ".tar") {
		layerName = filepath.Base(filepath.Dir(layer))
	} else {
		layerName = filepath.Base(layer)
	}

	// Check if already cached
	ptrPath := filepath.Join(cacheDir, layerName+"-ptr")
	if data, err := os.ReadFile(ptrPath); err == nil {
		return string(data), nil
	}

	// Compress layer
	inFile, err := os.Open(layerPath)
	if err != nil {
		return "", err
	}
	defer inFile.Close()

	inProgressPath := filepath.Join(cacheDir, layerName+"-in-progress")
	os.Remove(inProgressPath)

	outFile, err := os.Create(inProgressPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	hasher := sha256.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(outFile, hasher))
	gzWriter.Name = ""
	gzWriter.Comment = ""
	gzWriter.ModTime = time.Time{}

	if _, err := io.Copy(gzWriter, inFile); err != nil {
		return "", err
	}

	if err := gzWriter.Close(); err != nil {
		return "", err
	}

	digest := fmt.Sprintf("%x", hasher.Sum(nil))
	finalPath := filepath.Join(cacheDir, digest)

	if err := os.Rename(inProgressPath, finalPath); err != nil {
		return "", err
	}

	if err := os.WriteFile(ptrPath, []byte(digest), 0644); err != nil {
		return "", err
	}

	return digest, nil
}

func parseImageURL(imageURL string) (host, repoPath, tag string, err error) {
	// Remove protocol
	parts := strings.SplitN(imageURL, "://", 2)
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("invalid image URL: %s", imageURL)
	}

	remainder := parts[1]

	// Split host and path
	hostAndPath := strings.SplitN(remainder, "/", 2)
	if len(hostAndPath) != 2 {
		return "", "", "", fmt.Errorf("invalid image URL format: %s", imageURL)
	}

	host = hostAndPath[0]
	pathWithTag := hostAndPath[1]

	// Split path and tag
	pathParts := strings.Split(pathWithTag, ":")
	if len(pathParts) > 1 {
		tag = pathParts[len(pathParts)-1]
		repoPath = "/" + strings.Join(pathParts[:len(pathParts)-1], ":")
	} else {
		tag = "latest"
		repoPath = "/" + pathWithTag
	}

	return host, repoPath, tag, nil
}

type LayerPusher struct {
	Proto       string
	Host        string
	RepoPath    string
	AuthHeader  string
	MaxRetries  int
	Concurrency int
}

func (p *LayerPusher) PushLayerFromBytes(digest string, data []byte) error {
	return p.pushLayerInternal(digest, bytes.NewReader(data), int64(len(data)))
}

func (p *LayerPusher) PushLayer(digest, filePath string, size int64) error {
	for retry := 0; retry < p.MaxRetries; retry++ {
		f, err := os.Open(filePath)
		if err != nil {
			return err
		}

		err = p.pushLayerInternal(digest, f, size)
		f.Close()

		if err == nil {
			return nil
		}

		if retry < p.MaxRetries-1 {
			fmt.Printf("%s failed to upload, %d retries left: %v\n", digest, p.MaxRetries-retry-1, err)
		}
	}

	return fmt.Errorf("failed to push layer %s after %d retries", digest, p.MaxRetries)
}

func (p *LayerPusher) pushLayerInternal(digest string, reader io.Reader, size int64) error {
	// Check if blob exists
	blobURL := fmt.Sprintf("%s://%s/v2%s/blobs/%s", p.Proto, p.Host, p.RepoPath, digest)
	req, err := http.NewRequest("HEAD", blobURL, nil)
	if err != nil {
		return err
	}

	if p.AuthHeader != "" {
		req.Header.Set("Authorization", p.AuthHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("%s already exists...\n", digest)
		return nil
	}

	if resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status checking blob: %d", resp.StatusCode)
	}

	// Create upload
	uploadURL := fmt.Sprintf("%s://%s/v2%s/blobs/uploads/", p.Proto, p.Host, p.RepoPath)
	req, err = http.NewRequest("POST", uploadURL, nil)
	if err != nil {
		return err
	}

	if p.AuthHeader != "" {
		req.Header.Set("Authorization", p.AuthHeader)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Get max chunk length
	maxChunkLength := int64(470 * 1024 * 1024) // 470MB default
	if chunkHeader := resp.Header.Get("oci-chunk-max-length"); chunkHeader != "" {
		if parsed, err := strconv.ParseInt(chunkHeader, 10, 64); err == nil {
			maxChunkLength = parsed
		}
	}

	uploadID := resp.Header.Get("docker-upload-uuid")
	if uploadID == "" {
		return fmt.Errorf("Docker-Upload-UUID not defined in headers")
	}

	location := resp.Header.Get("location")
	if location == "" {
		location = fmt.Sprintf("/v2%s/blobs/uploads/%s", p.RepoPath, uploadID)
	}

	// Upload in chunks
	written := int64(0)
	buf := make([]byte, maxChunkLength)

	for written < size {
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}

		if n == 0 {
			break
		}

		chunkData := buf[:n]
		// Range header is cumulative from start (0 to current_end), not per-chunk
		rangeHeader := fmt.Sprintf("0-%d", written+int64(n)-1)

		patchURL := p.parseLocation(location)
		req, err := http.NewRequest("PATCH", patchURL, bytes.NewReader(chunkData))
		if err != nil {
			return err
		}

		if p.AuthHeader != "" {
			req.Header.Set("Authorization", p.AuthHeader)
		}
		req.Header.Add("range", rangeHeader)
		req.Header.Set("Content-Length", strconv.FormatInt(int64(n), 10))
		req.ContentLength = int64(n)

		patchResp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		if patchResp.StatusCode < 200 || patchResp.StatusCode >= 300 {
			body, _ := io.ReadAll(patchResp.Body)
			patchResp.Body.Close()
			return fmt.Errorf("patch chunk failed with status %d: %s", patchResp.StatusCode, string(body))
		}

		location = patchResp.Header.Get("location")
		if location == "" {
			location = patchURL
		}
		patchResp.Body.Close()

		written += int64(n)
		leftBytes := size - written
		if leftBytes > 0 {
			fmt.Printf("%s: %d upload bytes left.\n", digest, leftBytes)
		}
	}

	// Finalize upload
	// IMPORTANT: The location may already contain query parameters like _stateHash
	// We need to append the digest parameter, not replace all parameters
	parsedLocation := p.parseLocation(location)
	separator := "?"
	if strings.Contains(parsedLocation, "?") {
		separator = "&"
	}
	finalURL := parsedLocation + separator + "digest=" + digest
	rangeHeader := fmt.Sprintf("0-%d", written-1)

	req, err = http.NewRequest("PUT", finalURL, nil)
	if err != nil {
		return err
	}

	// Clear all default headers that Go might add
	req.Header = make(http.Header)

	// Only set the headers that TypeScript sends
	if p.AuthHeader != "" {
		req.Header.Set("Authorization", p.AuthHeader)
	}
	req.Header.Set("Range", rangeHeader)
	// Don't set Content-Length at all - let the client decide

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("finalize upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Pushed %s\n", digest)
	return nil
}

func (p *LayerPusher) parseLocation(location string) string {
	if strings.HasPrefix(location, "/") {
		return fmt.Sprintf("%s://%s%s", p.Proto, p.Host, location)
	}
	return location
}
