package uploader

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dknowles2/gdrive_sync/gdrive"
	"github.com/dustin/go-humanize"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/api/drive/v3"
)

var ignoreFiles = map[string]bool{
	".DS_Store": true,
}

type Uploader struct {
	watcher    *fsnotify.Watcher
	drive      *drive.Service
	inputDir   string
	outputDir  string
	folderId   string
	mu         sync.Mutex
	inProgress map[string]bool
}

func New(in, out string, d *drive.Service) (*Uploader, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}
	if err := w.Add(in); err != nil {
		return nil, fmt.Errorf("failed to add watcher for %s: %w", in, err)
	}
	folderId, err := gdrive.GetFolderId(d, out)
	if err != nil {
		return nil, err
	}
	u := &Uploader{
		watcher:    w,
		drive:      d,
		inputDir:   in,
		outputDir:  out,
		folderId:   folderId,
		inProgress: make(map[string]bool),
	}
	return u, nil
}

func (u *Uploader) Close() {
	u.watcher.Close()
}

func (u *Uploader) Run(ctx context.Context) error {
	if err := u.initialUpload(ctx); err != nil {
		return err
	}
	return u.watch(ctx)
}

func (u *Uploader) initialUpload(ctx context.Context) error {
	log.Printf("Looking for files already in %s...", u.inputDir)
	files, err := ioutil.ReadDir(u.inputDir)
	if err != nil {
		return fmt.Errorf("failed to list directory contents: %w", err)
	}
	for _, f := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// carry on
		}
		if shouldIgnore(f.Name()) {
			continue
		}
		go u.upload(ctx, filepath.Join(u.inputDir, f.Name()))
	}
	return nil
}

func (u *Uploader) watch(ctx context.Context) error {
	first := true
	last := time.Now()
	for {
		if first || time.Since(last) > 1*time.Second {
			log.Printf("Waiting for new files in %s...", u.inputDir)
		}
		first = false
		last = time.Now()

		select {
		case event, ok := <-u.watcher.Events:
			if !ok {
				// channel closed, exit cleanly
				return nil
			}
			u.mu.Lock()
			inProgress := u.inProgress[event.Name]
			u.mu.Unlock()
			if inProgress || event.Op&fsnotify.Write != fsnotify.Write || shouldIgnore(event.Name) {
				continue
			}
			if _, err := os.Stat(event.Name); os.IsNotExist(err) {
				// File has already been removed; ignore.
				continue
			}
			log.Printf("Found new file: %s", event.Name)
			go u.upload(ctx, event.Name)
		case err, ok := <-u.watcher.Errors:
			if !ok {
				return err
			}
			log.Printf("error: %s", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func shouldIgnore(f string) bool {
	baseFile := filepath.Base(f)
	return ignoreFiles[baseFile] || strings.HasPrefix(baseFile, ".")
}

func waitForFileWrite(ctx context.Context, f string) error {
	return waitForFileSizeToStabilize(ctx, f)
}

func sleep(ctx context.Context, t time.Duration) error {
	select {
	case <-time.Tick(t):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForFileSizeToStabilize(ctx context.Context, f string) error {
	var s, c int64
	first := true
	for {
		if first {
			log.Printf("Waiting for %s to stop growing...", f)
			first = false
		}
		fi, err := os.Stat(f)
		if err != nil {
			return err
		}
		if s == fi.Size() {
			if c < 10 {
				c++
			} else {
				return nil
			}
		} else {
			// reset the count, in case the file size temporarily stalled.
			c = 0
		}
		s = fi.Size()
		if err := sleep(ctx, 1*time.Second); err != nil {
			return err
		}
	}
	return nil // unreachable
}

func waitForFileToClose(ctx context.Context, f string) error {
	first := true
	for {
		if first {
			log.Printf("Waiting for %s to be closed...", f)
			first = false
		}
		isOpen, err := fileIsOpen(ctx, f)
		if err != nil {
			return err
		}
		if !isOpen {
			return nil
		}
		if err := sleep(ctx, 1*time.Second); err != nil {
			return err
		}
	}
	return nil // unreachable
}

func fileIsOpen(ctx context.Context, f string) (bool, error) {
	lsof, err := exec.LookPath("lsof")
	if err != nil {
		return false, err
	}
	_, err = exec.CommandContext(ctx, lsof, "-w", "-F", "p", f).Output()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// lsof will return an error return code and an empty stderr if the file is not open.
		if string(exitErr.Stderr) == "" {
			return false, nil
		}
		return false, err
	}
	return false, err
}

func (u *Uploader) upload(ctx context.Context, f string) {
	u.mu.Lock()
	if u.inProgress[f] {
		return
	}
	u.inProgress[f] = true
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		u.inProgress[f] = false
		u.mu.Unlock()
	}()

	if err := waitForFileWrite(ctx, f); err != nil {
		log.Printf("failed waiting for file %s: %s", f, err)
		return
	}

	if err := u.doUpload(ctx, f); err != nil {
		log.Printf("failed to upload file %s: %s", f, err)
		return
	}

	log.Printf("Removing %s", f)
	if err := os.Remove(f); err != nil {
		log.Printf("failed to delete file %s: %s", f, err)
		return
	}
}

func (u *Uploader) doUpload(ctx context.Context, name string) error {
	log.Printf("Uploading file: %s", name)

	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	driveFile := &drive.File{
		Name:    filepath.Base(name),
		Parents: []string{u.folderId},
	}
	progress := func(now, size int64) {
		log.Printf("uploaded %s/%s of %s", humanize.Bytes(uint64(now)), humanize.Bytes(uint64(size)), name)
	}
	_, err = u.drive.Files.Create(driveFile).ResumableMedia(ctx, f, fi.Size(), "").ProgressUpdater(progress).Do()
	if err != nil {
		return err
	}
	return nil
}
