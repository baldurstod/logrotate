package logrotate

import (
	"sync"
	"os"
	"strings"
	"sort"
	"io/fs"
	"errors"
	"fmt"
	"time"
	"path/filepath"
)

const TIME_FORMAT = "_20060102_150405.000"
const MEGABYTE = 1000 * 1000

type Logger struct {
	Filename string
	FileMode fs.FileMode
	Size int64
	MaxAge int
	MaxBackups int

	logMutex sync.Mutex
	file *os.File
	fileSize int64

	cleanupOnce sync.Once
	cleanupChannel chan bool
	wg sync.WaitGroup
}

func (this *Logger) Write(p []byte) (int, error) {
	this.logMutex.Lock()
	defer this.logMutex.Unlock()

	if this.file == nil {
		if err := this.openFile(); err != nil {
			return 0, err
		}
	}

	if (this.fileSize > this.size()) {
		if err := this.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := this.file.Write(p)
	this.fileSize += int64(n)

	return n, err
}

func (this *Logger) Close() error {
	this.logMutex.Lock()
	defer this.logMutex.Unlock()

	return this.closeFile()
}

func (this *Logger) Terminate() {
	this.logMutex.Lock()
	defer this.logMutex.Unlock()

	if this.cleanupChannel != nil {
		close(this.cleanupChannel)
	}
	this.wg.Wait()
}

func (this *Logger) createFile() error {
	f, err := os.OpenFile(this.filename(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, this.filemode())
	if err != nil {
		return fmt.Errorf("Error while opening file in createFile: %s", err)
	}
	this.file = f
	this.fileSize = 0
	return nil
}

func (this *Logger) closeFile() error {
	if this.file == nil {
		return nil
	}
	err := this.file.Close()
	this.file = nil
	return err
}

func (this *Logger) openFile() error {
	_, err := os.Stat(this.filename())
	if errors.Is(err, fs.ErrNotExist) {
		return this.openNewFile()
	}

	if err != nil {
		return fmt.Errorf("Error in openFile: %s", err)
	} else {
		return this.openExistingFile()
	}

	return nil
}

func (this *Logger) openNewFile() error {
	err := os.MkdirAll(this.dir(), 0755)
	if err != nil {
		return fmt.Errorf("Error while creating dir in openNewFile: %s", err)
	}

	name := this.filename()
	_, err = os.Stat(name)
	if err == nil {
		this.rotate()
	} else {
		return this.createFile()
	}

	return nil
}

func (this *Logger) openExistingFile() error {
	name := this.filename()
	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, this.filemode())
	if err != nil {
		return this.openNewFile()
	}

	info, err := os.Stat(name);
	if err != nil {
		return fmt.Errorf("Error in openExistingFile: %s", err)
	}
	this.file = f
	this.fileSize = info.Size()
	return nil
}

func (this *Logger) rotate() error {
	timestamp := time.Now().Format(TIME_FORMAT)

	name := this.filename()
	newName := name + timestamp
	this.closeFile()

	if err := os.Rename(name, newName); err != nil {
		return fmt.Errorf("Error while renaming file in rotate: %s", err)
	}

	this.cleanup()

	return this.createFile()
}

func (this *Logger) cleanup() {
	this.cleanupOnce.Do(func() {
		this.cleanupChannel = make(chan bool, 1)
		go this.cleanupLoop()
	})

	select {
	case this.cleanupChannel <- true:
	default:
	}

}

func (this *Logger) cleanupLoop() {
	this.wg.Add(1);
	for range this.cleanupChannel {
		this.processCleanup()
	}
	this.wg.Done();
}

func (this *Logger) processCleanup() {
	if this.MaxBackups == 0 && this.MaxAge == 0 {
		return
	}

	cutoff := time.Now().Add(time.Duration(int64(-this.MaxAge * 24) * int64(time.Hour)))

	if backupFiles, err := this.listBackupFiles(); err == nil {
		keep := 0
		for _, f := range backupFiles {
			if keep >= this.MaxBackups {
				os.Remove(filepath.Join(this.dir(), f.Name()))
				continue
			}

			if this.MaxAge > 0 {
				if f.timestamp.Before(cutoff) {
					os.Remove(filepath.Join(this.dir(), f.Name()))
					continue
				}
			}

			keep++
		}
	}
}

type backupFile struct {
	timestamp time.Time
	fs.DirEntry
}

type backupFiles []backupFile

func (this backupFiles) Less(i, j int) bool {
	return this[i].timestamp.After(this[j].timestamp)
}

func (this backupFiles) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this backupFiles) Len() int {
	return len(this)
}

func (this *Logger) listBackupFiles() (backupFiles, error) {
	files, _ := os.ReadDir(this.dir())

	backupFiles := backupFiles{}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		filename := this.filename()
		if !strings.HasPrefix(name, filename) {
			continue
		}

		t, err := time.Parse(TIME_FORMAT, strings.TrimPrefix(name, filename))
		if err != nil {
			continue
		}

		backupFiles = append(backupFiles, backupFile{t, f})
	}

	sort.Sort(backupFiles)

	return backupFiles, nil
}

func (this *Logger) filename() string {
	if this.Filename != "" {
		return this.Filename
	}
	return filepath.Base(os.Args[0]) + "-log.log"
}

func (this *Logger) dir() string {
	return filepath.Dir(this.filename())
}

func (this *Logger) filemode() fs.FileMode {
	if this.FileMode != 0 {
		return this.FileMode
	}
	return os.FileMode(0644)
}

func (this *Logger) size() int64 {
	if this.Size != 0 {
		return this.Size * MEGABYTE
	}
	return 100 * MEGABYTE
}
