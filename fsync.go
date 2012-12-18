// package fsync keeps two files or directories in sync.
//
//         err := fsync.Sync("~/dst", ".")
//
// After the above code, if err is nil, every file and directory in the current 
// directory is copied to ~/dst and has the same permissions. Consequent calls
// will only copy changed or new files. You can use SyncDel to also delete
// extra files in the destination:
//
//         err := fsync.SyncDel("~/dst", ".")
package fsync

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
)

var (
	ErrFileOverDir = errors.New(
		"fsync: trying to overwrite a non-empty directory with a file")
)

// Sync copies files and directories inside src into dst.
func Sync(dst, src string) error {
	// return error instead of replacing a non-empty directory with a file
	if b, err := checkDir(dst, src); err != nil {
		return err
	} else if b {
		return ErrFileOverDir
	}

	return syncRecover(false, dst, src)
}

// SyncDel makes sure dst is a copy of src. It's only difference with Sync is in
// deleting files in dst that are not found in src.
func SyncDel(dst, src string) error {
	// return error instead of replacing a non-empty directory with a file
	if b, err := checkDir(dst, src); err != nil {
		return err
	} else if b {
		return ErrFileOverDir
	}

	return syncRecover(true, dst, src)
}

// SyncTo syncs srcs files or directories **into** to directory. Calling
//
//         SyncTo("a", "b", "c/d")
//
// is equivalent to calling
//
//         Sync("a/b", "b")
//         Sync("a/d", "c/d")
//
// Actually, this is also implementation of SyncTo.
func SyncTo(to string, srcs ...string) error {
	for _, src := range srcs {
		dst := path.Join(to, path.Base(src))
		if err := Sync(dst, src); err != nil {
			return err
		}
	}
	return nil
}

// SyncDelTo syncs srcs files or directories **into** to directory. It differs
// with SyncDelTo in using SyncDel instead of Sync.
func SyncDelTo(to string, srcs ...string) error {
	for _, src := range srcs {
		dst := path.Join(to, path.Base(src))
		if err := SyncDel(dst, src); err != nil {
			return err
		}
	}
	return nil
}

// syncRecover handles errors and calls sync
func syncRecover(del bool, dst, src string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	sync(del, dst, src)
	return nil
}

// sync updates dst to match with src, handling both files and directories.
func sync(del bool, dst, src string) {
	// sync permissions after handling content
	defer syncperms(dst, src)

	// read files info
	d, err := os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	s, err := os.Stat(src)
	check(err)

	if !s.IsDir() {
		// src is a file
		// delete dst if its a directory
		if d != nil && d.IsDir() {
			check(os.RemoveAll(dst))
		}
		if !equal(dst, src) {
			// perform copy
			df, err := os.Create(dst)
			check(err)
			defer df.Close()
			sf, err := os.Open(src)
			check(err)
			defer sf.Close()
			_, err = io.Copy(df, sf)
			check(err)
		}
		return
	}

	// src is a directory
	// make dst if necessary
	if d == nil {
		// dst does not exist; create directory
		check(os.MkdirAll(dst, 0755)) // permissions will be synced later
	} else if !d.IsDir() {
		// dst is a file; remove and create directory
		check(os.Remove(dst))
		check(os.MkdirAll(dst, 0755)) // permissions will be synced later
	}

	// go through sf files and sync them
	files, err := ioutil.ReadDir(src)
	check(err)
	// make a map of filenames for quick lookup; used in deletion
	// deletion below
	m := make(map[string]bool, len(files))
	for _, file := range files {
		dst2 := path.Join(dst, file.Name())
		src2 := path.Join(src, file.Name())
		sync(del, dst2, src2)
		m[file.Name()] = true
	}

	// delete files from dst that does not exist in src
	if del {
		files, err = ioutil.ReadDir(dst)
		check(err)
		for _, file := range files {
			if !m[file.Name()] {
				check(os.RemoveAll(path.Join(dst, file.Name())))
			}
		}
	}
}

// syncperms makes sure dst has the same pemissions as src
func syncperms(dst, src string) {
	// get file infos; return if not exist and panic if error
	d, err1 := os.Stat(dst)
	s, err2 := os.Stat(src)
	if os.IsNotExist(err1) || os.IsNotExist(err2) {
		return
	}
	check(err1)
	check(err2)

	// return if they are already the same
	if d.Mode().Perm() == s.Mode().Perm() {
		return
	}

	// update dst's permission bits
	check(os.Chmod(dst, s.Mode().Perm()))
}

// equal returns true if both files are equal
func equal(a, b string) bool {
	// get file infos
	info1, err1 := os.Stat(a)
	info2, err2 := os.Stat(b)
	if os.IsNotExist(err1) || os.IsNotExist(err2) {
		return false
	}
	check(err1)
	check(err2)

	// check sizes
	if info1.Size() != info2.Size() {
		return false
	}

	// both have the same size, check the contents
	f1, err := os.Open(a)
	check(err)
	defer f1.Close()
	f2, err := os.Open(b)
	check(err)
	defer f2.Close()
	buf1 := make([]byte, 1000)
	buf2 := make([]byte, 1000)
	for {
		// read from both
		n1, err := f1.Read(buf1)
		if err != nil && err != io.EOF {
			panic(err)
		}
		n2, err := f2.Read(buf2)
		if err != nil && err != io.EOF {
			panic(err)
		}

		// compare read bytes
		if !bytes.Equal(buf1[:n1], buf2[:n2]) {
			return false
		}

		// end of both files
		if n1 == 0 && n2 == 0 {
			break
		}
	}

	return true
}

// checkDir returns true if dst is a non-empty directory and src is a file
func checkDir(dst, src string) (b bool, err error) {
	// read file info
	d, err := os.Stat(dst)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	s, err := os.Stat(src)
	if err != nil {
		return false, err
	}

	// return false is dst is not a directory or src is a directory
	if !d.IsDir() || s.IsDir() {
		return false, nil
	}

	// dst is a directory and src is a file
	// check if dst is non-empty
	// read dst directory
	files, err := ioutil.ReadDir(dst)
	if err != nil {
		return false, err
	}
	if len(files) > 0 {
		return true, nil
	}
	return false, nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
