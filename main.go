// Copyright (c) 2020, Objective Security Corporation

// Permission to use, copy, modify, and/or distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.

// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
package main

import (
	"fmt"
	"github.com/docker/go-plugins-helpers/volume"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type ofsVolume struct {
	volume  *volume.Volume
	fs      string
	opts    string
	env     []string
	use     map[string]bool
	mounted bool
	asap    bool
}

type ofsDriver struct {
	sync.RWMutex
	volumes map[string]*ofsVolume
}

var version = "1.0"

func (d ofsDriver) Create(r *volume.CreateRequest) error {
	log.Printf("Create ObjectiveFS Volume '%s'", r.Name)
	d.Lock()
	defer d.Unlock()

	if _, ok := d.volumes[r.Name]; ok {
		return fmt.Errorf("volume '%s' already exists", r.Name)
	}
	v := &ofsVolume{}
	v.volume = &volume.Volume{Name: r.Name, Mountpoint: filepath.Join(volume.DefaultDockerRootDirectory, "objectivefs", r.Name), CreatedAt: time.Now().Format(time.RFC3339Nano)}
	v.use = make(map[string]bool)
	v.opts = "auto"
	for key, val := range r.Options {
		switch key {
		case "fs":
			v.fs = val
		case "options", "ptions":
			v.opts = v.opts + "," + val
		case "asap":
			v.asap = true
		default:
			v.env = append(v.env, key+"="+val)
		}
	}
	d.volumes[r.Name] = v
	return nil
}

func (d ofsDriver) List() (*volume.ListResponse, error) {
	d.Lock()
	defer d.Unlock()

	var vs []*volume.Volume
	for _, v := range d.volumes {
		vs = append(vs, v.volume)
	}
	return &volume.ListResponse{Volumes: vs}, nil
}

func (d ofsDriver) Get(r *volume.GetRequest) (*volume.GetResponse, error) {
	d.Lock()
	defer d.Unlock()

	v, ok := d.volumes[r.Name]
	if !ok {
		return &volume.GetResponse{}, fmt.Errorf("volume '%s' not found", r.Name)
	}
	return &volume.GetResponse{Volume: v.volume}, nil
}

func umount(v *ofsVolume) error {
	log.Printf("Unmount ObjectiveFS Volume '%s'", v.volume.Name)
	if !v.mounted {
		return nil
	}
	if err := exec.Command("umount", v.volume.Mountpoint).Run(); err != nil {
		return err
	}
	if err := os.Remove(v.volume.Mountpoint); err != nil {
		return err
	}
	v.mounted = false
	return nil
}

func (d ofsDriver) Remove(r *volume.RemoveRequest) error {
	d.Lock()
	defer d.Unlock()

	v, ok := d.volumes[r.Name]
	if !ok {
		return fmt.Errorf("volume '%s' not found", r.Name)
	}
	if len(v.use) != 0 {
		return fmt.Errorf("volume '%s' currently in use (%d unique)", r.Name, len(v.use))
	}
	if err := umount(v); err != nil {
		return err
	}
	delete(d.volumes, r.Name)
	return nil
}

func (d ofsDriver) Path(r *volume.PathRequest) (*volume.PathResponse, error) {
	d.Lock()
	defer d.Unlock()

	v, ok := d.volumes[r.Name]
	if !ok {
		return &volume.PathResponse{}, fmt.Errorf("volume '%s' not found", r.Name)
	}
	return &volume.PathResponse{Mountpoint: v.volume.Mountpoint}, nil
}

func (d ofsDriver) Mount(r *volume.MountRequest) (*volume.MountResponse, error) {
	d.Lock()
	defer d.Unlock()

	v, ok := d.volumes[r.Name]
	if !ok {
		return &volume.MountResponse{}, fmt.Errorf("volume '%s' not found", r.Name)
	}
	log.Printf("Attach ObjectiveFS Volume '%s' to '%s'", r.Name, r.ID)
	if !v.mounted {
		if err := os.MkdirAll(v.volume.Mountpoint, 0755); err != nil {
			return &volume.MountResponse{}, err
		}
		cmd := exec.Command("/sbin/mount.objectivefs", "-o"+v.opts, v.fs, v.volume.Mountpoint)
		cmd.Env = v.env
		log.Printf("Mount ObjectiveFS Volume '%s': '%s'", r.Name, cmd)
		if err := cmd.Run(); err != nil {
			return &volume.MountResponse{}, fmt.Errorf("unexpected error mounting '%s' check log (/var/log/syslog or /var/log/messages): %s", r.Name, err.Error())
		}
		v.mounted = true
	}
	v.use[r.ID] = true
	return &volume.MountResponse{Mountpoint: v.volume.Mountpoint}, nil
}

func (d ofsDriver) Unmount(r *volume.UnmountRequest) error {
	d.Lock()
	defer d.Unlock()

	v, ok := d.volumes[r.Name]
	if !ok {
		return fmt.Errorf("volume '%s' not found", r.Name)
	}
	log.Printf("Detach ObjectiveFS Volume '%s' from '%s'", r.Name, r.ID)
	delete(v.use, r.ID)
	if len(v.use) == 0 && v.asap {
		if err := umount(v); err != nil {
			return err
		}
	}
	return nil
}

func (d ofsDriver) Capabilities() *volume.CapabilitiesResponse {
	d.Lock()
	defer d.Unlock()

	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "local"}}
}

func main() {
	log.Printf("Starting ObjectiveFS Volume Driver, version " + version)
	d := ofsDriver{volumes: make(map[string]*ofsVolume)}
	h := volume.NewHandler(d)
	u, _ := user.Lookup("root")
	gid, _ := strconv.Atoi(u.Gid)
	h.ServeUnix("objectivefs", gid)
}
