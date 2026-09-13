package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type persistentState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

const stateFileName = "raft.state"

func (r *Raft) loadPersistentState(dir string) error {
	path := filepath.Join(dir, stateFileName)

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// First startup. Use the defaults already set in NewRaft.
		return nil
	}

	if err != nil {
		return err
	}

	var state persistentState

	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}

	r.term = state.CurrentTerm

	if state.VotedFor == "" {
		r.votedFor = "-1"
	} else {
		r.votedFor = state.VotedFor
	}

	return nil
}

func (r *Raft) persistStateLocked() error {
	state := persistentState{
		CurrentTerm: r.term,
		VotedFor:    r.votedFor,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	path := filepath.Join(
		filepath.Dir(r.log.sFile.Name()),
		stateFileName,
	)

	tmp := path + ".tmp"

	f, err := os.OpenFile(
		tmp,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}

	// Make sure the state reaches disk before replacing the old file.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	return nil
}
